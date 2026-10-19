//! Delta Lake access: open tables, list files, read incremental commits, create tables and
//! commit data together with progress (`txn` actions).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use deltalake_core::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties, TransactionError};
use deltalake_core::kernel::{Action, StructType, Transaction};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use object_store::path::Path;
use percent_encoding::percent_decode_str;
use serde_json::Value;
use url::Url;

use crate::error::{NodraError, Result};

/// A data file of a Delta table.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    /// Path relative to the table root (decoded).
    pub path: Path,
    /// Size in bytes.
    pub size: u64,
    /// Partition values in the order of the table's partition columns (`None` = NULL).
    pub partition_values: Vec<Option<String>>,
    /// Row count from the file statistics, if present.
    pub num_records: Option<u64>,
}

impl DataFile {
    /// Key identifying the file's partition (used for partition-level assignment).
    pub fn partition_key(&self) -> String {
        partition_key(&self.partition_values)
    }
}

/// Canonical text for a partition tuple.
pub fn partition_key(values: &[Option<String>]) -> String {
    values
        .iter()
        .map(|v| match v {
            Some(s) => format!("={s}"),
            None => "∅".to_string(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The data changes of one commit of a source table.
#[derive(Debug, Clone, Default)]
pub struct CommitChanges {
    /// Commit version.
    pub version: u64,
    /// Files added with `dataChange = true`.
    pub adds: Vec<DataFile>,
    /// Number of files removed with `dataChange = true` (deletes, updates, overwrites).
    pub data_removes: usize,
    /// Whether the commit changed the table metadata (schema, partitioning, properties).
    pub metadata_changed: bool,
    /// Whether the commit changed the protocol.
    pub protocol_changed: bool,
}

/// Turn a user-provided location (local path or URL) into a table URL.
pub fn table_url(location: &str) -> Result<Url> {
    Ok(deltalake_core::ensure_table_uri(location)?)
}

fn builder(location: &str) -> Result<DeltaTableBuilder> {
    #[cfg(feature = "azure")]
    azure::register();
    let url = table_url(location)?;
    Ok(DeltaTableBuilder::from_url(url)?)
}

/// Azure Data Lake / Blob storage support (feature `azure`). delta-rs resolves URL schemes
/// through registries; this registers `abfss://` & co. with `object_store`'s Azure client, which
/// reads credentials from the usual `AZURE_*` environment variables (or workload identity).
#[cfg(feature = "azure")]
mod azure {
    use std::sync::{Arc, Once};

    use deltalake_core::logstore::{
        default_logstore, logstore_factories, object_store_factories, LogStore, LogStoreFactory,
        ObjectStoreFactory, ObjectStoreRef, StorageConfig,
    };
    use deltalake_core::{DeltaResult, Path};
    use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
    use url::Url;

    #[derive(Debug, Default)]
    struct AzureFactory;

    impl ObjectStoreFactory for AzureFactory {
        fn parse_url_opts(&self, url: &Url, config: &StorageConfig) -> DeltaResult<(ObjectStoreRef, Path)> {
            // `object_store::parse_url_opts` would ignore the environment: start from it, so
            // `AZURE_STORAGE_ACCOUNT_KEY`, service principals and workload identity work.
            let mut builder = MicrosoftAzureBuilder::from_env().with_url(url.to_string());
            for (key, value) in &config.raw {
                if let Ok(k) = key.to_ascii_lowercase().parse::<AzureConfigKey>() {
                    builder = builder.with_config(k, value);
                }
            }
            let store = builder.build().map_err(object_store_error)?;
            let path = Path::from_url_path(url.path()).map_err(|e| object_store_error(e.into()))?;
            Ok((Arc::new(store), path))
        }
    }

    fn object_store_error(e: object_store::Error) -> deltalake_core::DeltaTableError {
        deltalake_core::DeltaTableError::ObjectStore { source: e }
    }

    impl LogStoreFactory for AzureFactory {
        fn with_options(
            &self,
            prefixed: ObjectStoreRef,
            root: ObjectStoreRef,
            location: &Url,
            options: &StorageConfig,
        ) -> DeltaResult<Arc<dyn LogStore>> {
            Ok(default_logstore(prefixed, root, location, options))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn azure_store_reads_the_environment() {
            // `abfss://container@account...` names the account itself; `az://container/...`
            // relies on AZURE_STORAGE_ACCOUNT_NAME, which `object_store::parse_url_opts` ignores.
            std::env::set_var("AZURE_STORAGE_ACCOUNT_NAME", "nodraenvaccount");
            std::env::set_var("AZURE_STORAGE_ACCOUNT_KEY", "a2V5");
            let url = Url::parse("az://container/lake/events").unwrap();
            let (store, path) = AzureFactory.parse_url_opts(&url, &StorageConfig::default()).unwrap();
            assert!(store.to_string().contains("nodraenvaccount"), "{store}");
            assert_eq!(path.as_ref(), "lake/events");
            let url = Url::parse("abfss://data@other.dfs.core.windows.net/t").unwrap();
            let (store, path) = AzureFactory.parse_url_opts(&url, &StorageConfig::default()).unwrap();
            assert!(store.to_string().contains("other"), "{store}");
            assert_eq!(path.as_ref(), "t");
        }
    }

    pub(super) fn register() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let f = Arc::new(AzureFactory);
            for scheme in ["az", "adl", "azure", "abfs", "abfss"] {
                let u = Url::parse(&format!("{scheme}://")).expect("static url");
                object_store_factories().insert(u.clone(), f.clone());
                logstore_factories().insert(u, f.clone());
            }
        });
    }
}

/// A handle to a table without loading its snapshot: enough to read commits, find the latest
/// version and access its object store.
pub fn handle(location: &str) -> Result<DeltaTable> {
    Ok(builder(location)?.build()?)
}

/// Latest version of the table behind `handle`, listing the log from `hint` onwards.
pub async fn latest_version(handle: &DeltaTable, hint: u64) -> Result<u64> {
    Ok(handle.log_store().get_latest_version(hint).await?)
}

/// Physical layout of a table: the schema of its data files and its partition columns.
#[derive(Debug, Clone)]
pub struct Layout {
    /// Columns stored in the Parquet files.
    pub file_schema: SchemaRef,
    /// Partition columns (not stored in the files), in table order.
    pub partition_fields: Vec<FieldRef>,
}

impl Layout {
    /// Layout of a loaded table.
    pub fn of(t: &DeltaTable) -> Result<Layout> {
        let (file_schema, partition_fields) = schemas(t)?;
        Ok(Layout { file_schema, partition_fields })
    }

    /// Partition column names.
    pub fn partition_columns(&self) -> Vec<String> {
        self.partition_fields.iter().map(|f| f.name().clone()).collect()
    }
}

/// Open the latest version of a table.
pub async fn open(location: &str) -> Result<DeltaTable> {
    let t = builder(location)?.load().await.map_err(|e| match e {
        DeltaTableError::NotATable(_) => NodraError::delta(format!("{location} is not a Delta table")),
        other => other.into(),
    })?;
    check_readable(&t, location)?;
    Ok(t)
}

/// Open a specific version of a table.
pub async fn open_version(location: &str, version: i64) -> Result<DeltaTable> {
    let v = u64::try_from(version).map_err(|_| NodraError::config("table versions are non-negative"))?;
    let t = builder(location)?.with_version(v).load().await?;
    check_readable(&t, location)?;
    Ok(t)
}

/// Open a table, or `None` if there is no table at `location` yet.
pub async fn try_open(location: &str) -> Result<Option<DeltaTable>> {
    use futures::StreamExt;
    // Check for a log first: loading a missing table makes delta-rs log an error.
    let probe = handle(location)?;
    let mut listing = probe.object_store().list(Some(&Path::from("_delta_log")));
    if listing.next().await.transpose()?.is_none() {
        return Ok(None);
    }
    match builder(location)?.load().await {
        Ok(t) => Ok(Some(t)),
        Err(DeltaTableError::NotATable(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Refuse tables whose physical layout Nodra's direct Parquet scan would read incorrectly.
pub fn check_readable(t: &DeltaTable, location: &str) -> Result<()> {
    let snap = t.snapshot()?;
    if let Some(features) = snap.protocol().reader_features() {
        for f in features {
            let name = format!("{f:?}").to_ascii_lowercase();
            if name.contains("deletionvector") || name.contains("columnmapping") {
                return Err(NodraError::delta(format!(
                    "{location} uses the `{f:?}` reader feature, which Nodra does not support yet"
                )));
            }
        }
    }
    if let Some(mode) = snap.metadata().configuration().get("delta.columnMapping.mode") {
        if mode != "none" {
            return Err(NodraError::delta(format!(
                "{location} uses column mapping, which Nodra does not support yet"
            )));
        }
    }
    Ok(())
}

/// Partition columns in table order.
pub fn partition_columns(t: &DeltaTable) -> Result<Vec<String>> {
    Ok(t.snapshot()?.metadata().partition_columns().to_vec())
}

/// `(file schema, partition fields)`: data columns as stored in the Parquet files, and the
/// partition columns (which Delta does not store in the files).
pub fn schemas(t: &DeltaTable) -> Result<(SchemaRef, Vec<FieldRef>)> {
    let snap = t.snapshot()?;
    let full: Schema = snap.schema().as_ref().try_into_arrow()?;
    let parts = partition_columns(t)?;
    let file_fields: Vec<Field> =
        full.fields().iter().filter(|f| !parts.contains(f.name())).map(|f| f.as_ref().clone()).collect();
    let part_fields = parts
        .iter()
        .map(|p| {
            full.field_with_name(p)
                .map(|f| Arc::new(f.clone()))
                .map_err(|_| NodraError::delta(format!("partition column `{p}` missing from schema")))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((Arc::new(Schema::new(file_fields)), part_fields))
}

/// The schema Nodra exposes for a table: data columns followed by partition columns.
pub fn exposed_schema(t: &DeltaTable) -> Result<SchemaRef> {
    let (file, parts) = schemas(t)?;
    let mut fields: Vec<FieldRef> = file.fields().iter().cloned().collect();
    fields.extend(parts);
    Ok(Arc::new(Schema::new(fields)))
}

/// All active data files.
pub fn active_files(t: &DeltaTable) -> Result<Vec<DataFile>> {
    let snap = t.snapshot()?;
    let parts = partition_columns(t)?;
    let mut out = Vec::with_capacity(snap.log_data().num_files());
    for f in snap.log_data().iter() {
        if f.deletion_vector_descriptor().is_some() {
            return Err(NodraError::delta("table contains deletion vectors, which Nodra does not support yet"));
        }
        let map = f.partition_values_map();
        out.push(DataFile {
            path: f.object_store_path(),
            size: u64::try_from(f.size()).unwrap_or(0),
            partition_values: parts.iter().map(|p| map.get(p).cloned().flatten()).collect(),
            num_records: f.num_records().map(|n| n as u64),
        });
    }
    Ok(out)
}

/// Current table version.
pub fn version(t: &DeltaTable) -> Result<u64> {
    t.version().ok_or_else(|| NodraError::delta("table has no version"))
}

/// Progress recorded under `app_id`, if any.
pub async fn txn_version(t: &DeltaTable, app_id: &str) -> Result<Option<i64>> {
    Ok(t.snapshot()?.transaction_version(t.log_store().as_ref(), app_id).await?)
}

/// All `txn` versions (`appId` -> version) of `t`'s snapshot, read in bulk from the newest
/// checkpoint (classic, multi-part or V2) and the commits after it.
pub async fn txn_map(t: &DeltaTable) -> Result<HashMap<String, i64>> {
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    let v = version(t)?;
    let store = t.object_store();
    let listed: Vec<Path> =
        store.list(Some(&Path::from("_delta_log"))).map_ok(|m| m.location).try_collect().await?;
    let mut map = HashMap::new();
    let start = match pick_checkpoint(&listed, v) {
        Some((cv, files)) => {
            for path in files {
                let bytes = store.get(&path).await?.bytes().await?;
                if path.as_ref().ends_with(".json") {
                    txns_from_json(&bytes, &mut map)?;
                } else {
                    txns_from_parquet(bytes, &mut map)?;
                }
            }
            cv + 1
        }
        None => 0,
    };
    if start <= v {
        map.extend(txns_in_commits(t, start, v).await?);
    }
    Ok(map)
}

/// The newest complete checkpoint at or below `max_version` in a `_delta_log` listing: a
/// classic (`<v>.checkpoint.parquet`), multi-part (`<v>.checkpoint.<i>.<n>.parquet`) or V2
/// (`<v>.checkpoint.<uuid>.parquet|json`; its sidecars hold only file actions) checkpoint.
fn pick_checkpoint(listed: &[Path], max_version: u64) -> Option<(u64, Vec<Path>)> {
    // version -> (parts expected, files)
    let mut found: std::collections::BTreeMap<u64, Vec<(usize, Vec<Path>)>> = Default::default();
    let mut multi: std::collections::BTreeMap<(u64, usize), Vec<Path>> = Default::default();
    for path in listed {
        let Some(name) = path.filename() else { continue };
        let parts: Vec<&str> = name.split('.').collect();
        if parts.len() < 3 || parts[1] != "checkpoint" {
            continue;
        }
        let Ok(cv) = parts[0].parse::<u64>() else { continue };
        if cv > max_version {
            continue;
        }
        match parts.as_slice() {
            [_, _, "parquet"] => found.entry(cv).or_default().push((1, vec![path.clone()])),
            [_, _, uuid, "parquet" | "json"] if uuid::Uuid::parse_str(uuid).is_ok() => {
                found.entry(cv).or_default().push((1, vec![path.clone()]))
            }
            [_, _, _part, total, "parquet"] => {
                if let Ok(total) = total.parse::<usize>() {
                    multi.entry((cv, total)).or_default().push(path.clone());
                }
            }
            _ => {}
        }
    }
    for ((cv, total), files) in multi {
        found.entry(cv).or_default().push((total, files));
    }
    found.into_iter().rev().find_map(|(cv, candidates)| {
        candidates.into_iter().find(|(n, files)| *n > 0 && files.len() == *n).map(|(_, files)| (cv, files))
    })
}

fn txns_from_parquet<R: datafusion::parquet::file::reader::ChunkReader + 'static>(
    bytes: R,
    map: &mut HashMap<String, i64>,
) -> Result<()> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::arrow::ProjectionMask;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let Some(idx) = builder.schema().fields().iter().position(|f| f.name() == "txn") else {
        return Ok(());
    };
    let mask = ProjectionMask::roots(builder.parquet_schema(), [idx]);
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let txn = batch.column(0).as_struct();
        let (Some(app), Some(ver)) = (txn.column_by_name("appId"), txn.column_by_name("version")) else {
            continue;
        };
        let app = datafusion::arrow::compute::cast(app, &datafusion::arrow::datatypes::DataType::Utf8)?;
        let app = app.as_string::<i32>();
        let ver = ver.as_primitive::<Int64Type>();
        for i in 0..txn.len() {
            if txn.is_valid(i) && app.is_valid(i) && ver.is_valid(i) {
                map.insert(app.value(i).to_string(), ver.value(i));
            }
        }
    }
    Ok(())
}

fn txns_from_json(bytes: &[u8], map: &mut HashMap<String, i64>) -> Result<()> {
    for line in bytes.split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) || !line.windows(6).any(|w| w == b"\"txn\":") {
            continue;
        }
        let val: Value = serde_json::from_slice(line)?;
        if let Some(txn) = val.get("txn") {
            if let (Some(app), Some(ver)) = (txn.get("appId").and_then(Value::as_str), txn.get("version").and_then(Value::as_i64)) {
                map.insert(app.to_string(), ver);
            }
        }
    }
    Ok(())
}

/// The `txn` versions set by commits `from..=to` (later commits win).
pub async fn txns_in_commits(t: &DeltaTable, from: u64, to: u64) -> Result<HashMap<String, i64>> {
    let log = t.log_store();
    let mut map = HashMap::new();
    for cv in from..=to {
        let bytes = log.read_commit_entry(cv).await?.ok_or_else(|| {
            NodraError::delta(format!("commit {cv} of {} is missing", t.table_url()))
        })?;
        txns_from_json(&bytes, &mut map)?;
    }
    Ok(map)
}

/// Changes of commits `(from, to]`, read from the commit files. `t` may be an unloaded handle.
pub async fn changes(t: &DeltaTable, partition_cols: &[String], from: u64, to: u64) -> Result<Vec<CommitChanges>> {
    let parts = partition_cols.to_vec();
    let store = t.log_store();
    let mut out = Vec::new();
    for v in (from + 1)..=to {
        let bytes = store.read_commit_entry(v).await?.ok_or_else(|| {
            NodraError::delta(format!(
                "commit {v} of {} is no longer available (log retention); \
                 restart the source from a newer version",
                t.table_url()
            ))
        })?;
        out.push(parse_commit(v, &bytes, &parts)?);
    }
    Ok(out)
}

/// Parse one commit file.
pub fn parse_commit(version: u64, bytes: &[u8], partition_cols: &[String]) -> Result<CommitChanges> {
    let mut c = CommitChanges { version, ..Default::default() };
    for line in bytes.split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let v: Value = serde_json::from_slice(line)?;
        if let Some(add) = v.get("add") {
            if add.get("dataChange").and_then(Value::as_bool) == Some(false) {
                continue; // compaction / OPTIMIZE rewrite: no new data
            }
            if add.get("deletionVector").map(|d| !d.is_null()).unwrap_or(false) {
                return Err(NodraError::delta(format!(
                    "commit {version} adds a file with a deletion vector, which Nodra does not support yet"
                )));
            }
            c.adds.push(file_from_add(add, partition_cols)?);
        } else if let Some(rm) = v.get("remove") {
            if rm.get("dataChange").and_then(Value::as_bool).unwrap_or(true) {
                c.data_removes += 1;
            }
        } else if v.get("metaData").is_some() {
            c.metadata_changed = true;
        } else if v.get("protocol").is_some() {
            c.protocol_changed = true;
        }
    }
    Ok(c)
}

fn file_from_add(add: &Value, partition_cols: &[String]) -> Result<DataFile> {
    let raw = add
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| NodraError::delta("add action without path"))?;
    if raw.contains("://") {
        return Err(NodraError::delta(format!(
            "absolute file path `{raw}` (shallow clone?) is not supported"
        )));
    }
    let decoded = percent_decode_str(raw).decode_utf8_lossy();
    let path = Path::parse(decoded.as_ref()).unwrap_or_else(|_| Path::from(decoded.as_ref()));
    let pv = add.get("partitionValues").and_then(Value::as_object);
    let partition_values = partition_cols
        .iter()
        .map(|p| pv.and_then(|m| m.get(p)).and_then(Value::as_str).map(str::to_string))
        .collect();
    let num_records = add
        .get("stats")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|s| s.get("numRecords").and_then(Value::as_u64));
    Ok(DataFile {
        path,
        size: add.get("size").and_then(Value::as_u64).unwrap_or(0),
        partition_values,
        num_records,
    })
}

/// Open the table at `location`, creating it with `schema` and `partition_columns` if it does
/// not exist. An existing table must have a compatible schema.
pub async fn create_or_open(location: &str, schema: &Schema, partition_cols: &[String]) -> Result<DeltaTable> {
    if let Some(t) = try_open(location).await? {
        check_compatible(&t, location, schema, partition_cols)?;
        return Ok(t);
    }
    for p in partition_cols {
        if schema.field_with_name(p).is_err() {
            return Err(NodraError::job(format!(
                "partition column `{p}` is not an output column of the query"
            )));
        }
    }
    if partition_cols.len() == schema.fields().len() {
        return Err(NodraError::job("a Delta table needs at least one non-partition column"));
    }
    let kernel: StructType = schema.try_into_kernel()?;
    let table = builder(location)?.build()?;
    // No retries: if another worker creates the table concurrently, our version-0 commit must
    // fail instead of being retried as a second CREATE at version 1.
    let created = CreateBuilder::new()
        .with_log_store(table.log_store())
        .with_columns(kernel.fields().cloned())
        .with_partition_columns(partition_cols.to_vec())
        .with_save_mode(SaveMode::Ignore)
        .with_commit_properties(CommitProperties::default().with_max_retries(0))
        .await;
    match created {
        Ok(t) => {
            check_compatible(&t, location, schema, partition_cols)?;
            Ok(t)
        }
        // Most likely another worker created the table concurrently (the create commit then
        // fails with a version or metadata conflict): use the table it created.
        Err(e) => match try_open(location).await? {
            Some(t) => {
                check_compatible(&t, location, schema, partition_cols)?;
                Ok(t)
            }
            None => Err(e.into()),
        },
    }
}

/// The table's schema as Arrow, in table column order (partition columns included).
pub fn arrow_schema(t: &DeltaTable) -> Result<SchemaRef> {
    let full: Schema = t.snapshot()?.schema().as_ref().try_into_arrow()?;
    Ok(Arc::new(full))
}

/// Normalize an Arrow schema to what a Delta table with that schema reports back (e.g. list
/// element names), so schemas can be compared.
pub fn roundtrip_schema(schema: &Schema) -> Result<Schema> {
    let kernel: StructType = schema.try_into_kernel()?;
    Ok((&kernel).try_into_arrow()?)
}

fn check_compatible(t: &DeltaTable, location: &str, schema: &Schema, partition_cols: &[String]) -> Result<()> {
    let existing_parts = partition_columns(t)?;
    if existing_parts != partition_cols {
        return Err(NodraError::delta(format!(
            "{location} is partitioned by [{}], but the job writes [{}]",
            existing_parts.join(", "),
            partition_cols.join(", ")
        )));
    }
    let snap = t.snapshot()?;
    let existing: Schema = snap.schema().as_ref().try_into_arrow()?;
    let schema = roundtrip_schema(schema)?;
    for f in schema.fields() {
        match existing.field_with_name(f.name()) {
            Ok(e) if e.data_type() == f.data_type() => {}
            Ok(e) => {
                return Err(NodraError::delta(format!(
                    "column `{}` of {location} has type {}, but the job writes {}",
                    f.name(),
                    e.data_type(),
                    f.data_type()
                )))
            }
            Err(_) => {
                return Err(NodraError::delta(format!(
                    "{location} has no column `{}`; schema evolution is not supported yet",
                    f.name()
                )))
            }
        }
    }
    if existing.fields().len() != schema.fields().len() {
        return Err(NodraError::delta(format!(
            "{location} has {} columns, but the job writes {}",
            existing.fields().len(),
            schema.fields().len()
        )));
    }
    Ok(())
}

/// Outcome of a commit attempt.
#[derive(Debug)]
pub enum CommitOutcome {
    /// Committed as this version.
    Committed(u64),
    /// Another process committed progress for one of our `txn` app ids first.
    Conflict,
    /// A concurrent commit (e.g. a DELETE on the target) conflicts with our read snapshot; the
    /// same actions can be retried on a fresh snapshot after re-checking progress.
    Retry(String),
    /// `max_retries = 0` and the table moved on since the read snapshot: actions derived from
    /// the snapshot (such as `remove`s) must be recomputed on a fresh one.
    Stale,
}

/// Commit `actions` and `txns` on top of the snapshot `t` was loaded at. Commits that lose a
/// log-version race are retried by delta-rs after checking for conflicts; a concurrent commit
/// of one of our app ids is reported as [`CommitOutcome::Conflict`].
pub async fn commit(
    t: &mut DeltaTable,
    actions: Vec<Action>,
    txns: Vec<Transaction>,
    operation: DeltaOperation,
    max_retries: usize,
) -> Result<CommitOutcome> {
    use deltalake_core::kernel::transaction::CommitConflictError as C;
    let read_version = version(t)?;
    // Expired-log cleanup lists the whole `_delta_log`; do it on roughly every 100th commit only.
    let cleanup = (read_version + 1) % 100 == 0;
    let props = CommitProperties::default()
        .with_application_transactions(txns)
        .with_max_retries(max_retries)
        .with_cleanup_expired_logs(Some(cleanup));
    let res = CommitBuilder::from(props)
        .with_actions(actions)
        .build(Some(t.snapshot()?), t.log_store(), operation)
        .await;
    match res {
        Ok(done) => {
            let v = done.version();
            t.state = Some(done.snapshot);
            Ok(CommitOutcome::Committed(v))
        }
        Err(DeltaTableError::Transaction { source: TransactionError::CommitConflict(C::ConcurrentTransaction) }) => {
            Ok(CommitOutcome::Conflict)
        }
        Err(DeltaTableError::Transaction { source: TransactionError::CommitConflict(c) })
            if matches!(c, C::ConcurrentDeleteRead | C::ConcurrentDeleteDelete) =>
        {
            Ok(CommitOutcome::Retry(c.to_string()))
        }
        Err(DeltaTableError::Transaction { source: TransactionError::MaxCommitAttempts(_) }) if max_retries == 0 => {
            Ok(CommitOutcome::Stale)
        }
        Err(e) => Err(e.into()),
    }
}

/// Append operation descriptor.
pub fn append_op(partition_cols: &[String]) -> DeltaOperation {
    DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: (!partition_cols.is_empty()).then(|| partition_cols.to_vec()),
        predicate: None,
    }
}

/// Overwrite operation descriptor (optionally scoped by a predicate description).
pub fn overwrite_op(partition_cols: &[String], predicate: Option<String>) -> DeltaOperation {
    DeltaOperation::Write {
        mode: SaveMode::Overwrite,
        partition_by: (!partition_cols.is_empty()).then(|| partition_cols.to_vec()),
        predicate,
    }
}

/// `remove` actions for the given files (data change).
pub fn removes_for(t: &DeltaTable, keep: impl Fn(&HashMap<String, Option<String>>) -> bool) -> Result<Vec<Action>> {
    let snap = t.snapshot()?;
    let mut out = Vec::new();
    for f in snap.log_data().iter() {
        if !keep(&f.partition_values_map()) {
            out.push(Action::Remove(f.remove_action(true)));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commit_file() {
        let commit = br#"{"commitInfo":{"operation":"WRITE"}}
{"add":{"path":"day=2026-10-17/part-1.parquet","partitionValues":{"day":"2026-10-17"},"size":100,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":7}"}}
{"add":{"path":"day=2026-10-17/part-2.parquet","partitionValues":{"day":"2026-10-17"},"size":100,"modificationTime":1,"dataChange":false}}
{"add":{"path":"day=__HIVE_DEFAULT_PARTITION__/part%203.parquet","partitionValues":{"day":null},"size":5,"modificationTime":1,"dataChange":true}}
{"remove":{"path":"old.parquet","dataChange":true}}
{"remove":{"path":"old2.parquet","dataChange":false}}
"#;
        let c = parse_commit(3, commit, &["day".to_string()]).unwrap();
        assert_eq!(c.version, 3);
        assert_eq!(c.adds.len(), 2);
        assert_eq!(c.adds[0].num_records, Some(7));
        assert_eq!(c.adds[0].partition_values, vec![Some("2026-10-17".to_string())]);
        assert_eq!(c.adds[1].partition_values, vec![None]);
        assert_eq!(c.adds[1].path.as_ref(), "day=__HIVE_DEFAULT_PARTITION__/part 3.parquet");
        assert_eq!(c.data_removes, 1);
        assert!(!c.metadata_changed);
    }

    #[test]
    fn partition_keys_distinguish_null() {
        assert_ne!(partition_key(&[None]), partition_key(&[Some(String::new())]));
        assert_eq!(partition_key(&[Some("a".into()), None]), "=a/∅");
    }
}

#[cfg(test)]
mod txn_tests {
    use super::*;

    fn paths(names: &[&str]) -> Vec<Path> {
        names.iter().map(|n| Path::from(format!("_delta_log/{n}"))).collect()
    }

    #[test]
    fn picks_the_newest_complete_checkpoint() {
        let uuid = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5";
        let listed = paths(&[
            "00000000000000000010.checkpoint.parquet",
            "00000000000000000020.checkpoint.0000000001.0000000002.parquet", // incomplete
            &format!("00000000000000000015.checkpoint.{uuid}.json"),
            &format!("00000000000000000030.checkpoint.{uuid}.parquet"), // newer than the snapshot
            "00000000000000000025.checkpoint.notauuid.parquet",
            "_last_checkpoint",
        ]);
        let (v, files) = pick_checkpoint(&listed, 29).unwrap();
        assert_eq!(v, 15);
        assert!(files[0].as_ref().ends_with(".json"));
        assert_eq!(pick_checkpoint(&listed, 12).unwrap().0, 10);
        assert!(pick_checkpoint(&listed, 5).is_none());
        let listed = paths(&[
            "00000000000000000020.checkpoint.0000000001.0000000002.parquet",
            "00000000000000000020.checkpoint.0000000002.0000000002.parquet",
        ]);
        let (v, files) = pick_checkpoint(&listed, 20).unwrap();
        assert_eq!((v, files.len()), (20, 2));
    }

    #[test]
    fn reads_txns_from_v2_json_checkpoints() {
        let body = concat!(
            r#"{"checkpointMetadata":{"version":15}}"#,
            "\n",
            r#"{"txn":{"appId":"nodra/j/delta:/t/w0of2","version":7}}"#,
            "\n",
            r#"{"sidecar":{"path":"a.parquet","sizeInBytes":1,"modificationTime":1}}"#,
            "\n",
            r#"{"txn":{"appId":"nodra/j/merge","version":3,"lastUpdated":1}}"#,
            "\n"
        );
        let mut map = HashMap::new();
        txns_from_json(body.as_bytes(), &mut map).unwrap();
        assert_eq!(map.get("nodra/j/delta:/t/w0of2"), Some(&7));
        assert_eq!(map.get("nodra/j/merge"), Some(&3));
        assert_eq!(map.len(), 2);
    }
}
