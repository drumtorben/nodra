//! A DataFusion table over an explicit list of Parquet files with Delta partition values.
//!
//! Each worker registers its share of the driving table (and broadcast tables in full) as a
//! [`FileListTable`]; the files are read directly from the table's object store.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, ScalarValue, Statistics};
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DfResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectMeta;

use crate::delta::DataFile;
use crate::error::{NodraError, Result};

/// See module docs.
pub struct FileListTable {
    file_schema: SchemaRef,
    partition_fields: Vec<FieldRef>,
    schema: SchemaRef,
    store_url: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
    /// Row count of each file (same order as `files`) from the Delta log, if known.
    rows: Vec<Option<u64>>,
}

impl fmt::Debug for FileListTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileListTable")
            .field("store_url", &self.store_url.as_str())
            .field("files", &self.files.len())
            .finish()
    }
}

impl FileListTable {
    /// Build a table from `files`. `store_url` must be registered with the session and map to
    /// an object store rooted at the Delta table root.
    pub fn try_new(
        file_schema: SchemaRef,
        partition_fields: Vec<FieldRef>,
        store_url: ObjectStoreUrl,
        files: &[DataFile],
    ) -> Result<Self> {
        let mut fields: Vec<FieldRef> = file_schema.fields().iter().cloned().collect();
        fields.extend(partition_fields.iter().cloned());
        let schema = Arc::new(Schema::new(fields));
        let rows = files_rows(files);
        let files = files
            .iter()
            .map(|f| {
                let values = f
                    .partition_values
                    .iter()
                    .zip(&partition_fields)
                    .map(|(v, field)| partition_scalar(v.as_deref(), field))
                    .collect::<Result<Vec<_>>>()?;
                // `new_from_meta` keeps the object key as is (`PartitionedFile::new` would
                // re-encode `%` in hive-escaped partition directories).
                let meta = ObjectMeta {
                    location: f.path.clone(),
                    last_modified: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                    size: f.size,
                    e_tag: None,
                    version: None,
                };
                Ok(PartitionedFile::new_from_meta(meta).with_partition_values(values))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { file_schema, partition_fields, schema, store_url, files, rows })
    }

    /// Number of files.
    pub fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Indices of the files whose partition values can satisfy `filters`. Only filters that
    /// reference nothing but partition columns are used; DataFusion still applies all filters to
    /// the rows (and uses the others for Parquet statistics pruning).
    fn prune(&self, state: &dyn Session, filters: &[Expr]) -> DfResult<Vec<usize>> {
        let all = || (0..self.files.len()).collect::<Vec<_>>();
        let names: Vec<&str> = self.partition_fields.iter().map(|f| f.name().as_str()).collect();
        let applicable: Vec<Expr> = filters
            .iter()
            .filter(|e| !names.is_empty() && expr_applicable_for_cols(&names, e))
            .map(|e| unqualify(e.clone()))
            .collect::<DfResult<_>>()?;
        let Some(predicate) = conjunction(applicable) else { return Ok(all()) };
        let schema = Arc::new(Schema::new(self.partition_fields.clone()));
        let predicate =
            state.create_physical_expr(predicate, &DFSchema::try_from(schema.as_ref().clone())?)?;
        // One row per file, one column per partition column.
        let columns = (0..self.partition_fields.len())
            .map(|i| ScalarValue::iter_to_array(self.files.iter().map(|f| f.partition_values[i].clone())))
            .collect::<DfResult<Vec<ArrayRef>>>()?;
        let batch = RecordBatch::try_new(schema, columns)?;
        // A filter that fails on some partition values (e.g. a guarded division) only means
        // that pruning is not possible; DataFusion evaluates it on the rows anyway.
        let Ok(mask) = predicate.evaluate(&batch).and_then(|m| m.into_array(batch.num_rows())) else {
            return Ok(all());
        };
        let Some(mask) = mask.as_any().downcast_ref::<BooleanArray>() else {
            return Ok(all());
        };
        Ok((0..self.files.len()).filter(|i| mask.is_valid(*i) && mask.value(*i)).collect())
    }

    /// Table statistics of the files `picked`: row count (from the Delta log) and size on disk.
    ///
    /// DataFusion's join selection needs them to build hash joins on the small side: without
    /// statistics it keeps the query's join order and builds on the left input, so a driving
    /// table written first (`FROM events JOIN campaigns`) was loaded into memory in full. Both
    /// values are inexact (filters may remove rows; Parquet size is not memory size), which is
    /// all join selection needs and keeps rules that require exact counts away.
    fn statistics_of(&self, picked: &[usize]) -> Statistics {
        let mut stats = Statistics::new_unknown(&self.schema);
        let rows: Option<u64> = picked.iter().map(|&i| self.rows[i]).sum();
        if let Some(rows) = rows {
            stats.num_rows = Precision::Inexact(rows as usize);
        }
        let bytes: u64 = picked.iter().map(|&i| self.files[i].object_meta.size).sum();
        stats.total_byte_size = Precision::Inexact(bytes as usize);
        stats
    }

    /// Mark this table as the driving source (see [`crate::partial`]).
    pub fn mark_driving(mut self) -> Self {
        self.file_schema = crate::partial::mark_driving(&self.file_schema);
        self.schema = crate::partial::mark_driving(&self.schema);
        self
    }
}

fn files_rows(files: &[DataFile]) -> Vec<Option<u64>> {
    files.iter().map(|f| f.num_records).collect()
}

/// Drop table qualifiers (`e.day` → `day`) so the expression resolves against the partition schema.
fn unqualify(e: Expr) -> DfResult<Expr> {
    e.transform(|e| {
        Ok(match e {
            Expr::Column(c) if c.relation.is_some() => {
                Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))
            }
            e => Transformed::no(e),
        })
    })
    .map(|t| t.data)
}

/// Partition values in a canonical spelling (rendered from the typed value), so that values
/// written by different Delta writers compare equal (`2025-10-09 08:53:20` from Spark and
/// `2025-10-09 08:53:20.000000` from delta-rs). Values that do not parse are kept as they are.
pub fn canonical_partition_values(values: &[Option<String>], fields: &[FieldRef]) -> Vec<Option<String>> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| match (v, fields.get(i)) {
            (Some(raw), Some(field)) => Some(
                partition_scalar(Some(raw), field).map(|s| s.to_string()).unwrap_or_else(|_| raw.clone()),
            ),
            (other, _) => other.clone(),
        })
        .collect()
}

/// Convert a Delta partition value string to a typed scalar.
pub fn partition_scalar(value: Option<&str>, field: &FieldRef) -> Result<ScalarValue> {
    match value {
        None => Ok(ScalarValue::try_from(field.data_type())?),
        Some(v) => ScalarValue::try_from_string(v.to_string(), field.data_type()).map_err(|e| {
            NodraError::delta(format!(
                "partition value `{v}` of `{}` is not a valid {}: {e}",
                field.name(),
                field.data_type()
            ))
        }),
    }
}

#[async_trait]
impl TableProvider for FileListTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Filters reach `scan` for partition pruning; DataFusion keeps applying them as well.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let picked = if self.files.is_empty() { Vec::new() } else { self.prune(state, filters)? };
        if picked.is_empty() {
            let schema = match projection {
                Some(p) => Arc::new(self.schema.project(p)?),
                None => self.schema.clone(),
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        }
        let statistics = self.statistics_of(&picked);
        let files: Vec<PartitionedFile> = picked.iter().map(|&i| self.files[i].clone()).collect();
        let groups_n = state.config().target_partitions().max(1).min(files.len());
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); groups_n];
        for (i, f) in files.into_iter().enumerate() {
            groups[i % groups_n].push(f);
        }
        let table_schema = TableSchema::builder(self.file_schema.clone())
            .with_table_partition_cols(self.partition_fields.clone())
            .build();
        let parquet_options = TableParquetOptions {
            global: state.config().options().execution.parquet.clone(),
            ..Default::default()
        };
        let source = ParquetSource::new(table_schema).with_table_parquet_options(parquet_options);
        let config = FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(source))
            .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_statistics(statistics)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    #[tokio::test]
    async fn filters_prune_row_groups_and_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mut files = Vec::new();
        for day in ["2026-01-01", "2026-01-02"] {
            std::fs::create_dir_all(dir.path().join(format!("day={day}"))).unwrap();
            let rel = format!("day={day}/part.parquet");
            let file = std::fs::File::create(dir.path().join(&rel)).unwrap();
            let props = WriterProperties::builder().set_max_row_group_row_count(Some(10)).build();
            let mut w = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(0..100))])
                    .unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            let size = std::fs::metadata(dir.path().join(&rel)).unwrap().len();
            files.push(DataFile {
                path: Path::from(rel),
                size,
                partition_values: vec![Some(day.to_string())],
                num_records: Some(100),
            });
        }
        let ctx = SessionContext::new();
        let url = ObjectStoreUrl::parse("nodra://t").unwrap();
        ctx.register_object_store(
            url.as_ref(),
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        );
        let day = Arc::new(Field::new("day", DataType::Utf8, true));
        let table = FileListTable::try_new(schema, vec![day], url, &files).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();

        let sql = "SELECT count(*) AS n FROM t WHERE x >= 95 AND day = '2026-01-02'";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rows = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await.unwrap();
        let n = rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(n, 5);

        // The other partition's file is not scanned at all; the predicate reached the Parquet
        // scan, which skipped 9 of the 10 row groups of the remaining file.
        let metrics =
            datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(plan.as_ref())
                .indent(true)
                .to_string();
        assert!(!metrics.contains("day=2026-01-01/part.parquet"), "{metrics}");
        assert!(metrics.contains("predicate="), "{metrics}");
        assert!(metrics.contains("row_groups_pruned_statistics=10 total → 1 matched"), "{metrics}");
    }

    /// Write `n` rows of `x = 0..n` as one Parquet file at `rel` and describe it as a Delta file.
    fn parquet_file(dir: &std::path::Path, rel: &str, n: i64) -> DataFile {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let path = dir.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut w =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None).unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..n))]).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        DataFile {
            path: Path::from(rel),
            size: std::fs::metadata(&path).unwrap().len(),
            partition_values: vec![],
            num_records: Some(n as u64),
        }
    }

    /// Regression: without statistics DataFusion built the hash join on the left input, so the
    /// large driving table of `FROM events JOIN campaigns` was loaded into memory in full.
    #[tokio::test]
    async fn joins_build_on_the_small_table() {
        let dir = tempfile::tempdir().unwrap();
        let events = vec![
            parquet_file(dir.path(), "events/a.parquet", 20_000),
            parquet_file(dir.path(), "events/b.parquet", 20_000),
        ];
        let campaigns = vec![parquet_file(dir.path(), "campaigns/a.parquet", 10)];
        let ctx = SessionContext::new();
        let url = ObjectStoreUrl::parse("nodra://t").unwrap();
        ctx.register_object_store(
            url.as_ref(),
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        for (name, files) in [("events", &events), ("campaigns", &campaigns)] {
            let table = FileListTable::try_new(schema.clone(), vec![], url.clone(), files).unwrap();
            ctx.register_table(name, Arc::new(table)).unwrap();
        }

        let sql = "SELECT e.x FROM events e JOIN campaigns c ON e.x = c.x";
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        let text = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let join = text.find("HashJoinExec").unwrap_or_else(|| panic!("no hash join:\n{text}"));
        assert!(text[join..].starts_with("HashJoinExec: mode=CollectLeft"), "{text}");
        // The build side is the join's first input: the first scan below the join.
        let build = &text[join..][text[join..].find("DataSourceExec").unwrap()..];
        let build = build.lines().next().unwrap();
        assert!(build.contains("campaigns/a.parquet"), "build side is not the small table:\n{text}");

        let rows = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 20);
    }
}
