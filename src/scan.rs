//! A DataFusion table over an explicit list of Parquet files with Delta partition values.
//!
//! Each worker registers its share of the driving table (and broadcast tables in full) as a
//! [`FileListTable`]; the files are read directly from the table's object store.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DfResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
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
        Ok(Self { file_schema, partition_fields, schema, store_url, files })
    }

    /// Number of files.
    pub fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Mark this table as the driving source (see [`crate::partial`]).
    pub fn mark_driving(mut self) -> Self {
        self.file_schema = crate::partial::mark_driving(&self.file_schema);
        self.schema = crate::partial::mark_driving(&self.schema);
        self
    }
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if self.files.is_empty() {
            let schema = match projection {
                Some(p) => Arc::new(self.schema.project(p)?),
                None => self.schema.clone(),
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        }
        let groups_n = state.config().target_partitions().max(1).min(self.files.len());
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); groups_n];
        for (i, f) in self.files.iter().enumerate() {
            groups[i % groups_n].push(f.clone());
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
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}
