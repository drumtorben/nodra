//! Type handling: column definitions in source calls and Delta-compatible output types.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::{NodraError, Result};

/// Parse column definitions such as `ts TIMESTAMP, id BIGINT, tags VARCHAR` into an Arrow
/// schema, using DataFusion's own SQL type mapping. All columns are nullable.
pub async fn parse_columns(ddl: &str) -> Result<SchemaRef> {
    let stmts = Parser::parse_sql(&GenericDialect {}, &format!("CREATE TABLE t ({ddl})"))
        .map_err(|e| NodraError::job(format!("invalid schema `{ddl}`: {e}")))?;
    let columns = match stmts.first() {
        Some(Statement::CreateTable(ct)) => ct.columns.clone(),
        _ => return Err(NodraError::job(format!("invalid schema `{ddl}`"))),
    };
    if columns.is_empty() {
        return Err(NodraError::job("schema must declare at least one column"));
    }
    let select = columns
        .iter()
        .map(|c| {
            let name = c.name.value.replace('"', "\"\"");
            format!("CAST(NULL AS {}) AS \"{name}\"", c.data_type)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let df = SessionContext::new()
        .sql(&format!("SELECT {select}"))
        .await
        .map_err(|e| NodraError::job(format!("invalid schema `{ddl}`: {e}")))?;
    let fields: Vec<Field> = df
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

/// Map an Arrow type to the type Nodra writes to Delta.
///
/// Delta has no unsigned integers, string/binary views, nanosecond timestamps or dictionaries,
/// so those are widened or unwrapped (`UINT64` becomes `BIGINT`; larger values fail the cast). Timestamps are written as Delta `timestamp`
/// (microseconds, UTC); timestamps without a time zone are interpreted as UTC.
pub fn delta_type(dt: &DataType) -> Result<DataType> {
    Ok(match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::Date32
        | DataType::Decimal128(_, _) => dt.clone(),
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        // Counts such as approx_distinct are UInt64; values above i64::MAX fail the cast loudly.
        DataType::UInt64 => DataType::Int64,
        DataType::Float16 => DataType::Float32,
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::BinaryView | DataType::LargeBinary | DataType::FixedSizeBinary(_) => DataType::Binary,
        DataType::Date64 => DataType::Date32,
        DataType::Timestamp(_, _) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Dictionary(_, v) => delta_type(v)?,
        DataType::List(f) | DataType::LargeList(f) | DataType::ListView(f) | DataType::LargeListView(f) => {
            DataType::List(Arc::new(delta_field(f)?))
        }
        DataType::FixedSizeList(f, _) => DataType::List(Arc::new(delta_field(f)?)),
        DataType::Struct(fields) => DataType::Struct(
            fields.iter().map(|f| delta_field(f)).collect::<Result<Fields>>()?,
        ),
        DataType::Map(f, sorted) => DataType::Map(Arc::new(delta_field(f)?), *sorted),
        DataType::Null => {
            return Err(NodraError::job(
                "a result column has type NULL; cast it explicitly, e.g. CAST(NULL AS VARCHAR)",
            ))
        }
        other => {
            return Err(NodraError::job(format!(
                "result type {other} cannot be stored in Delta; cast it to a supported type"
            )))
        }
    })
}

fn delta_field(f: &Field) -> Result<Field> {
    Ok(Field::new(f.name(), delta_type(f.data_type())?, f.is_nullable()))
}

/// The schema Nodra writes to Delta for query output `schema`. All columns are nullable,
/// so that schema evolution of the query does not trip over nullability.
pub fn delta_schema(schema: &Schema) -> Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            validate_column_name(f.name())?;
            Ok(Field::new(f.name(), delta_type(f.data_type())?, true))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Delta (without column mapping) forbids some characters in column names.
pub fn validate_column_name(name: &str) -> Result<()> {
    const BAD: &[char] = &[' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '='];
    if name.is_empty() || name.contains(BAD) {
        return Err(NodraError::job(format!(
            "output column `{name}` is not a valid Delta column name; give it an alias \
             (e.g. `count(*) AS n`) without spaces or any of ,;{{}}()="
        )));
    }
    Ok(())
}

/// Cast `batch` to `target` (same column order and count).
pub fn cast_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema().fields().len() != target.fields().len() {
        return Err(NodraError::job(format!(
            "result has {} columns, expected {}",
            batch.schema().fields().len(),
            target.fields().len()
        )));
    }
    let opts = CastOptions { safe: false, ..Default::default() };
    let cols = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c.clone())
            } else {
                cast_with_options(c, f.data_type(), &opts).map_err(NodraError::from)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(target.clone(), cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parses_columns() {
        let s = parse_columns("ts TIMESTAMP, id BIGINT, name VARCHAR, ok BOOLEAN, d DATE, amount DECIMAL(10,2)")
            .await
            .unwrap();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["ts", "id", "name", "ok", "d", "amount"]);
        assert_eq!(s.field(1).data_type(), &DataType::Int64);
        assert!(matches!(s.field(0).data_type(), DataType::Timestamp(_, _)));
        assert_eq!(s.field(5).data_type(), &DataType::Decimal128(10, 2));
        assert!(s.fields().iter().all(|f| f.is_nullable()));
        assert!(parse_columns("").await.is_err());
        assert!(parse_columns("x NOTATYPE").await.is_err());
    }

    #[test]
    fn delta_types() {
        assert_eq!(delta_type(&DataType::UInt32).unwrap(), DataType::Int64);
        assert_eq!(delta_type(&DataType::Utf8View).unwrap(), DataType::Utf8);
        assert_eq!(
            delta_type(&DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(delta_type(&DataType::Null).is_err());
        assert!(validate_column_name("count(*)").is_err());
        assert!(validate_column_name("n").is_ok());
    }
}
