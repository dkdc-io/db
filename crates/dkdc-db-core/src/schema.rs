use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{self, Error, Result};

pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Map a SQLite declared type to an Arrow DataType using SQLite type affinity rules.
pub fn sqlite_type_to_arrow(declared_type: Option<&str>) -> DataType {
    match declared_type {
        None => DataType::Utf8,
        Some(t) => {
            let upper = t.to_uppercase();
            if upper.contains("INT") {
                DataType::Int64
            } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
                DataType::Float64
            } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
                DataType::Utf8
            } else if upper.contains("BLOB") {
                DataType::Binary
            } else {
                DataType::Utf8
            }
        }
    }
}

/// Introspect a table's schema via PRAGMA table_info.
pub async fn introspect_table(
    conn: &turso::Connection,
    table_name: &str,
) -> Result<Vec<ColumnInfo>> {
    error::validate_table_name(table_name)?;
    // Table name is validated above (alphanumeric + underscores only), safe for interpolation
    let sql = format!("PRAGMA table_info('{table_name}')");
    let mut rows = conn.query(&sql, ()).await?;
    let mut columns = Vec::new();

    while let Some(row) = rows.next().await? {
        let name: String = row.get(1)?;
        let type_str: Option<String> = row.get(2).ok();
        let notnull: i64 = row.get(3)?;

        columns.push(ColumnInfo {
            name,
            data_type: sqlite_type_to_arrow(type_str.as_deref()),
            nullable: notnull == 0,
        });
    }

    if columns.is_empty() {
        return Err(Error::Schema(format!("table '{table_name}' not found")));
    }

    Ok(columns)
}

/// Build an Arrow Schema from column info.
pub fn build_arrow_schema(columns: &[ColumnInfo]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

/// List all user tables in the database.
pub async fn list_tables(conn: &turso::Connection) -> Result<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            (),
        )
        .await?;
    let mut tables = Vec::new();
    while let Some(row) = rows.next().await? {
        let name: String = row.get(0)?;
        tables.push(name);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mapping() {
        assert_eq!(sqlite_type_to_arrow(Some("INTEGER")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("BIGINT")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("SMALLINT")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("REAL")), DataType::Float64);
        assert_eq!(sqlite_type_to_arrow(Some("DOUBLE")), DataType::Float64);
        assert_eq!(sqlite_type_to_arrow(Some("FLOAT")), DataType::Float64);
        assert_eq!(sqlite_type_to_arrow(Some("TEXT")), DataType::Utf8);
        assert_eq!(sqlite_type_to_arrow(Some("VARCHAR(255)")), DataType::Utf8);
        assert_eq!(sqlite_type_to_arrow(Some("BLOB")), DataType::Binary);
        assert_eq!(sqlite_type_to_arrow(None), DataType::Utf8);
        assert_eq!(sqlite_type_to_arrow(Some("UNKNOWN_TYPE")), DataType::Utf8);
    }

    #[test]
    fn test_type_mapping_case_insensitive() {
        assert_eq!(sqlite_type_to_arrow(Some("integer")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("Integer")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("real")), DataType::Float64);
        assert_eq!(sqlite_type_to_arrow(Some("text")), DataType::Utf8);
        assert_eq!(sqlite_type_to_arrow(Some("blob")), DataType::Binary);
    }

    #[test]
    fn test_type_mapping_compound_types() {
        // SQLite affinity rules: INT anywhere in the name -> INTEGER
        assert_eq!(sqlite_type_to_arrow(Some("TINYINT")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("MEDIUMINT")), DataType::Int64);
        assert_eq!(sqlite_type_to_arrow(Some("INT8")), DataType::Int64);
        assert_eq!(
            sqlite_type_to_arrow(Some("UNSIGNED BIG INT")),
            DataType::Int64
        );

        // CHAR/CLOB
        assert_eq!(sqlite_type_to_arrow(Some("CHARACTER(20)")), DataType::Utf8);
        assert_eq!(
            sqlite_type_to_arrow(Some("VARYING CHARACTER(255)")),
            DataType::Utf8
        );
        assert_eq!(sqlite_type_to_arrow(Some("CLOB")), DataType::Utf8);
        assert_eq!(
            sqlite_type_to_arrow(Some("NATIVE CHARACTER(70)")),
            DataType::Utf8
        );

        // REAL/FLOAT/DOUBLE
        assert_eq!(
            sqlite_type_to_arrow(Some("DOUBLE PRECISION")),
            DataType::Float64
        );
        assert_eq!(sqlite_type_to_arrow(Some("FLOAT")), DataType::Float64);
    }

    #[test]
    fn test_type_mapping_empty_string() {
        assert_eq!(sqlite_type_to_arrow(Some("")), DataType::Utf8);
    }

    #[test]
    fn test_build_arrow_schema() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ];
        let schema = build_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_build_arrow_schema_empty() {
        let columns: Vec<ColumnInfo> = vec![];
        let schema = build_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 0);
    }
}
