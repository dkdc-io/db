use arrow::record_batch::RecordBatch;

use crate::config::DbConfig;
use crate::convert::rows_to_record_batch_with_first;
use crate::error::{Error, Result};
use crate::router;
use crate::schema;
use crate::write::WriteEngine;

/// Try to extract a table name from a simple SELECT query for PRAGMA fallback.
/// Handles `SELECT ... FROM table_name` patterns.
fn extract_table_name(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let from_idx = upper.find(" FROM ")?;
    let after_from = sql[from_idx + 6..].trim_start();
    // Take the first word (table name), stop at whitespace, comma, semicolon, or parenthesis
    let name: String = after_from
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() { None } else { Some(name) }
}

pub struct DkdcDb {
    write: WriteEngine,
    db: turso::Database,
}

impl DkdcDb {
    /// Open or create a database at `~/.dkdc/db/{name}.db`.
    pub async fn open(name: &str) -> Result<Self> {
        let config = DbConfig::default_for(name)?;
        Self::open_with_config(config).await
    }

    /// Open or create a database at a custom path.
    pub async fn open_with_config(config: DbConfig) -> Result<Self> {
        let db = turso::Builder::new_local(config.path.to_string_lossy().as_ref())
            .build()
            .await?;

        let write_conn = db.connect()?;

        // Enable WAL mode for concurrent read+write
        write_conn.pragma_update("journal_mode", "'wal'").await?;

        let write = WriteEngine::new(write_conn);

        Ok(Self { write, db })
    }

    /// Open an in-memory database (for testing).
    /// Turso connections from the same Database share the same in-memory data.
    pub async fn open_in_memory() -> Result<Self> {
        let db = turso::Builder::new_local(":memory:").build().await?;
        let write_conn = db.connect()?;

        // Enable WAL mode
        write_conn.pragma_update("journal_mode", "'wal'").await?;

        let write = WriteEngine::new(write_conn);

        Ok(Self { write, db })
    }

    /// Execute a write statement (CREATE, INSERT, UPDATE, DELETE).
    /// Returns the number of rows affected.
    /// DDL detection is handled by DbManager (it needs to refresh the catalog).
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        self.write.execute(sql).await
    }

    /// OLTP fast-path read. Direct turso execution, no DataFusion.
    pub async fn query_oltp(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        if router::is_write(sql) {
            return Err(Error::WriteOnReadPath(sql.to_string()));
        }
        let conn = self.db.connect()?;
        let mut rows = conn.query(sql, ()).await?;

        let col_count = rows.column_count();
        if col_count == 0 {
            return Ok(vec![]);
        }

        // Get column names
        let col_names: Vec<String> = (0..col_count)
            .map(|i| {
                rows.column_name(i)
                    .unwrap_or_else(|_| format!("column_{i}"))
            })
            .collect();

        // Peek first row to infer types from actual values
        let first_row = match rows.next().await? {
            Some(row) => row,
            None => return Ok(vec![]),
        };

        // Infer Arrow types from the first row's values
        let mut fields = Vec::new();
        let mut has_nulls = false;
        for (i, name) in col_names.iter().enumerate() {
            let value = first_row.get_value(i)?;
            let dt = match value {
                turso::Value::Integer(_) => arrow::datatypes::DataType::Int64,
                turso::Value::Real(_) => arrow::datatypes::DataType::Float64,
                turso::Value::Text(_) => arrow::datatypes::DataType::Utf8,
                turso::Value::Blob(_) => arrow::datatypes::DataType::Binary,
                turso::Value::Null => {
                    has_nulls = true;
                    arrow::datatypes::DataType::Utf8 // placeholder, may be refined below
                }
            };
            fields.push(arrow::datatypes::Field::new(name, dt, true));
        }

        // If any columns were NULL, try to refine types via PRAGMA table_info
        if has_nulls {
            if let Some(table_name) = extract_table_name(sql) {
                if let Ok(columns) = schema::introspect_table(&conn, &table_name).await {
                    // Build a name->type map from the schema
                    let type_map: std::collections::HashMap<&str, &arrow::datatypes::DataType> =
                        columns
                            .iter()
                            .map(|c| (c.name.as_str(), &c.data_type))
                            .collect();
                    for field in &mut fields {
                        if first_row
                            .get_value(
                                col_names
                                    .iter()
                                    .position(|n| n == field.name())
                                    .unwrap_or(0),
                            )
                            .map(|v| matches!(v, turso::Value::Null))
                            .unwrap_or(false)
                        {
                            if let Some(&dt) = type_map.get(field.name().as_str()) {
                                *field =
                                    arrow::datatypes::Field::new(field.name(), dt.clone(), true);
                            }
                        }
                    }
                }
            }
        }

        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));

        let batch = rows_to_record_batch_with_first(&first_row, &mut rows, schema).await?;
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }
        Ok(vec![batch])
    }

    /// List all user tables in the database.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.db.connect()?;
        schema::list_tables(&conn).await
    }

    /// Expose turso::Database for catalog registration.
    pub fn turso_db(&self) -> &turso::Database {
        &self.db
    }

    /// Create a new connection to the underlying database.
    pub fn connect(&self) -> Result<turso::Connection> {
        Ok(self.db.connect()?)
    }
}
