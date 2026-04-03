use std::path::PathBuf;

use arrow::record_batch::RecordBatch;

use crate::config::DbConfig;
use crate::convert::rows_to_record_batch_with_first;
use crate::error::Result;
use crate::read::ReadEngine;
use crate::router;
use crate::schema;
use crate::write::WriteEngine;

pub struct DkdcDb {
    write: WriteEngine,
    read: ReadEngine,
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
        let read = ReadEngine::new(db.clone());

        // Register existing tables with DataFusion
        read.register_tables().await?;

        Ok(Self { write, read, db })
    }

    /// Open an in-memory database (for testing).
    /// Turso connections from the same Database share the same in-memory data.
    pub async fn open_in_memory() -> Result<Self> {
        let db = turso::Builder::new_local(":memory:").build().await?;
        let write_conn = db.connect()?;

        // Enable WAL mode
        write_conn.pragma_update("journal_mode", "'wal'").await?;

        let write = WriteEngine::new(write_conn);
        let read = ReadEngine::new(db.clone());

        Ok(Self { write, read, db })
    }

    /// Execute a write statement (CREATE, INSERT, UPDATE, DELETE).
    /// Returns the number of rows affected.
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        let result = self.write.execute(sql).await?;
        if router::is_ddl(sql) {
            self.read.refresh_schema().await?;
        }
        Ok(result)
    }

    /// Execute a read query through DataFusion. Always routes through DataFusion.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.read.query(sql).await
    }

    /// Execute a read query directly through turso (fast path for point reads).
    pub async fn query_turso(&self, sql: &str) -> Result<Vec<RecordBatch>> {
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
        for (i, name) in col_names.iter().enumerate() {
            let value = first_row.get_value(i)?;
            let dt = match value {
                turso::Value::Integer(_) => arrow::datatypes::DataType::Int64,
                turso::Value::Real(_) => arrow::datatypes::DataType::Float64,
                turso::Value::Text(_) => arrow::datatypes::DataType::Utf8,
                turso::Value::Blob(_) => arrow::datatypes::DataType::Binary,
                turso::Value::Null => arrow::datatypes::DataType::Utf8,
            };
            fields.push(arrow::datatypes::Field::new(name, dt, true));
        }
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));

        let batch = rows_to_record_batch_with_first(&first_row, &mut rows, schema).await?;
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }
        Ok(vec![batch])
    }

    /// Refresh DataFusion's view of the schema.
    pub async fn refresh_schema(&self) -> Result<()> {
        self.read.refresh_schema().await
    }

    /// Get a DataFusion DataFrame for a table.
    pub async fn table(&self, name: &str) -> Result<datafusion::dataframe::DataFrame> {
        self.read.table(name).await
    }

    /// List all user tables in the database.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.db.connect()?;
        schema::list_tables(&conn).await
    }

    /// Get the path to the database file (if file-backed).
    pub fn path(&self) -> Option<PathBuf> {
        None
    }

    /// Get a reference to the underlying turso::Database.
    pub fn turso_db(&self) -> &turso::Database {
        &self.db
    }
}
