use std::path::PathBuf;

use arrow::record_batch::RecordBatch;

use crate::config::DbConfig;
use crate::convert::rows_to_record_batch;
use crate::error::Result;
use crate::read::ReadEngine;
use crate::router;
use crate::schema;
use crate::write::WriteEngine;

pub struct DkdcDb {
    write: WriteEngine,
    read: ReadEngine,
    db: libsql::Database,
}

impl DkdcDb {
    /// Open or create a database at `~/.dkdc/db/{name}.db`.
    pub async fn open(name: &str) -> Result<Self> {
        let config = DbConfig::default_for(name)?;
        Self::open_with_config(config).await
    }

    /// Open or create a database at a custom path.
    pub async fn open_with_config(config: DbConfig) -> Result<Self> {
        let db = libsql::Builder::new_local(config.path.to_string_lossy().as_ref())
            .build()
            .await?;

        let write_conn = db.connect()?;
        let read_conn = db.connect()?;

        // Enable WAL mode for concurrent read+write
        // Use query since PRAGMA returns rows
        let _ = write_conn.query("PRAGMA journal_mode=WAL", ()).await;

        let write = WriteEngine::new(write_conn);
        let read = ReadEngine::new(read_conn);

        // Register existing tables with DataFusion
        read.register_tables().await?;

        Ok(Self { write, read, db })
    }

    /// Open an in-memory database (for testing).
    /// Uses a shared named in-memory database so both connections see the same data.
    pub async fn open_in_memory() -> Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let uri = format!("file:dkdc_mem_{id}?mode=memory&cache=shared");

        let db = libsql::Builder::new_local(&uri).build().await?;
        let write_conn = db.connect()?;
        let read_conn = db.connect()?;

        // Enable WAL mode
        let _ = write_conn.query("PRAGMA journal_mode=WAL", ()).await;

        let write = WriteEngine::new(write_conn);
        let read = ReadEngine::new(read_conn);

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

    /// Execute a read query directly through libSQL (fast path for point reads).
    pub async fn query_libsql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut rows = self.read.conn().query(sql, ()).await?;

        // We need to infer schema from the query result
        let col_count = rows.column_count();
        if col_count == 0 {
            return Ok(vec![]);
        }

        // Build schema from column info
        let mut fields = Vec::new();
        for i in 0..col_count {
            let name = rows
                .column_name(i)
                .unwrap_or(&format!("column_{i}"))
                .to_string();
            let col_type = rows
                .column_type(i)
                .ok()
                .map(|t| match t {
                    libsql::ValueType::Integer => arrow::datatypes::DataType::Int64,
                    libsql::ValueType::Real => arrow::datatypes::DataType::Float64,
                    libsql::ValueType::Text => arrow::datatypes::DataType::Utf8,
                    libsql::ValueType::Blob => arrow::datatypes::DataType::Binary,
                    libsql::ValueType::Null => arrow::datatypes::DataType::Utf8,
                })
                .unwrap_or(arrow::datatypes::DataType::Utf8);
            fields.push(arrow::datatypes::Field::new(name, col_type, true));
        }
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));

        let batch = rows_to_record_batch(&mut rows, schema).await?;
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
        schema::list_tables(self.read.conn()).await
    }

    /// Get the path to the database file (if file-backed).
    pub fn path(&self) -> Option<PathBuf> {
        // The database keeps track internally, but we don't expose it directly
        None
    }

    /// Get a reference to the underlying libsql::Database.
    pub fn libsql_db(&self) -> &libsql::Database {
        &self.db
    }
}
