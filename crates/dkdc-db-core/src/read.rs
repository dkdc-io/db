use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::error::{Error, Result};
use crate::provider::SqliteTableProvider;
use crate::router;
use crate::schema;

pub struct ReadEngine {
    ctx: SessionContext,
    db: turso::Database,
}

impl ReadEngine {
    pub fn new(db: turso::Database) -> Self {
        Self {
            ctx: SessionContext::new(),
            db,
        }
    }

    /// Create a fresh connection for this operation.
    fn connect(&self) -> Result<turso::Connection> {
        Ok(self.db.connect()?)
    }

    /// Register all user tables from the database with DataFusion.
    pub async fn register_tables(&self) -> Result<()> {
        let conn = self.connect()?;
        let tables = schema::list_tables(&conn).await?;
        for table_name in &tables {
            self.register_table(table_name).await?;
        }
        Ok(())
    }

    /// Register a single table with DataFusion.
    async fn register_table(&self, table_name: &str) -> Result<()> {
        let conn = self.connect()?;
        let columns = schema::introspect_table(&conn, table_name).await?;
        let arrow_schema = schema::build_arrow_schema(&columns);
        let provider =
            SqliteTableProvider::new(table_name.to_string(), arrow_schema, self.db.clone());
        self.ctx
            .register_table(table_name, Arc::new(provider))
            .map_err(|e| Error::Schema(format!("failed to register table '{table_name}': {e}")))?;
        Ok(())
    }

    /// Refresh schema: deregister all tables then re-register.
    pub async fn refresh_schema(&self) -> Result<()> {
        for catalog_name in self.ctx.catalog_names() {
            if let Some(catalog) = self.ctx.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let table_names: Vec<String> = schema.table_names();
                        for table_name in table_names {
                            let _ = schema.deregister_table(&table_name);
                        }
                    }
                }
            }
        }
        self.register_tables().await
    }

    /// Execute a read query through DataFusion.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        if router::is_write(sql) {
            return Err(Error::WriteOnReadPath(sql.to_string()));
        }
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Get a DataFusion DataFrame for a table.
    pub async fn table(&self, name: &str) -> Result<datafusion::dataframe::DataFrame> {
        Ok(self.ctx.table(name).await?)
    }
}
