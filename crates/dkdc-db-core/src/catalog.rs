use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::provider::SqliteTableProvider;
use crate::schema;

/// One catalog per database. Contains a single "public" schema.
pub struct SqliteCatalogProvider {
    schema: Arc<SqliteSchemaProvider>,
}

impl SqliteCatalogProvider {
    pub fn new(db: turso::Database) -> Self {
        Self {
            schema: Arc::new(SqliteSchemaProvider::new(db)),
        }
    }

    pub fn schema_provider(&self) -> &Arc<SqliteSchemaProvider> {
        &self.schema
    }
}

impl fmt::Debug for SqliteCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteCatalogProvider").finish()
    }
}

impl CatalogProvider for SqliteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            "public" => Some(self.schema.clone()),
            _ => None,
        }
    }
}

/// Lists and provides tables from one turso::Database.
pub struct SqliteSchemaProvider {
    db: turso::Database,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl SqliteSchemaProvider {
    pub fn new(db: turso::Database) -> Self {
        Self {
            db,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Discover tables from sqlite_master and register SqliteTableProviders.
    pub async fn refresh(&self) -> Result<()> {
        let conn = self.db.connect()?;
        let table_names = schema::list_tables(&conn).await?;
        let mut map = HashMap::new();
        for name in &table_names {
            let columns = schema::introspect_table(&conn, name).await?;
            let arrow_schema = schema::build_arrow_schema(&columns);
            let provider = SqliteTableProvider::new(name.clone(), arrow_schema, self.db.clone());
            map.insert(name.clone(), Arc::new(provider) as Arc<dyn TableProvider>);
        }
        *self.tables.write().await = map;
        Ok(())
    }
}

impl fmt::Debug for SqliteSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteSchemaProvider").finish()
    }
}

#[async_trait]
impl SchemaProvider for SqliteSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // SchemaProvider::table_names is sync, so use try_read.
        // If locked, return empty (rare, transient during refresh).
        self.tables
            .try_read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.read().await.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .try_read()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }
}
