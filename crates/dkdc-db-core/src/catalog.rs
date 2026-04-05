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

const DEFAULT_SCHEMA: &str = "public";

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
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            DEFAULT_SCHEMA => Some(self.schema.clone()),
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

    /// Refresh schema for a specific DDL statement. For CREATE TABLE, only registers
    /// the new table. For DROP TABLE, only removes it. For ALTER TABLE, refreshes
    /// just that table. Falls back to full refresh if the SQL can't be parsed.
    pub async fn refresh_for_ddl(&self, sql: &str) -> Result<()> {
        let upper = sql.trim_start().to_uppercase();

        if upper.starts_with("CREATE TABLE") || upper.starts_with("CREATE TEMP TABLE") {
            // Extract table name and add just that table
            if let Some(name) = extract_ddl_table_name(&upper) {
                let conn = self.db.connect()?;
                if let Ok(columns) = schema::introspect_table(&conn, &name).await {
                    let arrow_schema = schema::build_arrow_schema(&columns);
                    let provider =
                        SqliteTableProvider::new(name.clone(), arrow_schema, self.db.clone());
                    self.tables
                        .write()
                        .await
                        .insert(name, Arc::new(provider) as Arc<dyn TableProvider>);
                    return Ok(());
                }
            }
        } else if upper.starts_with("DROP TABLE") {
            if let Some(name) = extract_ddl_table_name(&upper) {
                self.tables.write().await.remove(&name);
                return Ok(());
            }
        } else if upper.starts_with("ALTER TABLE") {
            if let Some(name) = extract_ddl_table_name(&upper) {
                let conn = self.db.connect()?;
                if let Ok(columns) = schema::introspect_table(&conn, &name).await {
                    let arrow_schema = schema::build_arrow_schema(&columns);
                    let provider =
                        SqliteTableProvider::new(name.clone(), arrow_schema, self.db.clone());
                    self.tables
                        .write()
                        .await
                        .insert(name, Arc::new(provider) as Arc<dyn TableProvider>);
                    return Ok(());
                }
            }
        }

        // Fallback: full refresh
        self.refresh().await
    }
}

impl fmt::Debug for SqliteSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteSchemaProvider").finish()
    }
}

/// Extract the table name from a DDL statement (uppercased).
/// Handles: CREATE [TEMP] TABLE [IF NOT EXISTS] name, DROP TABLE [IF EXISTS] name,
/// ALTER TABLE name.
fn extract_ddl_table_name(upper: &str) -> Option<String> {
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    let name_idx = if tokens.first() == Some(&"CREATE") {
        // CREATE [TEMP] TABLE [IF NOT EXISTS] name
        let table_pos = tokens.iter().position(|t| *t == "TABLE")?;
        let mut idx = table_pos + 1;
        // Skip IF NOT EXISTS
        if tokens.get(idx) == Some(&"IF") {
            idx += 3; // skip IF NOT EXISTS
        }
        idx
    } else if tokens.first() == Some(&"DROP") {
        // DROP TABLE [IF EXISTS] name
        let table_pos = tokens.iter().position(|t| *t == "TABLE")?;
        let mut idx = table_pos + 1;
        if tokens.get(idx) == Some(&"IF") {
            idx += 2; // skip IF EXISTS
        }
        idx
    } else if tokens.first() == Some(&"ALTER") {
        // ALTER TABLE name
        let table_pos = tokens.iter().position(|t| *t == "TABLE")?;
        table_pos + 1
    } else {
        return None;
    };

    tokens.get(name_idx).map(|name| {
        // Remove any surrounding quotes or backticks, and convert to lowercase
        name.trim_matches(|c| c == '"' || c == '`' || c == '\'')
            .to_lowercase()
    })
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
