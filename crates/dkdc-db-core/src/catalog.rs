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
            if let Some(name) = extract_ddl_table_name(sql) {
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
            if let Some(name) = extract_ddl_table_name(sql) {
                self.tables.write().await.remove(&name);
                return Ok(());
            }
        } else if upper.starts_with("ALTER TABLE") {
            if let Some(name) = extract_ddl_table_name(sql) {
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

/// Extract the table name from a DDL statement (original case).
/// Handles: CREATE [TEMP] TABLE [IF NOT EXISTS] name, DROP TABLE [IF EXISTS] name,
/// ALTER TABLE name. Supports quoted identifiers (double quotes, backticks, single
/// quotes) including those containing spaces, and schema-qualified names (extracts
/// the last segment after dot).
fn extract_ddl_table_name(sql: &str) -> Option<String> {
    let sql = sql.trim_start();

    // Find the keyword position after TABLE where the name begins.
    // We do case-insensitive matching on keywords but preserve the name's original case.
    let upper = sql.to_uppercase();
    let tokens_upper: Vec<&str> = upper.split_whitespace().collect();

    let skip_keywords = if tokens_upper.first() == Some(&"CREATE") {
        let table_pos = tokens_upper.iter().position(|t| *t == "TABLE")?;
        let mut skip = table_pos + 1;
        if tokens_upper.get(skip) == Some(&"IF") {
            skip += 3; // IF NOT EXISTS
        }
        skip
    } else if tokens_upper.first() == Some(&"DROP") {
        let table_pos = tokens_upper.iter().position(|t| *t == "TABLE")?;
        let mut skip = table_pos + 1;
        if tokens_upper.get(skip) == Some(&"IF") {
            skip += 2; // IF EXISTS
        }
        skip
    } else if tokens_upper.first() == Some(&"ALTER") {
        let table_pos = tokens_upper.iter().position(|t| *t == "TABLE")?;
        table_pos + 1
    } else {
        return None;
    };

    // Now walk the original SQL to skip that many whitespace-delimited tokens,
    // then extract the name (which may be quoted and contain spaces).
    let mut pos = 0;
    let bytes = sql.as_bytes();
    let len = bytes.len();

    for _ in 0..skip_keywords {
        // Skip whitespace
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        // Skip token
        while pos < len && !bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
    }

    // Skip whitespace before the name
    while pos < len && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }

    if pos >= len {
        return None;
    }

    // Extract the name token. If it starts with a quote, read until the matching
    // closing quote (handling dot-separated quoted segments).
    let raw_name = extract_name_token(&sql[pos..])?;

    // Handle schema-qualified names: take the last segment after dot
    let table_part = if let Some(dot_pos) = raw_name.rfind('.') {
        &raw_name[dot_pos + 1..]
    } else {
        &raw_name
    };

    // Strip surrounding quotes
    let name = strip_quotes(table_part);
    if name.is_empty() {
        return None;
    }

    Some(name.to_lowercase())
}

/// Extract a possibly-quoted, possibly-schema-qualified name token from the
/// start of `s`. Returns the raw token including any quotes and dots.
fn extract_name_token(s: &str) -> Option<String> {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    loop {
        let Some(&ch) = chars.peek() else {
            break; // end of input
        };

        if ch == '"' || ch == '`' || ch == '\'' {
            // Quoted segment: read until matching close quote
            let quote = ch;
            result.push(chars.next().unwrap());
            loop {
                let c = chars.next()?; // unclosed quote → None
                result.push(c);
                if c == quote {
                    break;
                }
            }
        } else if ch.is_ascii_whitespace() || ch == '(' || ch == ';' {
            // End of name token
            break;
        } else {
            result.push(chars.next().unwrap());
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Strip one layer of surrounding quotes (double quotes, backticks, or single quotes).
fn strip_quotes(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"')
            || (first == b'`' && last == b'`')
            || (first == b'\'' && last == b'\'')
        {
            return &s[1..s.len() - 1];
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_create() {
        assert_eq!(
            extract_ddl_table_name("CREATE TABLE users (id INTEGER)"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_create_if_not_exists() {
        assert_eq!(
            extract_ddl_table_name("CREATE TABLE IF NOT EXISTS users (id INTEGER)"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_create_temp_table() {
        assert_eq!(
            extract_ddl_table_name("CREATE TEMP TABLE staging (id INTEGER)"),
            Some("staging".to_string())
        );
    }

    #[test]
    fn test_drop_table() {
        assert_eq!(
            extract_ddl_table_name("DROP TABLE users"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_drop_table_if_exists() {
        assert_eq!(
            extract_ddl_table_name("DROP TABLE IF EXISTS users"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_alter_table() {
        assert_eq!(
            extract_ddl_table_name("ALTER TABLE users ADD COLUMN email TEXT"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_quoted_double() {
        assert_eq!(
            extract_ddl_table_name(r#"CREATE TABLE "MyTable" (id INTEGER)"#),
            Some("mytable".to_string())
        );
    }

    #[test]
    fn test_quoted_backtick() {
        assert_eq!(
            extract_ddl_table_name("CREATE TABLE `MyTable` (id INTEGER)"),
            Some("mytable".to_string())
        );
    }

    #[test]
    fn test_quoted_with_spaces() {
        assert_eq!(
            extract_ddl_table_name(r#"CREATE TABLE "my table" (id INTEGER)"#),
            Some("my table".to_string())
        );
    }

    #[test]
    fn test_schema_qualified() {
        assert_eq!(
            extract_ddl_table_name("CREATE TABLE myschema.mytable (id INTEGER)"),
            Some("mytable".to_string())
        );
    }

    #[test]
    fn test_schema_qualified_quoted() {
        assert_eq!(
            extract_ddl_table_name(r#"CREATE TABLE "myschema"."MyTable" (id INTEGER)"#),
            Some("mytable".to_string())
        );
    }

    #[test]
    fn test_case_insensitive_keywords() {
        assert_eq!(
            extract_ddl_table_name("create table Users (id INTEGER)"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_leading_whitespace() {
        assert_eq!(
            extract_ddl_table_name("  CREATE TABLE users (id INTEGER)"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_no_table_keyword() {
        assert_eq!(extract_ddl_table_name("SELECT * FROM users"), None);
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(extract_ddl_table_name(""), None);
    }

    #[test]
    fn test_name_followed_by_semicolon() {
        assert_eq!(
            extract_ddl_table_name("DROP TABLE users;"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_name_with_no_trailing() {
        assert_eq!(
            extract_ddl_table_name("DROP TABLE users"),
            Some("users".to_string())
        );
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
