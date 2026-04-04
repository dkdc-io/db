#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("turso error: {0}")]
    Turso(#[from] turso::Error),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("write attempted through read path: {0}")]
    WriteOnReadPath(String),

    #[error("read attempted through write path: {0}")]
    ReadOnWritePath(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

const MAX_DB_NAME_LEN: usize = 128;
const MAX_SQL_LEN: usize = 10 * 1024 * 1024; // 10 MB
const MAX_TABLE_NAME_LEN: usize = 128;

/// Validate a database name. Allows alphanumeric, hyphens, underscores, and forward slashes
/// (for nested names like `project/mydb`). Rejects empty names, path traversal, and control chars.
pub fn validate_db_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Validation("database name cannot be empty".into()));
    }
    if name.len() > MAX_DB_NAME_LEN {
        return Err(Error::Validation(format!(
            "database name exceeds max length of {MAX_DB_NAME_LEN}"
        )));
    }
    if name.contains("..") {
        return Err(Error::Validation(
            "database name cannot contain '..'".into(),
        ));
    }
    if name.starts_with('/') || name.starts_with('.') {
        return Err(Error::Validation(
            "database name cannot start with '/' or '.'".into(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '/')
    {
        return Err(Error::Validation(
            "database name may only contain alphanumeric characters, hyphens, underscores, and forward slashes".into(),
        ));
    }
    if name.starts_with('/') || name.ends_with('/') || name.contains("//") {
        return Err(Error::Validation(
            "database name has invalid slash placement".into(),
        ));
    }
    Ok(())
}

/// Validate SQL input size.
pub fn validate_sql(sql: &str) -> Result<()> {
    if sql.len() > MAX_SQL_LEN {
        return Err(Error::Validation(format!(
            "SQL exceeds max length of {MAX_SQL_LEN} bytes"
        )));
    }
    if sql.trim().is_empty() {
        return Err(Error::Validation("SQL cannot be empty".into()));
    }
    validate_sql_safety(sql)?;
    Ok(())
}

/// Reject obviously dangerous SQL patterns.
/// This is a lightweight defense layer — turso handles parameterization,
/// but we reject patterns that should never appear in legitimate queries.
fn validate_sql_safety(sql: &str) -> Result<()> {
    let upper = sql.to_uppercase();

    // Reject stacked queries (multiple statements separated by semicolons).
    // Allow trailing semicolons but reject "SELECT 1; DROP TABLE x".
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.contains(';') {
        return Err(Error::Validation(
            "multiple SQL statements are not allowed".into(),
        ));
    }

    // Reject ATTACH DATABASE — could open arbitrary files
    if upper.contains("ATTACH") && upper.contains("DATABASE") {
        return Err(Error::Validation("ATTACH DATABASE is not allowed".into()));
    }

    // Reject LOAD_EXTENSION — could load arbitrary shared libraries
    if upper.contains("LOAD_EXTENSION") {
        return Err(Error::Validation("LOAD_EXTENSION is not allowed".into()));
    }

    Ok(())
}

/// Validate a table name for use in schema introspection queries.
pub fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Validation("table name cannot be empty".into()));
    }
    if name.len() > MAX_TABLE_NAME_LEN {
        return Err(Error::Validation(format!(
            "table name exceeds max length of {MAX_TABLE_NAME_LEN}"
        )));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(Error::Validation(
            "table name may only contain alphanumeric characters and underscores".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = Error::Schema("test".into());
        assert_eq!(e.to_string(), "schema error: test");

        let e = Error::Validation("bad input".into());
        assert_eq!(e.to_string(), "validation error: bad input");

        let e = Error::WriteOnReadPath("INSERT".into());
        assert_eq!(e.to_string(), "write attempted through read path: INSERT");

        let e = Error::ReadOnWritePath("SELECT".into());
        assert_eq!(e.to_string(), "read attempted through write path: SELECT");
    }

    #[test]
    fn error_is_debug() {
        let e = Error::Schema("test".into());
        let debug = format!("{e:?}");
        assert!(debug.contains("Schema"));
    }

    #[test]
    fn sql_safety_rejects_stacked_queries() {
        assert!(validate_sql("SELECT 1; DROP TABLE users").is_err());
        assert!(validate_sql("SELECT 1;SELECT 2").is_err());
    }

    #[test]
    fn sql_safety_allows_trailing_semicolon() {
        assert!(validate_sql("SELECT 1;").is_ok());
        assert!(validate_sql("SELECT 1 ;  ").is_ok());
    }

    #[test]
    fn sql_safety_rejects_attach_database() {
        assert!(validate_sql("ATTACH DATABASE '/etc/passwd' AS pw").is_err());
        assert!(validate_sql("attach database 'foo.db' as bar").is_err());
    }

    #[test]
    fn sql_safety_rejects_load_extension() {
        assert!(validate_sql("SELECT load_extension('evil.so')").is_err());
        assert!(validate_sql("SELECT LOAD_EXTENSION('/tmp/lib')").is_err());
    }

    #[test]
    fn sql_safety_allows_normal_queries() {
        assert!(validate_sql("SELECT * FROM users WHERE id = 1").is_ok());
        assert!(validate_sql("INSERT INTO logs (msg) VALUES ('hello')").is_ok());
        assert!(validate_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)").is_ok());
    }
}
