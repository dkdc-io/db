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
}
