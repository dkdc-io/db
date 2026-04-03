use crate::error::{Error, Result};
use crate::router;

pub struct WriteEngine {
    conn: turso::Connection,
}

impl WriteEngine {
    pub fn new(conn: turso::Connection) -> Self {
        Self { conn }
    }

    pub async fn execute(&self, sql: &str) -> Result<u64> {
        if router::is_read(sql) && !sql.trim_start().to_uppercase().starts_with("PRAGMA") {
            return Err(Error::ReadOnWritePath(sql.to_string()));
        }
        Ok(self.conn.execute(sql, ()).await?)
    }
}
