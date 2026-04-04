pub fn is_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_uppercase();
    upper.starts_with("CREATE") || upper.starts_with("ALTER") || upper.starts_with("DROP")
}

pub fn is_write(sql: &str) -> bool {
    let upper = sql.trim_start().to_uppercase();
    is_ddl(sql)
        || upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("REPLACE")
}

pub fn is_read(sql: &str) -> bool {
    let upper = sql.trim_start().to_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("PRAGMA")
        || upper.starts_with("WITH")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_ddl() {
        assert!(is_ddl("CREATE TABLE t (id INTEGER)"));
        assert!(is_ddl("  ALTER TABLE t ADD COLUMN x TEXT"));
        assert!(is_ddl("DROP TABLE t"));
        assert!(!is_ddl("INSERT INTO t VALUES (1)"));
        assert!(!is_ddl("SELECT * FROM t"));
    }

    #[test]
    fn test_is_write() {
        assert!(is_write("INSERT INTO t VALUES (1)"));
        assert!(is_write("UPDATE t SET x = 1"));
        assert!(is_write("DELETE FROM t"));
        assert!(is_write("REPLACE INTO t VALUES (1)"));
        assert!(is_write("CREATE TABLE t (id INTEGER)"));
        assert!(!is_write("SELECT * FROM t"));
    }

    #[test]
    fn test_is_read() {
        assert!(is_read("SELECT * FROM t"));
        assert!(is_read("EXPLAIN SELECT * FROM t"));
        assert!(is_read("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(!is_read("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn test_case_insensitivity() {
        assert!(is_ddl("create table t (id int)"));
        assert!(is_write("insert into t values (1)"));
        assert!(is_read("select 1"));
        assert!(is_ddl("Create Table t (id int)"));
    }

    #[test]
    fn test_leading_whitespace() {
        assert!(is_ddl("  \t\nCREATE TABLE t (id int)"));
        assert!(is_write("  INSERT INTO t VALUES (1)"));
        assert!(is_read("  SELECT 1"));
    }

    #[test]
    fn test_pragma_is_read() {
        assert!(is_read("PRAGMA table_info('t')"));
        assert!(is_read("pragma table_list"));
    }

    #[test]
    fn test_ddl_is_also_write() {
        assert!(is_write("CREATE TABLE t (id int)"));
        assert!(is_write("ALTER TABLE t ADD COLUMN x TEXT"));
        assert!(is_write("DROP TABLE t"));
    }
}
