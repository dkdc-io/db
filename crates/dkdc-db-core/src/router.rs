pub fn is_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_uppercase();
    upper.starts_with("CREATE") || upper.starts_with("ALTER") || upper.starts_with("DROP")
}

pub fn is_write(sql: &str) -> bool {
    if is_ddl(sql) {
        return true;
    }
    let upper = strip_with_prefix(sql);
    upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("REPLACE")
}

pub fn is_read(sql: &str) -> bool {
    let upper = sql.trim_start().to_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("EXPLAIN") || upper.starts_with("PRAGMA") {
        return true;
    }
    if upper.starts_with("WITH") {
        // WITH ... INSERT/UPDATE/DELETE is a write
        return !is_write(sql);
    }
    false
}

/// Strip leading WITH clause(s) and return the remaining SQL uppercased.
/// Used to detect `WITH ... INSERT INTO ...` as a write.
fn strip_with_prefix(sql: &str) -> String {
    let upper = sql.trim_start().to_uppercase();
    if !upper.starts_with("WITH") {
        return upper;
    }
    // Find the final action keyword after WITH clause(s).
    // CTEs are `WITH name AS (...)`, potentially nested parens.
    // We scan past balanced parentheses to find the terminal statement.
    let bytes = upper.as_bytes();
    // We work on the original SQL (preserving case) for string-literal tracking,
    // but use the uppercased version for keyword detection. Both have the same byte
    // length because to_uppercase on ASCII-only SQL is length-preserving.
    let orig_bytes = sql.trim_start().as_bytes();
    let mut i = 4; // skip "WITH"
    loop {
        // Skip to next open paren (the CTE body)
        while i < bytes.len() && bytes[i] != b'(' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        // Skip balanced parens, tracking single-quoted strings
        let mut depth = 0;
        while i < bytes.len() {
            if orig_bytes[i] == b'\'' {
                // Inside a single-quoted string — advance past it
                i += 1;
                while i < orig_bytes.len() {
                    if orig_bytes[i] == b'\'' {
                        // Check for escaped quote ('')
                        if i + 1 < orig_bytes.len() && orig_bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        // End of string
                        break;
                    }
                    i += 1;
                }
                // skip closing quote
                if i < orig_bytes.len() {
                    i += 1;
                }
                continue;
            }
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth -= 1;
                if depth == 0 {
                    i += 1;
                    break;
                }
            }
            i += 1;
        }
        // After closing paren, skip whitespace and check for comma (another CTE) or keyword
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b',' {
            i += 1; // skip comma, continue to next CTE
            continue;
        }
        break;
    }
    upper[i..].trim_start().to_string()
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
        // WITH ... INSERT is a write, not a read
        assert!(!is_read(
            "WITH cte AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM cte"
        ));
    }

    #[test]
    fn test_with_cte_write() {
        assert!(is_write(
            "WITH cte AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM cte"
        ));
        assert!(is_write(
            "WITH cte AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM cte)"
        ));
        assert!(is_write(
            "WITH cte AS (SELECT 1) UPDATE t SET x = 1 WHERE id IN (SELECT * FROM cte)"
        ));
        // WITH ... SELECT is not a write
        assert!(!is_write("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        // Multiple CTEs
        assert!(is_write(
            "WITH a AS (SELECT 1), b AS (SELECT 2) INSERT INTO t SELECT * FROM a"
        ));
    }

    #[test]
    fn test_cte_with_string_literal_parens() {
        // String containing '(' must not misbalance the paren counter
        assert!(is_write(
            "WITH cte AS (SELECT '(' AS x) INSERT INTO t SELECT * FROM cte"
        ));
        assert!(!is_read(
            "WITH cte AS (SELECT '(' AS x) INSERT INTO t SELECT * FROM cte"
        ));
        // String containing ')'
        assert!(is_write(
            "WITH cte AS (SELECT ')' AS x) INSERT INTO t SELECT * FROM cte"
        ));
        // Both parens in string
        assert!(is_write(
            "WITH cte AS (SELECT '()' AS x) INSERT INTO t SELECT * FROM cte"
        ));
        // Read variant should still work
        assert!(is_read("WITH cte AS (SELECT '(' AS x) SELECT * FROM cte"));
    }

    #[test]
    fn test_cte_with_recursive_and_strings() {
        assert!(is_write(
            "WITH RECURSIVE cte AS (SELECT '(' AS x UNION ALL SELECT x FROM cte) INSERT INTO t SELECT * FROM cte"
        ));
        assert!(is_read(
            "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"
        ));
    }

    #[test]
    fn test_cte_with_escaped_quotes() {
        // Escaped single quote ('') inside a string
        assert!(is_write(
            "WITH cte AS (SELECT '''' AS x) INSERT INTO t SELECT * FROM cte"
        ));
        assert!(is_write(
            "WITH cte AS (SELECT 'it''s a (test)' AS x) INSERT INTO t SELECT * FROM cte"
        ));
        assert!(is_read(
            "WITH cte AS (SELECT 'it''s a (test)' AS x) SELECT * FROM cte"
        ));
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
