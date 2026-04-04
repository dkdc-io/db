use dkdc_db_core::{DbManager, validate_db_name, validate_sql, validate_table_name};

// -- validate_db_name --

#[test]
fn valid_db_names() {
    assert!(validate_db_name("mydb").is_ok());
    assert!(validate_db_name("my-db").is_ok());
    assert!(validate_db_name("my_db").is_ok());
    assert!(validate_db_name("project/mydb").is_ok());
    assert!(validate_db_name("a").is_ok());
    assert!(validate_db_name("ABC123").is_ok());
}

#[test]
fn empty_db_name() {
    assert!(validate_db_name("").is_err());
}

#[test]
fn db_name_too_long() {
    let long_name = "a".repeat(129);
    assert!(validate_db_name(&long_name).is_err());
    // Exactly at limit should be fine
    let max_name = "a".repeat(128);
    assert!(validate_db_name(&max_name).is_ok());
}

#[test]
fn db_name_path_traversal() {
    assert!(validate_db_name("../etc/passwd").is_err());
    assert!(validate_db_name("foo/../../bar").is_err());
    assert!(validate_db_name("..").is_err());
    assert!(validate_db_name("./local").is_err());
}

#[test]
fn db_name_starts_with_slash() {
    assert!(validate_db_name("/absolute").is_err());
}

#[test]
fn db_name_invalid_chars() {
    assert!(validate_db_name("my db").is_err());
    assert!(validate_db_name("my.db").is_err());
    assert!(validate_db_name("my;db").is_err());
    assert!(validate_db_name("my'db").is_err());
    assert!(validate_db_name("my\"db").is_err());
    assert!(validate_db_name("db\0name").is_err());
    assert!(validate_db_name("db\nname").is_err());
}

#[test]
fn db_name_double_slash() {
    assert!(validate_db_name("a//b").is_err());
}

#[test]
fn db_name_trailing_slash() {
    assert!(validate_db_name("mydb/").is_err());
}

// -- validate_sql --

#[test]
fn valid_sql() {
    assert!(validate_sql("SELECT 1").is_ok());
    assert!(validate_sql("INSERT INTO t VALUES (1)").is_ok());
}

#[test]
fn empty_sql() {
    assert!(validate_sql("").is_err());
    assert!(validate_sql("   ").is_err());
    assert!(validate_sql("\n\t").is_err());
}

#[test]
fn sql_too_large() {
    let huge = "x".repeat(10 * 1024 * 1024 + 1);
    assert!(validate_sql(&huge).is_err());
    // Exactly at limit
    let at_limit = "x".repeat(10 * 1024 * 1024);
    assert!(validate_sql(&at_limit).is_ok());
}

// -- validate_table_name --

#[test]
fn valid_table_names() {
    assert!(validate_table_name("users").is_ok());
    assert!(validate_table_name("my_table").is_ok());
    assert!(validate_table_name("T1").is_ok());
}

#[test]
fn empty_table_name() {
    assert!(validate_table_name("").is_err());
}

#[test]
fn table_name_too_long() {
    let long_name = "t".repeat(129);
    assert!(validate_table_name(&long_name).is_err());
}

#[test]
fn table_name_injection() {
    assert!(validate_table_name("users'); DROP TABLE users; --").is_err());
    assert!(validate_table_name("a b").is_err());
    assert!(validate_table_name("tab.le").is_err());
    assert!(validate_table_name("tab'le").is_err());
}

// -- integration: validation through DbManager --

#[tokio::test]
async fn create_db_validates_name() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    assert!(mgr.create_db("").await.is_err());
    assert!(mgr.create_db("../evil").await.is_err());
    assert!(mgr.create_db("has space").await.is_err());
    assert!(mgr.create_db("valid-name").await.is_ok());
}

#[tokio::test]
async fn execute_validates_sql() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    assert!(mgr.execute("test", "").await.is_err());
    assert!(mgr.execute("test", "   ").await.is_err());
}

#[tokio::test]
async fn query_validates_sql() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    assert!(mgr.query("").await.is_err());
    assert!(mgr.query("   ").await.is_err());
}

#[tokio::test]
async fn table_schema_validates_name() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE users (id INTEGER)")
        .await
        .unwrap();
    // Valid table name works
    let result = mgr.table_schema("test", "users").await;
    assert!(result.is_ok());
    // SQL injection attempt rejected
    let result = mgr
        .table_schema("test", "users'); DROP TABLE users; --")
        .await;
    assert!(result.is_err());
}
