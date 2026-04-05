use dkdc_db_core::DbManager;

#[tokio::test]
async fn execute_on_nonexistent_db() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    let result = mgr.execute("nope", "SELECT 1").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("not found"), "error: {err}");
}

#[tokio::test]
async fn query_oltp_on_nonexistent_db() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    let result = mgr.query_oltp("nope", "SELECT 1").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn list_tables_on_nonexistent_db() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    let result = mgr.list_tables("nope").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn table_schema_nonexistent_table() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    let result = mgr.table_schema("test", "nonexistent").await;
    // PRAGMA table_info returns empty for nonexistent tables -> empty batch
    let batches = result.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "nonexistent table should return zero rows");
}

#[tokio::test]
async fn write_on_read_path() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    let cases = [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "CREATE TABLE t (id INT)",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN x TEXT",
        "REPLACE INTO t VALUES (1)",
    ];

    for sql in &cases {
        let result = mgr.query(sql).await;
        assert!(result.is_err(), "query() should reject: {sql}");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("write attempted through read path"),
            "wrong error for {sql}: {err}"
        );
    }
}

#[tokio::test]
async fn read_on_write_path() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    let cases = [
        "SELECT * FROM t",
        "EXPLAIN SELECT 1",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
    ];

    for sql in &cases {
        let result = mgr.execute("test", sql).await;
        assert!(result.is_err(), "execute() should reject: {sql}");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("read attempted through write path"),
            "wrong error for {sql}: {err}"
        );
    }
}

#[tokio::test]
async fn invalid_sql_returns_error() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // Malformed SQL
    let result = mgr.execute("test", "INSERT INTO t VALUEZ (1)").await;
    assert!(result.is_err());

    // Wrong column count
    let result = mgr.execute("test", "INSERT INTO t VALUES (1, 2, 3)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn drop_db_then_operations_fail() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.drop_db("test").await.unwrap();

    // All operations should fail on dropped db
    assert!(
        mgr.execute("test", "INSERT INTO t VALUES (1)")
            .await
            .is_err()
    );
    assert!(mgr.query_oltp("test", "SELECT * FROM t").await.is_err());
    assert!(mgr.list_tables("test").await.is_err());
}
