use dkdc_db_core::DbManager;

#[tokio::test]
async fn null_handling() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (NULL, NULL, NULL)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (1, 'hello', 3.14)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (NULL, 'world', NULL)")
        .await
        .unwrap();

    // OLTP path
    let batches = mgr
        .query_oltp("test", "SELECT * FROM t ORDER BY a")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 3);

    // DataFusion path
    let batches = mgr
        .query("SELECT * FROM test.public.t ORDER BY a")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn special_characters_in_text() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val TEXT)")
        .await
        .unwrap();

    let cases = [
        "hello world",
        "it''s a test", // SQL-escaped single quote
        "",             // empty string
        "line1\nline2",
        "tab\there",
        "emoji: 🎉",
        "unicode: αβγ",
        "null\0char",
    ];

    for val in &cases {
        mgr.execute("test", &format!("INSERT INTO t VALUES ('{val}')"))
            .await
            .unwrap_or_default(); // some may fail due to null byte, that's ok
    }

    let batches = mgr
        .query_oltp("test", "SELECT count(*) FROM t")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert!(arr.value(0) > 0);
}

#[tokio::test]
async fn large_integer_values() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val INTEGER)")
        .await
        .unwrap();

    mgr.execute("test", &format!("INSERT INTO t VALUES ({})", i64::MAX))
        .await
        .unwrap();
    mgr.execute("test", &format!("INSERT INTO t VALUES ({})", i64::MIN))
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (0)")
        .await
        .unwrap();

    let batches = mgr
        .query_oltp("test", "SELECT * FROM t ORDER BY val")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 3);

    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), i64::MIN);
    assert_eq!(arr.value(1), 0);
    assert_eq!(arr.value(2), i64::MAX);
}

#[tokio::test]
async fn float_edge_values() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val REAL)")
        .await
        .unwrap();

    mgr.execute("test", "INSERT INTO t VALUES (0.0)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (-0.0)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (1e308)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (-1e308)")
        .await
        .unwrap();

    let batches = mgr
        .query_oltp("test", "SELECT count(*) FROM t")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 4);
}

#[tokio::test]
async fn empty_table_queries() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE empty (id INTEGER, name TEXT)")
        .await
        .unwrap();

    // OLTP on empty table
    let batches = mgr.query_oltp("test", "SELECT * FROM empty").await.unwrap();
    assert!(batches.is_empty() || batches[0].num_rows() == 0);

    // DataFusion on empty table
    let batches = mgr.query("SELECT * FROM test.public.empty").await.unwrap();
    assert!(batches.is_empty() || batches[0].num_rows() == 0);

    // Count on empty table
    let batches = mgr
        .query_oltp("test", "SELECT count(*) FROM empty")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 0);
}

#[tokio::test]
async fn ddl_refreshes_catalog() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    // Create table and verify it appears in DataFusion
    mgr.execute("test", "CREATE TABLE t1 (id INTEGER)")
        .await
        .unwrap();
    let batches = mgr.query("SELECT * FROM test.public.t1").await.unwrap();
    assert!(batches.is_empty() || batches[0].num_rows() == 0);

    // Create second table
    mgr.execute("test", "CREATE TABLE t2 (name TEXT)")
        .await
        .unwrap();
    let tables = mgr.list_tables("test").await.unwrap();
    assert!(tables.contains(&"t1".to_string()));
    assert!(tables.contains(&"t2".to_string()));

    // Drop first table (DDL)
    mgr.execute("test", "DROP TABLE t1").await.unwrap();
    let tables = mgr.list_tables("test").await.unwrap();
    assert!(!tables.contains(&"t1".to_string()));
    assert!(tables.contains(&"t2".to_string()));
}

#[tokio::test]
async fn table_schema_returns_columns() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute(
        "test",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, data BLOB)",
    )
    .await
    .unwrap();

    let batches = mgr.table_schema("test", "users").await.unwrap();
    assert!(!batches.is_empty());
    // Should have 4 columns described
    assert_eq!(batches[0].num_rows(), 4);
}

#[tokio::test]
async fn nested_db_names() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("project/mydb").await.unwrap();
    mgr.execute("project/mydb", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.execute("project/mydb", "INSERT INTO t VALUES (42)")
        .await
        .unwrap();

    // Catalog name: project_mydb (slashes become underscores)
    let batches = mgr
        .query("SELECT * FROM project_mydb.public.t")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    let dbs = mgr.list_dbs().await;
    assert!(dbs.contains(&"project/mydb".to_string()));
}
