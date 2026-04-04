use dkdc_db_core::DbManager;

#[tokio::test]
async fn schema_refresh_after_alter_table() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute("test", "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (1, 'alice')")
        .await
        .unwrap();

    // Verify initial schema
    let batches = mgr.query("SELECT * FROM test.public.t").await.unwrap();
    assert_eq!(batches[0].schema().fields().len(), 2);

    // ALTER TABLE ADD COLUMN
    mgr.execute("test", "ALTER TABLE t ADD COLUMN email TEXT")
        .await
        .unwrap();

    // Insert with new column
    mgr.execute("test", "INSERT INTO t VALUES (2, 'bob', 'bob@example.com')")
        .await
        .unwrap();

    // DataFusion should see the new column after auto-refresh
    let batches = mgr
        .query("SELECT * FROM test.public.t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(batches[0].schema().fields().len(), 3);
    assert_eq!(batches[0].num_rows(), 2);

    // Old row should have NULL for new column
    assert!(batches[0].column(2).is_null(0));
    // New row should have the email
    let email_arr = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(email_arr.value(1), "bob@example.com");
}

#[tokio::test]
async fn schema_refresh_auto_after_create_table() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute("test", "CREATE TABLE new_table (id INTEGER, val TEXT)")
        .await
        .unwrap();

    // Should be queryable immediately via DataFusion without manual refresh
    mgr.execute("test", "INSERT INTO new_table VALUES (1, 'test')")
        .await
        .unwrap();
    let batches = mgr
        .query("SELECT * FROM test.public.new_table")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}
