use dkdc_db_core::DbManager;

// ---------------------------------------------------------------------------
// Case sensitivity through DDL refresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mixed_case_table_name_through_ddl_refresh() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("testdb").await.unwrap();

    // Create a table with a quoted mixed-case name
    mgr.execute(
        "testdb",
        r#"CREATE TABLE "MixedCase" (id INTEGER, name TEXT)"#,
    )
    .await
    .unwrap();

    // Table should appear in list_tables
    let tables = mgr.list_tables("testdb").await.unwrap();
    assert!(
        tables.iter().any(|t| t.eq_ignore_ascii_case("mixedcase")),
        "expected MixedCase table in {tables:?}"
    );

    // Insert and read back via OLTP path
    mgr.execute("testdb", r#"INSERT INTO "MixedCase" VALUES (1, 'alice')"#)
        .await
        .unwrap();

    let batches = mgr
        .query_oltp("testdb", r#"SELECT id, name FROM "MixedCase""#)
        .await
        .unwrap();
    assert!(!batches.is_empty());

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn mixed_case_table_accessible_via_datafusion() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("testdb").await.unwrap();

    mgr.execute("testdb", r#"CREATE TABLE "MixedCase" (id INTEGER)"#)
        .await
        .unwrap();
    mgr.execute("testdb", r#"INSERT INTO "MixedCase" VALUES (42)"#)
        .await
        .unwrap();

    // The DDL refresh registers the table in the DataFusion catalog.
    // The exact registered name depends on extract_ddl_table_name (lowercased).
    // Try the lowercased variant through DataFusion.
    let result = mgr.query("SELECT id FROM testdb.public.mixedcase").await;
    assert!(
        result.is_ok(),
        "DataFusion query for mixed-case table failed: {result:?}"
    );

    let batches = result.unwrap();
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);
}

// ---------------------------------------------------------------------------
// Concurrent DDL + reads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_ddl_and_reads_no_panic() {
    use std::sync::Arc;

    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("concdb").await.unwrap();
    mgr.execute("concdb", "CREATE TABLE t (id INTEGER, val TEXT)")
        .await
        .unwrap();
    mgr.execute("concdb", "INSERT INTO t VALUES (1, 'init')")
        .await
        .unwrap();

    let iterations = 20;

    // Spawn a writer that does ALTER TABLE (add columns)
    let mgr_w = Arc::clone(&mgr);
    let writer = tokio::spawn(async move {
        for i in 0..iterations {
            let col = format!("col_{i}");
            let sql = format!(r#"ALTER TABLE t ADD COLUMN "{col}" TEXT"#);
            // ALTER may fail if column already exists on retry — that's fine
            let _ = mgr_w.execute("concdb", &sql).await;
        }
    });

    // Spawn a reader doing SELECTs concurrently
    let mgr_r = Arc::clone(&mgr);
    let reader = tokio::spawn(async move {
        for _ in 0..iterations {
            // OLTP path
            let result = mgr_r.query_oltp("concdb", "SELECT id, val FROM t").await;
            assert!(result.is_ok(), "OLTP read failed: {result:?}");

            // DataFusion path
            let result = mgr_r.query("SELECT id, val FROM concdb.public.t").await;
            // DataFusion may see stale schema mid-refresh — that's acceptable,
            // but it must not panic.
            match result {
                Ok(batches) => {
                    // Should have at least the original row
                    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total_rows >= 1);
                }
                Err(_) => {
                    // A transient schema mismatch error is acceptable, a panic is not.
                }
            }
        }
    });

    // Both tasks must complete without panic
    let (w, r) = tokio::join!(writer, reader);
    w.expect("writer task panicked");
    r.expect("reader task panicked");
}
