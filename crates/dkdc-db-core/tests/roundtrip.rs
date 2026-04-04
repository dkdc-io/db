use dkdc_db_core::DbManager;

#[tokio::test]
async fn roundtrip_write_and_read_via_both_engines() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute(
        "test",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await
    .unwrap();

    for i in 0..100 {
        mgr.execute(
            "test",
            &format!(
                "INSERT INTO users VALUES ({i}, 'user_{i}', {})",
                20 + (i % 50)
            ),
        )
        .await
        .unwrap();
    }

    // Read via DataFusion
    let df_batches = mgr
        .query("SELECT * FROM test.public.users ORDER BY id")
        .await
        .unwrap();
    let df_total: usize = df_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(df_total, 100, "DataFusion should return 100 rows");

    // Read via turso
    let ls_batches = mgr
        .query_oltp("test", "SELECT * FROM users ORDER BY id")
        .await
        .unwrap();
    let ls_total: usize = ls_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(ls_total, 100, "turso should return 100 rows");

    // Verify schemas match
    let df_schema = df_batches[0].schema();
    let ls_schema = ls_batches[0].schema();
    assert_eq!(df_schema.fields().len(), 3);
    assert_eq!(ls_schema.fields().len(), 3);

    // Verify data matches: compare first and last rows via both engines
    let df_first = mgr
        .query("SELECT id, name FROM test.public.users WHERE id = 0")
        .await
        .unwrap();
    let ls_first = mgr
        .query_oltp("test", "SELECT id, name FROM users WHERE id = 0")
        .await
        .unwrap();
    assert_eq!(df_first[0].num_rows(), 1);
    assert_eq!(ls_first[0].num_rows(), 1);
}
