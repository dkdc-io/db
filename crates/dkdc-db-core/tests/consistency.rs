use std::sync::Arc;

use dkdc_db_core::DbManager;

#[tokio::test]
async fn concurrent_write_and_read() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("test").await.unwrap();

    mgr.execute(
        "test",
        "CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await
    .unwrap();

    // Insert 1000 rows
    for i in 0..1000 {
        mgr.execute(
            "test",
            &format!("INSERT INTO counter VALUES ({i}, {})", i * 10),
        )
        .await
        .unwrap();
    }

    // Spawn 3 concurrent reader tasks using analytical path
    let mut handles = Vec::new();
    for _ in 0..3 {
        let mgr_clone = mgr.clone();
        handles.push(tokio::spawn(async move {
            let batches = mgr_clone
                .query("SELECT count(*) as cnt FROM test.public.counter")
                .await
                .unwrap();
            let cnt_arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            cnt_arr.value(0)
        }));
    }

    // All readers should see 1000 rows
    for handle in handles {
        let count = handle.await.unwrap();
        assert_eq!(count, 1000, "Reader should see all 1000 rows");
    }

    // Verify via turso too
    let ls_batches = mgr
        .query_oltp("test", "SELECT count(*) FROM counter")
        .await
        .unwrap();
    let ls_cnt = ls_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(ls_cnt.value(0), 1000);
}

#[tokio::test]
async fn write_enforcement() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // query() should reject writes
    let result = mgr.query("INSERT INTO test.public.t VALUES (1)").await;
    assert!(result.is_err(), "query() should reject INSERT");

    let result = mgr.query("DELETE FROM test.public.t").await;
    assert!(result.is_err(), "query() should reject DELETE");

    // execute() should reject pure reads
    let result = mgr.execute("test", "SELECT * FROM t").await;
    assert!(result.is_err(), "execute() should reject SELECT");
}

#[tokio::test]
async fn in_memory_works_end_to_end() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute("test", "CREATE TABLE test_tbl (a INTEGER, b TEXT)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO test_tbl VALUES (1, 'hello')")
        .await
        .unwrap();
    let batches = mgr
        .query("SELECT * FROM test.public.test_tbl")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn multiple_tables_with_join() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute(
        "test",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await
    .unwrap();
    mgr.execute(
        "test",
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)",
    )
    .await
    .unwrap();
    mgr.execute(
        "test",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await
    .unwrap();

    mgr.execute("test", "INSERT INTO users VALUES (1, 'alice')")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO users VALUES (2, 'bob')")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO orders VALUES (1, 1, 99.99)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO orders VALUES (2, 1, 49.99)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO orders VALUES (3, 2, 25.00)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO products VALUES (1, 'widget')")
        .await
        .unwrap();

    // Join query via DataFusion
    let batches = mgr
        .query(
            "SELECT u.name, count(*) as order_count, sum(o.amount) as total
             FROM test.public.users u JOIN test.public.orders o ON u.id = o.user_id
             GROUP BY u.name ORDER BY u.name",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 2);

    // Verify tables list
    let tables = mgr.list_tables("test").await.unwrap();
    assert_eq!(tables.len(), 3);
}

#[tokio::test]
async fn aggregation_correctness() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();

    mgr.execute("test", "CREATE TABLE metrics (grp TEXT, val REAL)")
        .await
        .unwrap();

    // Insert 10K rows across 10 groups
    for i in 0..10_000 {
        let grp = format!("g{}", i % 10);
        let val = (i as f64) * 0.1;
        mgr.execute(
            "test",
            &format!("INSERT INTO metrics VALUES ('{grp}', {val})"),
        )
        .await
        .unwrap();
    }

    let batches = mgr
        .query(
            "SELECT grp, count(*) as cnt, avg(val) as avg_val
             FROM test.public.metrics GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 10);

    // Each group should have 1000 rows
    let cnt_arr = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for i in 0..10 {
        assert_eq!(cnt_arr.value(i), 1000, "group {i} should have 1000 rows");
    }
}
