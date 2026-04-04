use std::sync::Arc;

use dkdc_db_core::DbManager;

#[tokio::test]
async fn concurrent_create_drop() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    let mut handles = Vec::new();

    // Spawn 10 tasks that each create a unique database
    for i in 0..10 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            let name = format!("db{i}");
            mgr.create_db(&name).await.unwrap();
            mgr.execute(&name, "CREATE TABLE t (id INTEGER)")
                .await
                .unwrap();
            mgr.execute(&name, &format!("INSERT INTO t VALUES ({i})"))
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let dbs = mgr.list_dbs().await;
    assert_eq!(dbs.len(), 10);

    // Now drop them all concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            mgr.drop_db(&format!("db{i}")).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let dbs = mgr.list_dbs().await;
    assert!(dbs.is_empty());
}

#[tokio::test]
async fn concurrent_reads_on_same_db() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("shared").await.unwrap();
    mgr.execute(
        "shared",
        "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await
    .unwrap();

    for i in 0..100 {
        mgr.execute(
            "shared",
            &format!("INSERT INTO data VALUES ({i}, 'row_{i}')"),
        )
        .await
        .unwrap();
    }

    // 20 concurrent OLTP reads
    let mut handles = Vec::new();
    for _ in 0..20 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            let batches = mgr
                .query_oltp("shared", "SELECT count(*) FROM data")
                .await
                .unwrap();
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 100);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // 20 concurrent DataFusion reads
    let mut handles = Vec::new();
    for _ in 0..20 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            let batches = mgr
                .query("SELECT count(*) FROM shared.public.data")
                .await
                .unwrap();
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 100);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_writes_then_read() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("wdb").await.unwrap();
    mgr.execute("wdb", "CREATE TABLE log (id INTEGER, msg TEXT)")
        .await
        .unwrap();

    // Sequential writes (WriteEngine serializes them via single connection)
    for i in 0..500 {
        mgr.execute("wdb", &format!("INSERT INTO log VALUES ({i}, 'msg_{i}')"))
            .await
            .unwrap();
    }

    let batches = mgr
        .query_oltp("wdb", "SELECT count(*) FROM log")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 500);
}

#[tokio::test]
async fn mixed_read_write_interleaving() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("mix").await.unwrap();
    mgr.execute("mix", "CREATE TABLE kv (k TEXT, v INTEGER)")
        .await
        .unwrap();

    // Write 100 rows, reading after every 10
    for batch in 0..10 {
        for i in 0..10 {
            let k = batch * 10 + i;
            mgr.execute("mix", &format!("INSERT INTO kv VALUES ('key_{k}', {k})"))
                .await
                .unwrap();
        }
        let expected = (batch + 1) * 10;
        let batches = mgr
            .query_oltp("mix", "SELECT count(*) FROM kv")
            .await
            .unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), expected as i64);
    }
}
