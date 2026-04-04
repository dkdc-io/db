use std::sync::Arc;

use dkdc_db_core::DbManager;

/// Concurrent operations on different databases should not interfere.
#[tokio::test]
async fn concurrent_ops_on_different_dbs() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());

    // Create 5 databases
    for i in 0..5 {
        mgr.create_db(&format!("db{i}")).await.unwrap();
        mgr.execute(&format!("db{i}"), "CREATE TABLE data (id INTEGER, val TEXT)")
            .await
            .unwrap();
    }

    // Concurrently write to all 5 databases
    let mut handles = Vec::new();
    for i in 0..5 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            let db = format!("db{i}");
            for j in 0..50 {
                mgr.execute(&db, &format!("INSERT INTO data VALUES ({j}, 'val_{i}_{j}')"))
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Verify each database has exactly 50 rows
    for i in 0..5 {
        let batches = mgr
            .query_oltp(&format!("db{i}"), "SELECT count(*) FROM data")
            .await
            .unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 50);
    }
}

/// Cross-database join after schema changes (ALTER TABLE, new columns).
#[tokio::test]
async fn cross_db_join_after_schema_change() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("a").await.unwrap();
    mgr.create_db("b").await.unwrap();

    mgr.execute("a", "CREATE TABLE t (id INTEGER, name TEXT)")
        .await
        .unwrap();
    mgr.execute("a", "INSERT INTO t VALUES (1, 'alice')")
        .await
        .unwrap();

    mgr.execute("b", "CREATE TABLE t (id INTEGER, score REAL)")
        .await
        .unwrap();
    mgr.execute("b", "INSERT INTO t VALUES (1, 95.5)")
        .await
        .unwrap();

    // Join works before schema change
    let batches = mgr
        .query(
            "SELECT a.public.t.name, b.public.t.score
             FROM a.public.t JOIN b.public.t ON a.public.t.id = b.public.t.id",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    // Add column to db a
    mgr.execute("a", "ALTER TABLE t ADD COLUMN age INTEGER")
        .await
        .unwrap();
    mgr.execute("a", "INSERT INTO t VALUES (2, 'bob', 30)")
        .await
        .unwrap();

    mgr.execute("b", "INSERT INTO t VALUES (2, 88.0)")
        .await
        .unwrap();

    // Join should still work with new schema
    let batches = mgr
        .query(
            "SELECT a.public.t.name, b.public.t.score
             FROM a.public.t JOIN b.public.t ON a.public.t.id = b.public.t.id
             ORDER BY a.public.t.name",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 2);
}

/// Same table name in multiple databases — they should be independent.
#[tokio::test]
async fn same_table_name_different_dbs() {
    let mgr = DbManager::new_in_memory().await.unwrap();

    for name in &["alpha", "beta", "gamma"] {
        mgr.create_db(name).await.unwrap();
        mgr.execute(name, "CREATE TABLE items (val INTEGER)")
            .await
            .unwrap();
    }

    mgr.execute("alpha", "INSERT INTO items VALUES (1)")
        .await
        .unwrap();
    mgr.execute("beta", "INSERT INTO items VALUES (2)")
        .await
        .unwrap();
    mgr.execute("beta", "INSERT INTO items VALUES (3)")
        .await
        .unwrap();
    mgr.execute("gamma", "INSERT INTO items VALUES (4)")
        .await
        .unwrap();
    mgr.execute("gamma", "INSERT INTO items VALUES (5)")
        .await
        .unwrap();
    mgr.execute("gamma", "INSERT INTO items VALUES (6)")
        .await
        .unwrap();

    // Each DB should have correct count
    for (db, expected) in &[("alpha", 1), ("beta", 2), ("gamma", 3)] {
        let batches = mgr
            .query_oltp(db, "SELECT count(*) FROM items")
            .await
            .unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), *expected);
    }

    // Cross-db UNION via DataFusion
    let batches = mgr
        .query(
            "SELECT val FROM alpha.public.items
             UNION ALL SELECT val FROM beta.public.items
             UNION ALL SELECT val FROM gamma.public.items
             ORDER BY val",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 6);
}

/// Nested database names with slashes — catalog name mapping.
#[tokio::test]
async fn nested_db_names_catalog_mapping() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("org/team/db1").await.unwrap();
    mgr.create_db("org/team/db2").await.unwrap();

    mgr.execute("org/team/db1", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.execute("org/team/db1", "INSERT INTO t VALUES (1)")
        .await
        .unwrap();

    mgr.execute("org/team/db2", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.execute("org/team/db2", "INSERT INTO t VALUES (2)")
        .await
        .unwrap();

    // Catalog names: slashes become underscores
    let batches = mgr
        .query(
            "SELECT a.id, b.id
             FROM org_team_db1.public.t a
             JOIN org_team_db2.public.t b ON true",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

/// Create many databases and verify list ordering and cross-db queries.
#[tokio::test]
async fn many_databases() {
    let mgr = DbManager::new_in_memory().await.unwrap();

    for i in 0..20 {
        let name = format!("db{i:02}");
        mgr.create_db(&name).await.unwrap();
        mgr.execute(&name, "CREATE TABLE t (val INTEGER)")
            .await
            .unwrap();
        mgr.execute(&name, &format!("INSERT INTO t VALUES ({i})"))
            .await
            .unwrap();
    }

    let dbs = mgr.list_dbs().await;
    assert_eq!(dbs.len(), 20);

    // Cross-db query joining first and last
    let batches = mgr
        .query(
            "SELECT a.val, b.val
             FROM db00.public.t a, db19.public.t b",
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    let a = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let b = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(a.value(0), 0);
    assert_eq!(b.value(0), 19);
}

/// Drop a database while others are actively being queried.
#[tokio::test]
async fn drop_db_while_querying_others() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("keep").await.unwrap();
    mgr.create_db("remove").await.unwrap();

    mgr.execute("keep", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.execute("keep", "INSERT INTO t VALUES (42)")
        .await
        .unwrap();

    mgr.execute("remove", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // Drop one database
    mgr.drop_db("remove").await.unwrap();

    // The kept database should still work fine
    let batches = mgr
        .query_oltp("keep", "SELECT id FROM t")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 42);

    // DataFusion path should also work
    let batches = mgr
        .query("SELECT id FROM keep.public.t")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

/// Concurrent reads and writes across different databases.
#[tokio::test]
async fn concurrent_cross_db_reads_and_writes() {
    let mgr = Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("writers").await.unwrap();
    mgr.create_db("readers").await.unwrap();

    mgr.execute("writers", "CREATE TABLE log (id INTEGER)")
        .await
        .unwrap();
    mgr.execute("readers", "CREATE TABLE ref_data (id INTEGER, label TEXT)")
        .await
        .unwrap();
    mgr.execute("readers", "INSERT INTO ref_data VALUES (1, 'one')")
        .await
        .unwrap();

    // Write to one db while reading from another, concurrently
    let mgr_w = mgr.clone();
    let write_handle = tokio::spawn(async move {
        for i in 0..100 {
            mgr_w
                .execute("writers", &format!("INSERT INTO log VALUES ({i})"))
                .await
                .unwrap();
        }
    });

    let mgr_r = mgr.clone();
    let read_handle = tokio::spawn(async move {
        for _ in 0..50 {
            let batches = mgr_r
                .query_oltp("readers", "SELECT count(*) FROM ref_data")
                .await
                .unwrap();
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 1); // should always be 1
        }
    });

    write_handle.await.unwrap();
    read_handle.await.unwrap();

    // Verify writes completed
    let batches = mgr
        .query_oltp("writers", "SELECT count(*) FROM log")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 100);
}
