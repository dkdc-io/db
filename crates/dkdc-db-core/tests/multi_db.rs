use dkdc_db_core::DbManager;

#[tokio::test]
async fn create_and_list_dbs() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("a").await.unwrap();
    mgr.create_db("b").await.unwrap();
    let dbs = mgr.list_dbs().await;
    assert_eq!(dbs, vec!["a", "b"]);
}

#[tokio::test]
async fn execute_and_query_across_dbs() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("sales").await.unwrap();
    mgr.create_db("users").await.unwrap();

    mgr.execute(
        "users",
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await
    .unwrap();
    mgr.execute("users", "INSERT INTO customers VALUES (1, 'alice')")
        .await
        .unwrap();
    mgr.execute("users", "INSERT INTO customers VALUES (2, 'bob')")
        .await
        .unwrap();

    mgr.execute(
        "sales",
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount REAL)",
    )
    .await
    .unwrap();
    mgr.execute("sales", "INSERT INTO orders VALUES (1, 1, 99.99)")
        .await
        .unwrap();
    mgr.execute("sales", "INSERT INTO orders VALUES (2, 2, 49.99)")
        .await
        .unwrap();
    mgr.execute("sales", "INSERT INTO orders VALUES (3, 1, 25.00)")
        .await
        .unwrap();

    // Cross-database join
    let batches = mgr
        .query(
            "SELECT c.name, count(*) as order_count, sum(o.amount) as total
             FROM users.public.customers c
             JOIN sales.public.orders o ON c.id = o.customer_id
             GROUP BY c.name
             ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 2);

    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");

    let counts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 2); // alice has 2 orders
    assert_eq!(counts.value(1), 1); // bob has 1 order
}

#[tokio::test]
async fn drop_db_deregisters_catalog() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("temp").await.unwrap();
    mgr.execute("temp", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.execute("temp", "INSERT INTO t VALUES (1)")
        .await
        .unwrap();

    // Should work before drop
    let result = mgr.query("SELECT * FROM temp.public.t").await;
    assert!(result.is_ok());

    mgr.drop_db("temp").await.unwrap();

    // Should fail after drop — the catalog is now empty
    let result = mgr.query("SELECT * FROM temp.public.t").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn duplicate_db_name_errors() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("mydb").await.unwrap();
    let result = mgr.create_db("mydb").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn drop_nonexistent_db_errors() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    let result = mgr.drop_db("nope").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn oltp_scoped_to_single_db() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("db1").await.unwrap();
    mgr.create_db("db2").await.unwrap();

    mgr.execute("db1", "CREATE TABLE shared_name (val TEXT)")
        .await
        .unwrap();
    mgr.execute("db1", "INSERT INTO shared_name VALUES ('from_db1')")
        .await
        .unwrap();

    mgr.execute("db2", "CREATE TABLE shared_name (val TEXT)")
        .await
        .unwrap();
    mgr.execute("db2", "INSERT INTO shared_name VALUES ('from_db2')")
        .await
        .unwrap();

    let b1 = mgr
        .query_oltp("db1", "SELECT val FROM shared_name")
        .await
        .unwrap();
    let b2 = mgr
        .query_oltp("db2", "SELECT val FROM shared_name")
        .await
        .unwrap();

    let v1 = b1[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let v2 = b2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(v1.value(0), "from_db1");
    assert_eq!(v2.value(0), "from_db2");
}
