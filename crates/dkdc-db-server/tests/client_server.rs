use std::sync::Arc;

use dkdc_db_client::DbClient;
use dkdc_db_core::DbManager;
use dkdc_db_server::api;

/// Spin up a server on a random port, return the client.
async fn setup() -> (DbClient, tokio::task::JoinHandle<()>) {
    let manager = Arc::new(DbManager::new_in_memory().await.unwrap());
    let app = api::router(manager);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DbClient::localhost(port);

    // Wait for server to be ready
    for _ in 0..50 {
        if client.health().await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    (client, handle)
}

#[tokio::test]
async fn health_check() {
    let (client, _handle) = setup().await;
    assert!(client.health().await.unwrap());
}

#[tokio::test]
async fn create_and_list_dbs() {
    let (client, _handle) = setup().await;

    let dbs = client.list_dbs().await.unwrap();
    assert!(dbs.is_empty());

    client.create_db("mydb").await.unwrap();
    client.create_db("other").await.unwrap();

    let dbs = client.list_dbs().await.unwrap();
    assert_eq!(dbs, vec!["mydb", "other"]);
}

#[tokio::test]
async fn execute_and_query_roundtrip() {
    let (client, _handle) = setup().await;

    client.create_db("test").await.unwrap();

    client
        .execute(
            "test",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        )
        .await
        .unwrap();

    let affected = client
        .execute("test", "INSERT INTO users VALUES (1, 'alice')")
        .await
        .unwrap();
    assert_eq!(affected, 1);

    client
        .execute("test", "INSERT INTO users VALUES (2, 'bob')")
        .await
        .unwrap();

    // Query via DataFusion (qualified name)
    let resp = client
        .query("SELECT * FROM test.public.users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(resp.columns.len(), 2);
    assert_eq!(resp.columns[0].name, "id");
    assert_eq!(resp.columns[1].name, "name");
    assert_eq!(resp.rows.len(), 2);
    assert_eq!(resp.rows[0][0], serde_json::json!(1));
    assert_eq!(resp.rows[0][1], serde_json::json!("alice"));
    assert_eq!(resp.rows[1][0], serde_json::json!(2));
    assert_eq!(resp.rows[1][1], serde_json::json!("bob"));
}

#[tokio::test]
async fn query_oltp_path() {
    let (client, _handle) = setup().await;

    client.create_db("test").await.unwrap();

    client
        .execute("test", "CREATE TABLE t (id INTEGER, val TEXT)")
        .await
        .unwrap();
    client
        .execute("test", "INSERT INTO t VALUES (1, 'hello')")
        .await
        .unwrap();

    let resp = client.query_oltp("test", "SELECT * FROM t").await.unwrap();
    assert_eq!(resp.rows.len(), 1);
    assert_eq!(resp.rows[0][1], serde_json::json!("hello"));
}

#[tokio::test]
async fn list_tables_endpoint() {
    let (client, _handle) = setup().await;

    client.create_db("test").await.unwrap();

    let tables = client.list_tables("test").await.unwrap();
    assert!(tables.is_empty());

    client
        .execute("test", "CREATE TABLE foo (id INTEGER)")
        .await
        .unwrap();
    client
        .execute("test", "CREATE TABLE bar (id INTEGER)")
        .await
        .unwrap();

    let tables = client.list_tables("test").await.unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"foo".to_string()));
    assert!(tables.contains(&"bar".to_string()));
}

#[tokio::test]
async fn aggregation_through_api() {
    let (client, _handle) = setup().await;

    client.create_db("test").await.unwrap();

    client
        .execute("test", "CREATE TABLE sales (region TEXT, amount REAL)")
        .await
        .unwrap();

    for i in 0..100 {
        let region = if i % 2 == 0 { "north" } else { "south" };
        client
            .execute(
                "test",
                &format!("INSERT INTO sales VALUES ('{region}', {})", i as f64),
            )
            .await
            .unwrap();
    }

    let resp = client
        .query("SELECT region, count(*) as cnt, sum(amount) as total FROM test.public.sales GROUP BY region ORDER BY region")
        .await
        .unwrap();
    assert_eq!(resp.rows.len(), 2);
    assert_eq!(resp.rows[0][0], serde_json::json!("north"));
    assert_eq!(resp.rows[0][1], serde_json::json!(50));
    assert_eq!(resp.rows[1][0], serde_json::json!("south"));
    assert_eq!(resp.rows[1][1], serde_json::json!(50));
}

#[tokio::test]
async fn error_on_write_through_query() {
    let (client, _handle) = setup().await;

    client.create_db("test").await.unwrap();
    client
        .execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    let result = client.query("INSERT INTO test.public.t VALUES (1)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn schema_endpoint() {
    let (client, _handle) = setup().await;

    client.create_db("test").await.unwrap();

    client
        .execute(
            "test",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        )
        .await
        .unwrap();

    let resp = client.table_schema("test", "users").await.unwrap();
    assert_eq!(resp.rows.len(), 3);
}

#[tokio::test]
async fn cross_db_join() {
    let (client, _handle) = setup().await;

    client.create_db("hr").await.unwrap();
    client.create_db("sales").await.unwrap();

    client
        .execute("hr", "CREATE TABLE employees (id INTEGER, name TEXT)")
        .await
        .unwrap();
    client
        .execute("hr", "INSERT INTO employees VALUES (1, 'alice')")
        .await
        .unwrap();
    client
        .execute("hr", "INSERT INTO employees VALUES (2, 'bob')")
        .await
        .unwrap();

    client
        .execute(
            "sales",
            "CREATE TABLE orders (id INTEGER, emp_id INTEGER, amount REAL)",
        )
        .await
        .unwrap();
    client
        .execute("sales", "INSERT INTO orders VALUES (1, 1, 100.0)")
        .await
        .unwrap();
    client
        .execute("sales", "INSERT INTO orders VALUES (2, 2, 200.0)")
        .await
        .unwrap();

    let resp = client
        .query(
            "SELECT e.name, o.amount
             FROM hr.public.employees e
             JOIN sales.public.orders o ON e.id = o.emp_id
             ORDER BY e.name",
        )
        .await
        .unwrap();
    assert_eq!(resp.rows.len(), 2);
    assert_eq!(resp.rows[0][0], serde_json::json!("alice"));
    assert_eq!(resp.rows[1][0], serde_json::json!("bob"));
}

#[tokio::test]
async fn drop_db_via_api() {
    let (client, _handle) = setup().await;

    client.create_db("temp").await.unwrap();
    client
        .execute("temp", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    client.drop_db("temp").await.unwrap();

    let dbs = client.list_dbs().await.unwrap();
    assert!(dbs.is_empty());
}
