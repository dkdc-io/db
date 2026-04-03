use std::sync::Arc;

use dkdc_db_client::DbClient;
use dkdc_db_core::DkdcDb;
use dkdc_db_server::api;

/// Spin up a server on a random port, return the client.
async fn setup() -> (DbClient, tokio::task::JoinHandle<()>) {
    let db = DkdcDb::open_in_memory().await.unwrap();
    let state = Arc::new(db);
    let app = api::router(state);

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
async fn execute_and_query_roundtrip() {
    let (client, _handle) = setup().await;

    client
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    let affected = client
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .await
        .unwrap();
    assert_eq!(affected, 1);

    client
        .execute("INSERT INTO users VALUES (2, 'bob')")
        .await
        .unwrap();

    // Query via DataFusion
    let resp = client
        .query("SELECT * FROM users ORDER BY id")
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
async fn query_turso_path() {
    let (client, _handle) = setup().await;

    client
        .execute("CREATE TABLE t (id INTEGER, val TEXT)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO t VALUES (1, 'hello')")
        .await
        .unwrap();

    let resp = client.query_turso("SELECT * FROM t").await.unwrap();
    assert_eq!(resp.rows.len(), 1);
    assert_eq!(resp.rows[0][1], serde_json::json!("hello"));
}

#[tokio::test]
async fn list_tables_endpoint() {
    let (client, _handle) = setup().await;

    let tables = client.list_tables().await.unwrap();
    assert!(tables.is_empty());

    client
        .execute("CREATE TABLE foo (id INTEGER)")
        .await
        .unwrap();
    client
        .execute("CREATE TABLE bar (id INTEGER)")
        .await
        .unwrap();

    let tables = client.list_tables().await.unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"foo".to_string()));
    assert!(tables.contains(&"bar".to_string()));
}

#[tokio::test]
async fn aggregation_through_api() {
    let (client, _handle) = setup().await;

    client
        .execute("CREATE TABLE sales (region TEXT, amount REAL)")
        .await
        .unwrap();

    for i in 0..100 {
        let region = if i % 2 == 0 { "north" } else { "south" };
        client
            .execute(&format!(
                "INSERT INTO sales VALUES ('{region}', {})",
                i as f64
            ))
            .await
            .unwrap();
    }

    let resp = client
        .query("SELECT region, count(*) as cnt, sum(amount) as total FROM sales GROUP BY region ORDER BY region")
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

    client.execute("CREATE TABLE t (id INTEGER)").await.unwrap();

    let result = client.query("INSERT INTO t VALUES (1)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn schema_endpoint() {
    let (client, _handle) = setup().await;

    client
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .await
        .unwrap();

    let resp = client.table_schema("users").await.unwrap();
    assert_eq!(resp.rows.len(), 3);
}
