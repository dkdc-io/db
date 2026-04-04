use std::sync::Arc;

use dkdc_db_client::DbClient;
use dkdc_db_core::DbManager;
use dkdc_db_server::api;

async fn setup() -> (DbClient, tokio::task::JoinHandle<()>) {
    let manager = Arc::new(DbManager::new_in_memory().await.unwrap());
    let app = api::router(manager);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DbClient::localhost(port);

    for _ in 0..50 {
        if client.health().await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    (client, handle)
}

#[tokio::test]
async fn create_db_with_invalid_name() {
    let (client, _handle) = setup().await;

    // Path traversal attempt
    let result = client.create_db("../escape").await;
    assert!(result.is_err());

    // Empty name
    let result = client.create_db("").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn create_duplicate_db() {
    let (client, _handle) = setup().await;
    client.create_db("mydb").await.unwrap();

    let result = client.create_db("mydb").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn drop_nonexistent_db() {
    let (client, _handle) = setup().await;
    let result = client.drop_db("doesnotexist").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn execute_on_nonexistent_db() {
    let (client, _handle) = setup().await;
    let result = client.execute("nope", "CREATE TABLE t (id INTEGER)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn query_oltp_on_nonexistent_db() {
    let (client, _handle) = setup().await;
    let result = client.query_oltp("nope", "SELECT 1").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn execute_invalid_sql() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();

    let result = client.execute("test", "NOT VALID SQL AT ALL").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn query_oltp_invalid_sql() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();

    let result = client.query_oltp("test", "SELECTTTT *").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn query_datafusion_invalid_sql() {
    let (client, _handle) = setup().await;
    let result = client.query("THIS IS NOT SQL").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn query_nonexistent_table() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();

    let result = client
        .query_oltp("test", "SELECT * FROM nonexistent")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn list_tables_nonexistent_db() {
    let (client, _handle) = setup().await;
    let result = client.list_tables("nope").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn table_schema_nonexistent_table() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();

    // Schema for a table that doesn't exist — should return empty or succeed with 0 rows
    let result = client.table_schema("test", "nonexistent").await;
    // The PRAGMA table_info returns empty for nonexistent tables
    match result {
        Ok(resp) => assert!(resp.rows.is_empty()),
        Err(_) => {} // either behavior is acceptable
    }
}

#[tokio::test]
async fn create_db_with_simple_names() {
    let (client, _handle) = setup().await;

    // Various valid simple names
    client.create_db("mydb").await.unwrap();
    client.create_db("test123").await.unwrap();
    client.create_db("a").await.unwrap();

    let dbs = client.list_dbs().await.unwrap();
    assert_eq!(dbs.len(), 3);
    assert!(dbs.contains(&"mydb".to_string()));
    assert!(dbs.contains(&"test123".to_string()));
    assert!(dbs.contains(&"a".to_string()));
}

#[tokio::test]
async fn create_db_with_hyphens_and_underscores() {
    let (client, _handle) = setup().await;
    client.create_db("my-db").await.unwrap();
    client.create_db("my_db").await.unwrap();

    let dbs = client.list_dbs().await.unwrap();
    assert!(dbs.contains(&"my-db".to_string()));
    assert!(dbs.contains(&"my_db".to_string()));
}

#[tokio::test]
async fn write_through_query_rejected() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();
    client
        .execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // INSERT through the DataFusion query path should be rejected
    let result = client.query("INSERT INTO test.public.t VALUES (1)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn read_through_execute_rejected() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();
    client
        .execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // SELECT through the execute (write) path should be rejected
    let result = client.execute("test", "SELECT * FROM t").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn drop_then_recreate_db() {
    let (client, _handle) = setup().await;

    client.create_db("temp").await.unwrap();
    client
        .execute("temp", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    client
        .execute("temp", "INSERT INTO t VALUES (1)")
        .await
        .unwrap();

    client.drop_db("temp").await.unwrap();

    // Recreate with same name
    client.create_db("temp").await.unwrap();

    // Should be a fresh database — no tables
    let tables = client.list_tables("temp").await.unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn query_after_drop_fails() {
    let (client, _handle) = setup().await;
    client.create_db("gone").await.unwrap();
    client
        .execute("gone", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    client.drop_db("gone").await.unwrap();

    // DataFusion query should fail — catalog removed
    let result = client.query("SELECT * FROM gone.public.t").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn empty_sql_rejected() {
    let (client, _handle) = setup().await;
    client.create_db("test").await.unwrap();

    let result = client.execute("test", "").await;
    assert!(result.is_err());

    let result = client.query_oltp("test", "").await;
    assert!(result.is_err());
}
