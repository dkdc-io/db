use dkdc_db_client::{DbClient, Error};

/// Connecting to a port with no server should return an HTTP error.
#[tokio::test]
async fn connection_refused() {
    // Use a port that's almost certainly not listening
    let client = DbClient::localhost(19999);

    let result = client.health().await;
    assert!(result.is_err());

    match result.unwrap_err() {
        Error::Http(_) => {} // expected: connection refused
        Error::Server(msg) => panic!("expected Http error, got Server: {msg}"),
    }
}

/// All client methods should fail with Http error when server is down.
#[tokio::test]
async fn all_methods_fail_when_no_server() {
    let client = DbClient::localhost(19998);

    assert!(matches!(
        client.create_db("test").await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.drop_db("test").await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.list_dbs().await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.execute("test", "SELECT 1").await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.query("SELECT 1").await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.query_oltp("test", "SELECT 1").await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.list_tables("test").await.unwrap_err(),
        Error::Http(_)
    ));
    assert!(matches!(
        client.table_schema("test", "users").await.unwrap_err(),
        Error::Http(_)
    ));
}

/// Server errors should be returned as Error::Server with the message.
#[tokio::test]
async fn server_error_propagated() {
    let manager = std::sync::Arc::new(dkdc_db_core::DbManager::new_in_memory().await.unwrap());
    let app = dkdc_db_server::api::router(manager);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DbClient::localhost(port);
    for _ in 0..50 {
        if client.health().await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Execute on nonexistent DB should produce Error::Server
    let result = client.execute("nonexistent", "SELECT 1").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Server(msg) => {
            assert!(!msg.is_empty());
        }
        Error::Http(e) => panic!("expected Server error, got Http: {e}"),
    }
}

/// Client correctly handles successful responses too.
#[tokio::test]
async fn client_roundtrip_success() {
    let manager = std::sync::Arc::new(dkdc_db_core::DbManager::new_in_memory().await.unwrap());
    let app = dkdc_db_server::api::router(manager);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DbClient::localhost(port);
    for _ in 0..50 {
        if client.health().await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Health check returns true
    assert!(client.health().await.unwrap());

    // Create, execute, query cycle
    client.create_db("test").await.unwrap();
    client
        .execute("test", "CREATE TABLE t (id INTEGER, name TEXT)")
        .await
        .unwrap();
    let affected = client
        .execute("test", "INSERT INTO t VALUES (1, 'alice')")
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let resp = client.query_oltp("test", "SELECT * FROM t").await.unwrap();
    assert_eq!(resp.rows.len(), 1);
    assert_eq!(resp.columns.len(), 2);
    assert_eq!(resp.columns[0].name, "id");
    assert_eq!(resp.columns[1].name, "name");
}
