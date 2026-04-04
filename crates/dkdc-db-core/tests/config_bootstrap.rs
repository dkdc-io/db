use dkdc_db_core::DbManager;
use dkdc_db_core::toml_config::DbTomlConfig;

#[test]
fn parse_minimal_config() {
    let toml = r#"
        [databases.app]
    "#;
    let config: DbTomlConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.databases.len(), 1);
    assert!(config.databases.contains_key("app"));
    assert!(config.databases["app"].tables.is_empty());
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 4200);
}

#[test]
fn parse_full_config() {
    let toml = r#"
        [server]
        host = "0.0.0.0"
        port = 5000

        [databases.app]

        [databases.app.tables.users]
        sql = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"

        [databases.app.tables.users.indexes]
        idx_name = "CREATE INDEX IF NOT EXISTS idx_name ON users (name)"

        [databases.analytics]

        [databases.analytics.tables.events]
        sql = "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, kind TEXT)"
    "#;
    let config: DbTomlConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.server.port, 5000);
    assert_eq!(config.databases.len(), 2);
    assert_eq!(config.databases["app"].tables.len(), 1);
    assert_eq!(config.databases["app"].tables["users"].indexes.len(), 1);
    assert_eq!(config.databases["analytics"].tables.len(), 1);
}

#[test]
fn parse_empty_config() {
    let toml = "";
    let config: DbTomlConfig = toml::from_str(toml).unwrap();
    assert!(config.databases.is_empty());
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 4200);
}

#[tokio::test]
async fn bootstrap_creates_databases_and_tables() {
    let toml = r#"
        [databases.testdb]

        [databases.testdb.tables.items]
        sql = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"

        [databases.testdb.tables.items.indexes]
        idx_items_name = "CREATE INDEX IF NOT EXISTS idx_items_name ON items (name)"
    "#;
    let config: DbTomlConfig = toml::from_str(toml).unwrap();

    let manager = DbManager::new_in_memory().await.unwrap();
    manager.bootstrap(&config).await.unwrap();

    // Verify database was created
    let dbs = manager.list_dbs().await;
    assert!(dbs.contains(&"testdb".to_string()));

    // Verify table was created
    let tables = manager.list_tables("testdb").await.unwrap();
    assert!(tables.contains(&"items".to_string()));
}

#[tokio::test]
async fn bootstrap_is_idempotent() {
    let toml = r#"
        [databases.testdb]

        [databases.testdb.tables.items]
        sql = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)"
    "#;
    let config: DbTomlConfig = toml::from_str(toml).unwrap();

    let manager = DbManager::new_in_memory().await.unwrap();

    // Bootstrap twice — should not error
    manager.bootstrap(&config).await.unwrap();
    manager.bootstrap(&config).await.unwrap();

    let tables = manager.list_tables("testdb").await.unwrap();
    assert_eq!(tables, vec!["items".to_string()]);
}

#[tokio::test]
async fn bootstrap_multiple_databases() {
    let toml = r#"
        [databases.db1]
        [databases.db1.tables.t1]
        sql = "CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY)"

        [databases.db2]
        [databases.db2.tables.t2]
        sql = "CREATE TABLE IF NOT EXISTS t2 (id INTEGER PRIMARY KEY)"
    "#;
    let config: DbTomlConfig = toml::from_str(toml).unwrap();

    let manager = DbManager::new_in_memory().await.unwrap();
    manager.bootstrap(&config).await.unwrap();

    let dbs = manager.list_dbs().await;
    assert!(dbs.contains(&"db1".to_string()));
    assert!(dbs.contains(&"db2".to_string()));

    assert!(
        manager
            .list_tables("db1")
            .await
            .unwrap()
            .contains(&"t1".to_string())
    );
    assert!(
        manager
            .list_tables("db2")
            .await
            .unwrap()
            .contains(&"t2".to_string())
    );
}
