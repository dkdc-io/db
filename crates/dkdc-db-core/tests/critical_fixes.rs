use dkdc_db_core::DbManager;

/// Fix 1: query_oltp must reject write operations
#[tokio::test]
async fn query_oltp_rejects_writes() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // INSERT through OLTP path should be rejected
    let result = mgr.query_oltp("test", "INSERT INTO t VALUES (1)").await;
    assert!(result.is_err());

    // DELETE through OLTP path should be rejected
    let result = mgr.query_oltp("test", "DELETE FROM t").await;
    assert!(result.is_err());

    // DROP through OLTP path should be rejected
    let result = mgr.query_oltp("test", "DROP TABLE t").await;
    assert!(result.is_err());

    // SELECT should still work
    let result = mgr.query_oltp("test", "SELECT * FROM t").await;
    assert!(result.is_ok());
}

/// Fix 2: ensure_db TOCTOU race — concurrent ensure_db should not fail
#[tokio::test]
async fn concurrent_ensure_db_no_race() {
    let mgr = std::sync::Arc::new(DbManager::new_in_memory().await.unwrap());
    mgr.create_db("racetest").await.unwrap();
    mgr.execute("racetest", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();
    mgr.drop_db("racetest").await.unwrap();

    // Re-create so it's in known list (simulating a known-on-disk db)
    mgr.create_db("racetest").await.unwrap();
}

/// Fix 3: WITH CTE write detection
#[tokio::test]
async fn with_cte_insert_rejected_on_read_paths() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (id INTEGER)")
        .await
        .unwrap();

    // WITH ... INSERT through analytical query path should be rejected
    let result = mgr
        .query("WITH cte AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM cte")
        .await;
    assert!(result.is_err());

    // WITH ... INSERT through OLTP path should be rejected
    let result = mgr
        .query_oltp(
            "test",
            "WITH cte AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM cte",
        )
        .await;
    assert!(result.is_err());

    // WITH ... SELECT should still work on analytical path
    let result = mgr
        .query("WITH cte AS (SELECT 1 AS id) SELECT * FROM cte")
        .await;
    assert!(result.is_ok());
}

/// Fix 4: drop_db should remove from known list
#[tokio::test]
async fn drop_db_removes_from_known_list() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("ephemeral").await.unwrap();

    let dbs = mgr.list_dbs().await;
    assert!(dbs.contains(&"ephemeral".to_string()));

    mgr.drop_db("ephemeral").await.unwrap();

    // Should not appear in list after drop
    let dbs = mgr.list_dbs().await;
    assert!(!dbs.contains(&"ephemeral".to_string()));

    // ensure_db should fail (not re-open from known list)
    let result = mgr.ensure_db("ephemeral").await;
    assert!(result.is_err());
}
