use dkdc_db_core::{DbConfig, DkdcDb};

#[tokio::test]
async fn custom_path_works() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    let config = DbConfig::with_path(&db_path);

    let db = DkdcDb::open_with_config(config).await.unwrap();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1)").await.unwrap();
    let batches = db.query("SELECT * FROM t").await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    // Verify file exists
    assert!(db_path.exists());
}

#[tokio::test]
async fn default_path_uses_dkdc_home() {
    let tmp = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("DKDC_HOME", tmp.path().to_str().unwrap()) };

    let db = DkdcDb::open("testdb").await.unwrap();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();

    let expected = tmp.path().join("db").join("testdb.db");
    assert!(
        expected.exists(),
        "DB file should be at ~/.dkdc/db/testdb.db"
    );

    unsafe { std::env::remove_var("DKDC_HOME") };
}
