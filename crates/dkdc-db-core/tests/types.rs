use arrow::datatypes::DataType;
use dkdc_db_core::DkdcDb;

#[tokio::test]
async fn all_sqlite_types_roundtrip_through_arrow() {
    let db = DkdcDb::open_in_memory().await.unwrap();

    db.execute(
        "CREATE TABLE type_test (
            int_col INTEGER,
            real_col REAL,
            text_col TEXT,
            blob_col BLOB,
            null_col TEXT
        )",
    )
    .await
    .unwrap();

    db.execute("INSERT INTO type_test VALUES (42, 3.14, 'hello', X'DEADBEEF', NULL)")
        .await
        .unwrap();

    // Read via DataFusion
    let batches = db.query("SELECT * FROM type_test").await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify Arrow types
    let schema = batch.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(3).data_type(), &DataType::Binary);
    assert_eq!(schema.field(4).data_type(), &DataType::Utf8);

    // Verify values via DataFusion
    let int_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(int_arr.value(0), 42);

    let real_arr = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((real_arr.value(0) - 3.14).abs() < f64::EPSILON);

    let text_arr = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(text_arr.value(0), "hello");

    let blob_arr = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    assert_eq!(blob_arr.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);

    // null_col should be null
    assert!(batch.column(4).is_null(0));

    // Verify same data via turso
    let ls_batches = db.query_oltp("SELECT * FROM type_test").await.unwrap();
    assert_eq!(ls_batches.len(), 1);
    let ls_batch = &ls_batches[0];
    assert_eq!(ls_batch.num_rows(), 1);

    let ls_int = ls_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(ls_int.value(0), 42);
}
