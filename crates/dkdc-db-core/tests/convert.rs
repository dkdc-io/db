use arrow::array::{Array, BinaryArray, Float64Array, Int64Array, StringArray};
use dkdc_db_core::DbManager;

/// SQLite is dynamically typed — a REAL value can be stored in an INTEGER column.
/// The OLTP path infers types from first row data, not column declarations.
/// The DataFusion path uses schema declarations.
#[tokio::test]
async fn int_column_with_real_value_datafusion() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val INTEGER)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (3.7)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (42)")
        .await
        .unwrap();

    // DataFusion path uses the schema declaration (INTEGER → Int64)
    let batches = mgr.query("SELECT val FROM test.public.t").await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Real 3.7 cast to i64 = 3
    assert_eq!(arr.value(0), 3);
    assert_eq!(arr.value(1), 42);
}

/// OLTP path infers Float64 from first row's actual Real value.
#[tokio::test]
async fn int_column_with_real_value_oltp() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val INTEGER)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (3.7)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (42)")
        .await
        .unwrap();

    // OLTP path infers type from first row (3.7 = Real → Float64)
    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((arr.value(0) - 3.7).abs() < f64::EPSILON);
    // Integer 42 cast to f64
    assert!((arr.value(1) - 42.0).abs() < f64::EPSILON);
}

/// Text stored in an integer column should parse or default to 0.
#[tokio::test]
async fn int_column_with_text_value() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val INTEGER)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES ('123')")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES ('not_a_number')")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 123);
    assert_eq!(arr.value(1), 0); // unparseable defaults to 0
}

/// Text stored in a float column should parse or default to 0.0.
#[tokio::test]
async fn float_column_with_text_value() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val REAL)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES ('2.718')")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES ('abc')")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((arr.value(0) - 2.718).abs() < f64::EPSILON);
    assert!((arr.value(1) - 0.0).abs() < f64::EPSILON);
}

/// Integer stored in a float column should cast cleanly.
#[tokio::test]
async fn float_column_with_integer_value() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val REAL)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (42)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (-7)")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    assert!((arr.value(1) - (-7.0)).abs() < f64::EPSILON);
}

/// Integer and real values stored in a text column should stringify.
#[tokio::test]
async fn text_column_with_numeric_values() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val TEXT)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (42)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (3.14)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES ('hello')")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "42");
    assert_eq!(arr.value(1), "3.14");
    assert_eq!(arr.value(2), "hello");
}

/// Text stored in blob column — OLTP infers Utf8 from first row's Text value.
#[tokio::test]
async fn binary_column_with_text_value_oltp() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val BLOB)")
        .await
        .unwrap();
    // First row is text, so OLTP infers Utf8
    mgr.execute("test", "INSERT INTO t VALUES ('hello')")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "hello");
}

/// Blob data via DataFusion path uses schema declaration (BLOB → Binary).
#[tokio::test]
async fn binary_column_with_blob_data_datafusion() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val BLOB)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (X'CAFE')")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (X'DEADBEEF')")
        .await
        .unwrap();

    let batches = mgr.query("SELECT val FROM test.public.t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(arr.value(0), &[0xCA, 0xFE]);
    assert_eq!(arr.value(1), &[0xDE, 0xAD, 0xBE, 0xEF]);
}

/// Pure blob data via OLTP — first row is Blob so infers Binary.
#[tokio::test]
async fn binary_column_with_blob_data_oltp() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val BLOB)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (X'CAFE')")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(arr.value(0), &[0xCA, 0xFE]);
}

/// A column with all NULL values should produce all nulls without panicking.
#[tokio::test]
async fn all_null_column() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BLOB)")
        .await
        .unwrap();
    for _ in 0..5 {
        mgr.execute("test", "INSERT INTO t VALUES (NULL, NULL, NULL, NULL)")
            .await
            .unwrap();
    }

    let batches = mgr.query_oltp("test", "SELECT * FROM t").await.unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 5);

    for col_idx in 0..4 {
        for row_idx in 0..5 {
            assert!(batch.column(col_idx).is_null(row_idx));
        }
    }
}

/// Mixed null and non-null values in every column type.
#[tokio::test]
async fn mixed_null_non_null_all_types() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BLOB)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (1, 1.0, 'x', X'AA')")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (NULL, NULL, NULL, NULL)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (3, 3.0, 'z', X'BB')")
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT * FROM t").await.unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);

    // Row 0: all non-null
    for col in 0..4 {
        assert!(!batch.column(col).is_null(0));
    }
    // Row 1: all null
    for col in 0..4 {
        assert!(batch.column(col).is_null(1));
    }
    // Row 2: all non-null
    for col in 0..4 {
        assert!(!batch.column(col).is_null(2));
    }
}

/// Boolean values in SQLite are stored as integers (0/1).
#[tokio::test]
async fn boolean_as_integer() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (flag INTEGER)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (1)") // true
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (0)") // false
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT flag FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 1);
    assert_eq!(arr.value(1), 0);
}

/// Integer stored in blob column — OLTP infers Int64, DataFusion uses Binary (null fallback).
#[tokio::test]
async fn binary_column_integer_oltp_infers_int() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val BLOB)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (12345)")
        .await
        .unwrap();

    // OLTP infers Int64 from the integer value
    let batches = mgr.query_oltp("test", "SELECT val FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 12345);
}

/// Integer stored in blob column — DataFusion uses Binary schema, integer falls to null.
#[tokio::test]
async fn binary_column_integer_datafusion_fallback_null() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (val BLOB)")
        .await
        .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (12345)")
        .await
        .unwrap();

    let batches = mgr.query("SELECT val FROM test.public.t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    // Integer in Binary builder → null fallback
    assert!(arr.is_null(0));
}

/// Verify both OLTP and DataFusion paths produce consistent results for type coercion.
#[tokio::test]
async fn oltp_and_datafusion_consistency() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (i INTEGER, r REAL, t TEXT, b BLOB)")
        .await
        .unwrap();
    mgr.execute(
        "test",
        "INSERT INTO t VALUES (42, 3.14, 'hello', X'DEADBEEF')",
    )
    .await
    .unwrap();
    mgr.execute("test", "INSERT INTO t VALUES (NULL, NULL, NULL, NULL)")
        .await
        .unwrap();

    let oltp = mgr.query_oltp("test", "SELECT * FROM t").await.unwrap();
    let df = mgr.query("SELECT * FROM test.public.t").await.unwrap();

    assert_eq!(oltp[0].num_rows(), df[0].num_rows());
    assert_eq!(oltp[0].num_columns(), df[0].num_columns());

    // Same schema types
    for i in 0..oltp[0].num_columns() {
        assert_eq!(
            oltp[0].schema().field(i).data_type(),
            df[0].schema().field(i).data_type()
        );
    }

    // Same nullability pattern
    for col in 0..oltp[0].num_columns() {
        for row in 0..oltp[0].num_rows() {
            assert_eq!(
                oltp[0].column(col).is_null(row),
                df[0].column(col).is_null(row)
            );
        }
    }
}

/// Large blob roundtrip.
#[tokio::test]
async fn large_blob_roundtrip() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (data BLOB)")
        .await
        .unwrap();

    // Insert a 10KB blob via hex
    let blob_size = 10_000;
    let hex: String = (0..blob_size)
        .map(|i| format!("{:02X}", (i % 256) as u8))
        .collect();
    mgr.execute("test", &format!("INSERT INTO t VALUES (X'{hex}')"))
        .await
        .unwrap();

    let batches = mgr.query_oltp("test", "SELECT data FROM t").await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(arr.value(0).len(), blob_size);
}

/// Multiple rows with mixed types to stress the builder append loop.
#[tokio::test]
async fn many_rows_mixed_types() {
    let mgr = DbManager::new_in_memory().await.unwrap();
    mgr.create_db("test").await.unwrap();
    mgr.execute("test", "CREATE TABLE t (id INTEGER, score REAL, name TEXT)")
        .await
        .unwrap();

    for i in 0..200 {
        let name = if i % 3 == 0 {
            "NULL"
        } else {
            &format!("'user_{i}'")
        };
        let score = if i % 5 == 0 {
            "NULL".to_string()
        } else {
            format!("{}.{}", i, i % 10)
        };
        mgr.execute(
            "test",
            &format!("INSERT INTO t VALUES ({i}, {score}, {name})"),
        )
        .await
        .unwrap();
    }

    let batches = mgr
        .query_oltp("test", "SELECT count(*) FROM t")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 200);

    // Verify via DataFusion too
    let batches = mgr
        .query("SELECT count(*) FROM test.public.t")
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 200);
}
