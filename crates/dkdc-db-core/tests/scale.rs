use arrow::array::Array;
use dkdc_db_core::DkdcDb;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: i64 = 100_000;
const REGIONS: &[&str] = &[
    "North",
    "South",
    "East",
    "West",
    "Central",
    "Northeast",
    "Southeast",
    "Northwest",
    "Southwest",
    "Midwest",
];
const NUM_PRODUCTS: i64 = 100;
const SEED: u64 = 42;

async fn setup_sales_db() -> DkdcDb {
    let db = DkdcDb::open_in_memory().await.unwrap();

    db.execute(
        "CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            region TEXT,
            product TEXT,
            amount REAL,
            quantity INTEGER,
            ts INTEGER
        )",
    )
    .await
    .unwrap();

    let mut rng = StdRng::seed_from_u64(SEED);

    // Insert 100K rows in batches of 10K for speed
    let batch_size = 10_000;
    for batch_start in (0..NUM_ROWS).step_by(batch_size as usize) {
        db.execute("BEGIN").await.unwrap();
        let batch_end = (batch_start + batch_size).min(NUM_ROWS);
        for i in batch_start..batch_end {
            let region = REGIONS[rng.random_range(0..REGIONS.len())];
            let product = format!("product_{}", rng.random_range(0..NUM_PRODUCTS));
            let amount: f64 = rng.random_range(1.0..1000.0);
            let quantity: i64 = rng.random_range(1..100);
            // Timestamps spanning a year (2025-01-01 to 2025-12-31 in unix seconds)
            let ts: i64 = rng.random_range(1704067200..1735689600);
            db.execute(&format!(
                "INSERT INTO sales VALUES ({i}, '{region}', '{product}', {amount}, {quantity}, {ts})"
            ))
            .await
            .unwrap();
        }
        db.execute("COMMIT").await.unwrap();
    }

    db.refresh_schema().await.unwrap();
    db
}

fn count_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn get_f64_column(
    batches: &[arrow::record_batch::RecordBatch],
    col_idx: usize,
) -> Vec<Option<f64>> {
    let mut values = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(arr.value(i)));
                }
            }
        }
    }
    values
}

fn get_i64_column(
    batches: &[arrow::record_batch::RecordBatch],
    col_idx: usize,
) -> Vec<Option<i64>> {
    let mut values = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(arr.value(i)));
                }
            }
        }
    }
    values
}

fn get_string_column(
    batches: &[arrow::record_batch::RecordBatch],
    col_idx: usize,
) -> Vec<Option<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(arr.value(i).to_string()));
                }
            }
        }
    }
    values
}

#[tokio::test]
async fn scale_total_revenue_per_region() {
    let db = setup_sales_db().await;

    let sql =
        "SELECT region, SUM(amount) as total_revenue FROM sales GROUP BY region ORDER BY region";

    let df_result = db.query(sql).await.unwrap();
    let ls_result = db.query_turso(sql).await.unwrap();

    // Both should return 10 regions
    assert_eq!(count_rows(&df_result), 10);
    assert_eq!(count_rows(&ls_result), 10);

    // Compare region names
    let df_regions = get_string_column(&df_result, 0);
    let ls_regions = get_string_column(&ls_result, 0);
    assert_eq!(df_regions, ls_regions);

    // Compare revenue totals (within floating point tolerance)
    let df_revenue = get_f64_column(&df_result, 1);
    let ls_revenue = get_f64_column(&ls_result, 1);
    assert_eq!(df_revenue.len(), ls_revenue.len());
    for (df_val, ls_val) in df_revenue.iter().zip(ls_revenue.iter()) {
        let df_v = df_val.unwrap();
        let ls_v = ls_val.unwrap();
        let diff = (df_v - ls_v).abs();
        assert!(
            diff < 0.01,
            "Revenue mismatch: DataFusion={df_v}, turso={ls_v}"
        );
    }
}

#[tokio::test]
async fn scale_top_10_products_by_revenue() {
    let db = setup_sales_db().await;

    let sql = "SELECT product, SUM(amount) as total_revenue FROM sales GROUP BY product ORDER BY total_revenue DESC LIMIT 10";

    let df_result = db.query(sql).await.unwrap();
    let ls_result = db.query_turso(sql).await.unwrap();

    assert_eq!(count_rows(&df_result), 10);
    assert_eq!(count_rows(&ls_result), 10);

    // Compare product names (order should match)
    let df_products = get_string_column(&df_result, 0);
    let ls_products = get_string_column(&ls_result, 0);
    assert_eq!(df_products, ls_products);

    // Compare revenue
    let df_revenue = get_f64_column(&df_result, 1);
    let ls_revenue = get_f64_column(&ls_result, 1);
    for (df_val, ls_val) in df_revenue.iter().zip(ls_revenue.iter()) {
        let diff = (df_val.unwrap() - ls_val.unwrap()).abs();
        assert!(diff < 0.01, "Revenue mismatch in top 10 products");
    }
}

#[tokio::test]
async fn scale_multi_level_aggregation() {
    let db = setup_sales_db().await;

    let sql = "SELECT region, product, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg_amount \
               FROM sales GROUP BY region, product ORDER BY region, product";

    let df_result = db.query(sql).await.unwrap();
    let ls_result = db.query_turso(sql).await.unwrap();

    let df_rows = count_rows(&df_result);
    let ls_rows = count_rows(&ls_result);
    assert_eq!(
        df_rows, ls_rows,
        "Row count mismatch: DF={df_rows}, LS={ls_rows}"
    );

    // Should be up to 10 regions * 100 products = 1000 combinations
    assert!(df_rows > 0 && df_rows <= 1000);

    // Compare counts
    let df_counts = get_i64_column(&df_result, 2);
    let ls_counts = get_i64_column(&ls_result, 2);
    assert_eq!(
        df_counts, ls_counts,
        "Count mismatch in multi-level aggregation"
    );

    // Compare sums (floating point tolerance)
    let df_sums = get_f64_column(&df_result, 3);
    let ls_sums = get_f64_column(&ls_result, 3);
    for (i, (df_val, ls_val)) in df_sums.iter().zip(ls_sums.iter()).enumerate() {
        let diff = (df_val.unwrap() - ls_val.unwrap()).abs();
        assert!(
            diff < 0.01,
            "Sum mismatch at row {i}: DF={:?}, LS={:?}",
            df_val,
            ls_val
        );
    }
}

#[tokio::test]
async fn scale_self_join_above_average_revenue() {
    let db = setup_sales_db().await;

    let sql = "SELECT r.region, r.total_revenue \
               FROM (SELECT region, SUM(amount) as total_revenue FROM sales GROUP BY region) r \
               WHERE r.total_revenue > (SELECT AVG(total_revenue) FROM (SELECT region, SUM(amount) as total_revenue FROM sales GROUP BY region)) \
               ORDER BY r.total_revenue DESC";

    let df_result = db.query(sql).await.unwrap();
    let ls_result = db.query_turso(sql).await.unwrap();

    let df_rows = count_rows(&df_result);
    let ls_rows = count_rows(&ls_result);
    assert_eq!(
        df_rows, ls_rows,
        "Row count mismatch for above-avg regions: DF={df_rows}, LS={ls_rows}"
    );
    assert!(
        df_rows > 0 && df_rows < 10,
        "Should have some but not all regions above average"
    );

    let df_regions = get_string_column(&df_result, 0);
    let ls_regions = get_string_column(&ls_result, 0);
    assert_eq!(df_regions, ls_regions);

    let df_revenue = get_f64_column(&df_result, 1);
    let ls_revenue = get_f64_column(&ls_result, 1);
    for (df_val, ls_val) in df_revenue.iter().zip(ls_revenue.iter()) {
        let diff = (df_val.unwrap() - ls_val.unwrap()).abs();
        assert!(diff < 0.01);
    }
}

#[tokio::test]
async fn scale_subquery_products_above_avg_quantity() {
    let db = setup_sales_db().await;

    let sql = "SELECT p.product, p.avg_qty \
               FROM (SELECT product, AVG(quantity) as avg_qty FROM sales GROUP BY product) p, \
                    (SELECT AVG(quantity) as overall_avg FROM sales) o \
               WHERE p.avg_qty > o.overall_avg \
               ORDER BY p.avg_qty DESC";

    let df_result = db.query(sql).await.unwrap();
    let ls_result = db.query_turso(sql).await.unwrap();

    let df_rows = count_rows(&df_result);
    let ls_rows = count_rows(&ls_result);
    assert_eq!(
        df_rows, ls_rows,
        "Row count mismatch for above-avg quantity products: DF={df_rows}, LS={ls_rows}"
    );
    assert!(
        df_rows > 0,
        "Should have some products above average quantity"
    );

    let df_products = get_string_column(&df_result, 0);
    let ls_products = get_string_column(&ls_result, 0);
    assert_eq!(df_products, ls_products);

    let df_qty = get_f64_column(&df_result, 1);
    let ls_qty = get_f64_column(&ls_result, 1);
    for (df_val, ls_val) in df_qty.iter().zip(ls_qty.iter()) {
        let diff = (df_val.unwrap() - ls_val.unwrap()).abs();
        assert!(diff < 0.01);
    }
}
