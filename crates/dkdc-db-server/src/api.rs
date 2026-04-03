use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use dkdc_db_core::DkdcDb;
use serde::{Deserialize, Serialize};

type AppState = Arc<DkdcDb>;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/execute", post(execute))
        .route("/query", post(query))
        .route("/query/libsql", post(query_libsql))
        .route("/tables", get(list_tables))
        .route("/schema/{table}", get(table_schema))
        .route("/health", get(health))
        .with_state(state)
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Serialize)]
struct ExecuteResponse {
    affected: u64,
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<ColumnInfo>,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    r#type: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn error_response(status: StatusCode, msg: impl ToString) -> impl IntoResponse {
    (
        status,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn batches_to_response(batches: &[dkdc_db_core::RecordBatch]) -> QueryResponse {
    let mut columns = Vec::new();
    let mut rows = Vec::new();

    if let Some(first) = batches.first() {
        let schema = first.schema();
        columns = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                r#type: format!("{}", f.data_type()),
            })
            .collect();
    }

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = column_value_to_json(col, row_idx);
                row.push(value);
            }
            rows.push(row);
        }
    }

    QueryResponse { columns, rows }
}

fn column_value_to_json(col: &dyn arrow::array::Array, row: usize) -> serde_json::Value {
    use arrow::array::*;

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        serde_json::Value::Number(arr.value(row).into())
    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        serde_json::json!(arr.value(row))
    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        serde_json::Value::String(arr.value(row).to_string())
    } else if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        use base64::Engine;
        let bytes = arr.value(row);
        serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
    } else {
        // Fallback: try to format as string
        serde_json::Value::String(format!("{:?}", col))
    }
}

async fn execute(State(db): State<AppState>, Json(req): Json<SqlRequest>) -> impl IntoResponse {
    match db.execute(&req.sql).await {
        Ok(affected) => (StatusCode::OK, Json(ExecuteResponse { affected })).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn query(State(db): State<AppState>, Json(req): Json<SqlRequest>) -> impl IntoResponse {
    match db.query(&req.sql).await {
        Ok(batches) => (StatusCode::OK, Json(batches_to_response(&batches))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn query_libsql(
    State(db): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    match db.query_libsql(&req.sql).await {
        Ok(batches) => (StatusCode::OK, Json(batches_to_response(&batches))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn list_tables(State(db): State<AppState>) -> impl IntoResponse {
    match db.list_tables().await {
        Ok(tables) => (StatusCode::OK, Json(tables)).into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn table_schema(State(db): State<AppState>, Path(table): Path<String>) -> impl IntoResponse {
    let sql = format!(
        "SELECT name, type FROM pragma_table_info('{}')",
        table.replace('\'', "''")
    );
    match db.query_libsql(&sql).await {
        Ok(batches) => (StatusCode::OK, Json(batches_to_response(&batches))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}
