use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use dkdc_db_core::DbManager;
use serde::{Deserialize, Serialize};

/// Max request body size: 16 MB
const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

type AppState = Arc<DbManager>;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/db", post(create_db))
        .route("/db", get(list_dbs))
        .route("/db/{name}", delete(drop_db))
        .route("/db/{name}/execute", post(execute))
        .route("/db/{name}/query/oltp", post(query_oltp))
        .route("/db/{name}/tables", get(list_tables))
        .route("/db/{name}/schema/{table}", get(table_schema))
        .route("/query", post(query))
        .route("/health", get(health))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateDbRequest {
    name: String,
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

async fn create_db(
    State(mgr): State<AppState>,
    Json(req): Json<CreateDbRequest>,
) -> impl IntoResponse {
    match mgr.create_db(&req.name).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"name": req.name})),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn drop_db(State(mgr): State<AppState>, Path(name): Path<String>) -> impl IntoResponse {
    match mgr.drop_db(&name).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"dropped": name}))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn list_dbs(State(mgr): State<AppState>) -> impl IntoResponse {
    let dbs = mgr.list_dbs().await;
    (StatusCode::OK, Json(dbs))
}

async fn execute(
    State(mgr): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    match mgr.execute(&name, &req.sql).await {
        Ok(affected) => (StatusCode::OK, Json(ExecuteResponse { affected })).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn query(State(mgr): State<AppState>, Json(req): Json<SqlRequest>) -> impl IntoResponse {
    match mgr.query(&req.sql).await {
        Ok(batches) => (StatusCode::OK, Json(batches_to_response(&batches))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn query_oltp(
    State(mgr): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    match mgr.query_oltp(&name, &req.sql).await {
        Ok(batches) => (StatusCode::OK, Json(batches_to_response(&batches))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn list_tables(State(mgr): State<AppState>, Path(name): Path<String>) -> impl IntoResponse {
    match mgr.list_tables(&name).await {
        Ok(tables) => (StatusCode::OK, Json(tables)).into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn table_schema(
    State(mgr): State<AppState>,
    Path((name, table)): Path<(String, String)>,
) -> impl IntoResponse {
    match mgr.table_schema(&name, &table).await {
        Ok(batches) => (StatusCode::OK, Json(batches_to_response(&batches))).into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}
