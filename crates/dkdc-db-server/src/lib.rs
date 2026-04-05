pub mod api;
pub mod ui;

use std::sync::Arc;

use dkdc_db_core::DbManager;

/// Start the server on the given address with graceful shutdown on SIGINT/SIGTERM.
pub async fn serve(manager: Arc<DbManager>, host: &str, port: u16) -> std::io::Result<()> {
    let app = api::routes()
        .merge(ui::ui_routes())
        .layer(axum::extract::DefaultBodyLimit::max(api::MAX_BODY_SIZE))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(api::timeout_middleware))
        .with_state(manager);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("dkdc-db server listening on {addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
