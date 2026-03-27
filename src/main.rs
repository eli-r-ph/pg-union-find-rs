use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tower_http::catch_panic::CatchPanicLayer;

use pg_union_find_rs::handlers::AppState;
use pg_union_find_rs::{db, handlers};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.replace('_', "").parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pg_union_find_rs=info".parse().unwrap()),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    let pool_size = env_usize("WORKER_POOL_SIZE", 4);
    let channel_capacity = env_usize("WORKER_CHANNEL_CAPACITY", 16);
    let max_conns = (pool_size + 1) as u32;

    if pool_size == 0 {
        tracing::error!("WORKER_POOL_SIZE must be >= 1");
        std::process::exit(1);
    }

    let pool = PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, "failed to connect to database");
            std::process::exit(1);
        });

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, "failed to run migrations");
            std::process::exit(1);
        });

    let mut senders = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let worker_pool = pool.clone();
        tokio::spawn(async move {
            db::worker_loop(worker_pool, rx).await;
        });
        senders.push(tx);
    }

    tracing::info!(pool_size, channel_capacity, max_conns, "spawned workers");

    let state = AppState {
        workers: Arc::from(senders.into_boxed_slice()),
    };

    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/create", post(handlers::create))
        .route("/identify", post(handlers::identify))
        .route("/alias", post(handlers::alias))
        .route("/merge", post(handlers::merge))
        .with_state(state)
        .layer(CatchPanicLayer::custom(
            |_: Box<dyn std::any::Any + Send>| {
                let body = serde_json::json!({"error": "internal server error"});
                (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
            },
        ));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!(%addr, "listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, %addr, "failed to bind");
            std::process::exit(1);
        });

    axum::serve(listener, app).await.unwrap_or_else(|e| {
        tracing::error!(%e, "server error");
        std::process::exit(1);
    });
}
