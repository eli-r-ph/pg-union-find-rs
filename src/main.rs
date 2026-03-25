use std::net::SocketAddr;

use axum::{Router, routing::{get, post}};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;

use pg_union_find_rs::{db, handlers};
use pg_union_find_rs::handlers::AppState;

#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:54320/union_find".into()
    });

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    // Bounded channel — backpressure kicks in at 1024 pending ops.
    let (tx, rx) = mpsc::channel(1024);

    // Spawn the single DB-worker task that processes ops sequentially.
    let worker_pool = pool.clone();
    tokio::spawn(async move {
        db::worker_loop(worker_pool, rx).await;
    });

    let state = AppState { tx };

    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/identify", post(handlers::identify))
        .route("/create_alias", post(handlers::create_alias))
        .route("/merge", post(handlers::merge))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");

    axum::serve(listener, app).await.expect("server error");
}
