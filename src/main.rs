use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;

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
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    let pool_size = env_usize("WORKER_POOL_SIZE", 4);
    let channel_capacity = env_usize("WORKER_CHANNEL_CAPACITY", 16);
    let max_conns = (pool_size + 1) as u32;

    let pool = PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    // Spawn N worker tasks, each with its own bounded channel.
    // Handlers partition operations by team_id so same-team ops serialize
    // to one worker while different teams run in parallel.
    let mut senders = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let worker_pool = pool.clone();
        tokio::spawn(async move {
            db::worker_loop(worker_pool, rx).await;
        });
        senders.push(tx);
    }

    println!(
        "spawned {pool_size} workers (channel capacity {channel_capacity}, db pool {max_conns})"
    );

    let state = AppState {
        workers: Arc::from(senders.into_boxed_slice()),
    };

    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/create", post(handlers::create))
        .route("/identify", post(handlers::identify))
        .route("/alias", post(handlers::alias))
        .route("/merge", post(handlers::merge))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");

    axum::serve(listener, app).await.expect("server error");
}
