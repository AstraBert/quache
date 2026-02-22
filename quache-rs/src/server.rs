use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::core::KVStore;

const DEFAULT_PORT: u16 = 8000;
const DEFAULT_HOST: &str = "0.0.0.0";

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let code: StatusCode = if self.0.to_string().contains("not found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        (code, format!("Error: {}", self.0)).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

#[derive(Clone, Debug)]
struct AppState {
    kv_store: KVStore,
}

#[derive(Deserialize, Serialize, Debug)]
struct GetResponse {
    value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug)]
struct PutRequest {
    key: String,
    value: serde_json::Value,
    ttl: Option<f64>,
}

pub struct KVStoreServer {
    pub host: IpAddr,
    pub port: u16,
}

async fn handle_post(
    State(state): State<AppState>,
    Json(payload): Json<PutRequest>,
) -> Result<StatusCode, AppError> {
    state
        .kv_store
        .put(payload.key, payload.value, payload.ttl)?;
    Ok(StatusCode::CREATED)
}

async fn handle_get(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, AppError> {
    let value = state.kv_store.get(key)?;
    Ok(Json(GetResponse { value }))
}

async fn handle_delete(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, AppError> {
    state.kv_store.delete(key)?;
    Ok(StatusCode::NO_CONTENT)
}

impl KVStoreServer {
    pub fn new(port: Option<u16>, host: Option<String>) -> Self {
        let server_port = match port {
            Some(n) => n,
            None => DEFAULT_PORT,
        };
        let server_host = match host {
            Some(h) => {
                IpAddr::V4(Ipv4Addr::from_str(&h).expect("You should provide a valid IPv4 address"))
            }
            None => IpAddr::V4(
                Ipv4Addr::from_str(DEFAULT_HOST).expect("You should provide a valid IPv4 address"),
            ),
        };

        Self {
            port: server_port,
            host: server_host,
        }
    }

    pub async fn serve(&self, kv_store: KVStore) -> anyhow::Result<()> {
        let state = AppState { kv_store };
        let app = Router::new()
            .route("/kv", post(handle_post))
            .route("/kv/{key}", get(handle_get).delete(handle_delete))
            .with_state(state);
        let addr = SocketAddr::from((self.host, self.port));
        let listener = tokio::net::TcpListener::bind(addr).await?;
        println!("Starting to serve on {}:{:?}", self.host, self.port);
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
        Ok(())
    }
}
