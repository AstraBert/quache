use serde::{Deserialize, Serialize};

use crate::core::KVStore;

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
