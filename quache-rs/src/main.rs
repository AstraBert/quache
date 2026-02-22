mod core;
mod server;

use std::time;

use anyhow::Result;
use clap::Parser;

use crate::{core::KVStore, server::KVStoreServer};

const DEFAULT_DIRECTORY: &str = ".quache/";
const DEFAULT_SHARD_NUMBER: usize = 5;
const DEFAULT_FLUSHING_INTERVAL: u64 = 1000;
const DEFAULT_CLEANUP_INTERVAL: u64 = 500;

/// quache is a single-node in-memory KV store that can be served as an API server
#[derive(Debug, Parser)]
struct CliArgs {
    /// Directory which to flush the KV store data to. Defaults to .quache/
    #[arg(short, long, default_value=None)]
    directory: Option<String>,

    /// Number of shards to use to vertically shard the KV store. Defaults to 5.
    #[arg(short, long, default_value_t = DEFAULT_SHARD_NUMBER)]
    shards: usize,

    /// Load the KV store from disk. Does not load from disk by default
    #[arg(short, long, default_value_t = false)]
    load: bool,

    /// Host to bind the server to. Defaults to 0.0.0.0
    #[arg(short, long, default_value = None)]
    bind: Option<String>,

    /// Port to which to bind the server to. Defaults to 8000
    #[arg(short, long, default_value = None)]
    port: Option<u16>,

    /// Flushing interval (in ms). Defaults to 1000ms
    #[arg(short, long, default_value_t = DEFAULT_FLUSHING_INTERVAL)]
    flushing_interval: u64,

    /// Cleanup (of expired entries) interval (in ms). Defaults to 5ß0ms
    #[arg(short, long, default_value_t = DEFAULT_CLEANUP_INTERVAL)]
    cleanup_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let actual_dir = match args.directory {
        None => DEFAULT_DIRECTORY.to_string(),
        Some(d) => d,
    };
    let kv_store = if !args.load {
        KVStore::new(args.shards, actual_dir)?
    } else {
        KVStore::new_from_disk(args.shards, actual_dir)?
    };
    let server = KVStoreServer::new(args.port, args.bind);
    let mut kv_1 = kv_store.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(time::Duration::from_millis(args.flushing_interval));
            let flush_result = kv_1.to_disk();
            match flush_result {
                Ok(_) => {}
                Err(e) => eprintln!(
                    "An error occurred while flushing to disk: {}",
                    e.to_string()
                ),
            }
        }
    });

    let kv_2 = kv_store.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(time::Duration::from_millis(args.cleanup_interval));
            let cleanup_result = kv_2.cleanup();
            match cleanup_result {
                Ok(_) => {}
                Err(e) => eprintln!(
                    "An error occurred while cleaning up expired entries: {}",
                    e.to_string()
                ),
            }
        }
    });

    server.serve(kv_store).await?;

    Ok(())
}
