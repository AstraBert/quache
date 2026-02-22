use std::{
    collections::HashMap,
    fs,
    sync::{Arc, RwLock},
    time,
};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShardEntry {
    ttl: f64,
    value: serde_json::Value,
    timestamp: u128,
}

#[derive(Debug, Clone)]
pub struct Shard {
    data: Arc<RwLock<HashMap<String, ShardEntry>>>,
}

#[derive(Debug, Clone)]
pub struct KVStore {
    shards: Vec<Shard>,
    directory: String,
    shard_dimensions: HashMap<usize, usize>,
}

impl ShardEntry {
    pub fn new(value: serde_json::Value, ttl: Option<f64>) -> Self {
        let actual_ttl = match ttl {
            None => -1_f64,
            Some(f) => f,
        };
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        Self {
            value,
            timestamp,
            ttl: actual_ttl,
        }
    }
}

impl Shard {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn new_with_data(data: HashMap<String, ShardEntry>) -> Self {
        Self {
            data: Arc::new(RwLock::new(data)),
        }
    }

    pub fn flush(&self, file_name: String) -> Result<()> {
        let data = self.data.read().map_err(|e| anyhow!(e.to_string()))?;
        if data.len() == 0 {
            return Ok(());
        }
        let to_write = serde_json::to_string(&*data)?;
        let integrity_hash = md5::compute(&to_write.clone().into_bytes());
        let integrity_hash_string: String = integrity_hash
            .to_vec()
            .iter()
            .map(|c| c.to_string())
            .collect();
        let full_content = format!("{}\n{}", to_write, integrity_hash_string);
        fs::write(file_name, full_content.into_bytes())?;
        Ok(())
    }

    pub fn evict(&self) -> Result<()> {
        let mut data = self.data.write().map_err(|e| anyhow!(e.to_string()))?;
        if data.len() == 0 {
            return Ok(());
        }
        let current_time = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let keys_to_remove: Vec<String> = data
            .iter()
            .filter(|(_, entry)| {
                entry.ttl > 0_f64 && ((current_time - entry.timestamp) as f64) > entry.ttl
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            data.remove(&key);
        }
        Ok(())
    }

    fn get_length(&self) -> Result<usize> {
        let data = self.data.read().map_err(|e| anyhow!(e.to_string()))?;
        Ok(data.len())
    }
}

impl KVStore {
    pub fn new(num_shards: usize, directory: String) -> Result<Self> {
        if !fs::exists(&directory)? {
            fs::create_dir_all(&directory)?;
        }
        let mut shards: Vec<Shard> = vec![];
        let mut i = 0;
        while i < num_shards {
            shards.push(Shard::new());
            i += 1;
        }
        Ok(Self {
            directory,
            shards,
            shard_dimensions: HashMap::new(),
        })
    }

    pub fn new_from_disk(num_shards: usize, directory: String) -> Result<Self> {
        if !fs::exists(&directory)? {
            return Err(anyhow!("directory {} does not exist", &directory));
        }
        let mut shards: Vec<Shard> = vec![];
        let mut i = 0;
        while i < num_shards {
            let file_path = format!("{}/shard-{:?}", &directory.trim_end_matches("/"), i);
            if fs::exists(&file_path)? {
                let content = fs::read_to_string(&file_path)?;
                let lines: Vec<&str> = content.split("\n").collect();
                let integrity_hash_str = lines[lines.len() - 1].to_string();
                let raw_data = lines[0..lines.len() - 1].join("\n");
                let computed_hash = md5::compute(&raw_data.clone().into_bytes());
                let computed_hash_string: String = computed_hash
                    .to_vec()
                    .iter()
                    .map(|c| c.to_string())
                    .collect();
                if integrity_hash_str != computed_hash_string {
                    return Err(anyhow!(
                        "could not load shard {:?} because the computed hash does not match the reported integrity hash",
                        i
                    ));
                }
                let data: HashMap<String, ShardEntry> = serde_json::from_str(&raw_data)?;
                shards.push(Shard::new_with_data(data));
                i += 1;
            } else {
                shards.push(Shard::new());
            }
            i += 1;
        }
        Ok(Self {
            shards,
            directory,
            shard_dimensions: HashMap::new(),
        })
    }

    fn find_shard(&self, key: &str) -> usize {
        let hash = crc32fast::hash(key.as_bytes()) as usize;
        hash % self.shards.len()
    }

    pub fn put(&self, key: String, value: serde_json::Value, ttl: Option<f64>) -> Result<()> {
        let shard_idx = self.find_shard(&key);
        let entry = ShardEntry::new(value, ttl);
        let mut data = self.shards[shard_idx]
            .data
            .write()
            .map_err(|e| anyhow!(e.to_string()))?;
        data.entry(key)
            .and_modify(|v| *v = entry.clone())
            .or_insert(entry);

        Ok(())
    }

    pub fn get(&self, key: String) -> Result<serde_json::Value> {
        let shard_idx = self.find_shard(&key);
        let data = self.shards[shard_idx]
            .data
            .read()
            .map_err(|e| anyhow!(e.to_string()))?;
        match data.get(&key) {
            None => return Err(anyhow!("key {} not found", key)),
            Some(entry) => return Ok(entry.value.clone()),
        }
    }

    pub fn delete(&self, key: String) -> Result<()> {
        let shard_idx = self.find_shard(&key);
        let mut data = self.shards[shard_idx]
            .data
            .write()
            .map_err(|e| anyhow!(e.to_string()))?;
        data.remove(&key);
        Ok(())
    }

    pub fn to_disk(&mut self) -> Result<()> {
        let mut i = 0;
        while i < self.shards.len() {
            let shard_length = self.shards[i].get_length()?;
            let stored_shard_length: usize = match self.shard_dimensions.get(&i) {
                None => 0,
                Some(s) => *s,
            };
            if shard_length == stored_shard_length {
                // no changes, do not flush
                continue;
            }
            self.shard_dimensions
                .entry(i)
                .and_modify(|v| *v = shard_length)
                .or_insert(shard_length);
            let file_path = format!("{}/shard-{:?}", &self.directory.trim_end_matches("/"), i);
            self.shards[i].flush(file_path)?;
            i += 1;
        }
        Ok(())
    }

    pub fn cleanup(&self) -> Result<()> {
        let mut i = 0;
        while i < self.shards.len() {
            self.shards[i].evict()?;
            i += 1;
        }
        Ok(())
    }
}
