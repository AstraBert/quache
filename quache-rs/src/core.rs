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
    shard_dimensions: Arc<RwLock<HashMap<usize, usize>>>,
}

impl ShardEntry {
    pub fn new(value: serde_json::Value, ttl: Option<f64>) -> Self {
        let actual_ttl = match ttl {
            None => -1_f64,
            Some(f) => f * 1000_f64,
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
            shard_dimensions: Arc::new(RwLock::new(HashMap::new())),
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
                println!("Loading shard {:?} from file", i);
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
            } else {
                println!(
                    "File for shard {:?} not found, initializing an empty shard...",
                    i
                );
                shards.push(Shard::new());
            }
            i += 1;
        }
        Ok(Self {
            shards,
            directory,
            shard_dimensions: Arc::new(RwLock::new(HashMap::new())),
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
            let stored_shard_length: usize = {
                let dims = self
                    .shard_dimensions
                    .read()
                    .map_err(|e| anyhow!(e.to_string()))?;
                dims.get(&i).copied().unwrap_or(0)
            };
            if shard_length == stored_shard_length {
                // no changes, do not flush
                i += 1;
                continue;
            }
            {
                let mut dims = self
                    .shard_dimensions
                    .write()
                    .map_err(|e| anyhow!(e.to_string()))?;
                dims.entry(i)
                    .and_modify(|v| *v = shard_length)
                    .or_insert(shard_length);
            }

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

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    fn cleanup_test_file(file_name: String) {
        if fs::exists(&file_name).expect("Should be able to check file existence") {
            fs::remove_file(file_name).expect("Should be able to remove file");
        }
    }

    fn cleanup_test_directory(directory_name: String) {
        if fs::exists(&directory_name).expect("Should be able to check directory existence") {
            fs::remove_dir_all(directory_name).expect("Should be able to remove directory content");
        }
    }

    #[test]
    fn test_shard_entry_init() {
        let shard_entry = ShardEntry::new(serde_json::Value::from("hello"), Some(0.001));
        assert_eq!(shard_entry.value, serde_json::Value::from("hello"));
        assert_eq!(shard_entry.ttl, 1_f64);
        let current_time = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        assert!(current_time >= shard_entry.timestamp);
    }

    #[test]
    fn test_shard_empty_init() {
        let shard = Shard::new();
        let data = shard.data.read().expect("Should be able to read data");
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn test_shard_with_data_init() {
        let mut init_data: HashMap<String, ShardEntry> = HashMap::new();
        init_data.insert(
            "hello".to_string(),
            ShardEntry::new(serde_json::Value::from(1), None),
        );
        init_data.insert(
            "hey".to_string(),
            ShardEntry::new(serde_json::Value::from(2), Some(2_f64)),
        );
        let shard = Shard::new_with_data(init_data);
        let data = shard.data.read().expect("Should be able to read data");
        assert_eq!(data.len(), 2);
        let hello_entry = data
            .get("hello")
            .expect("Should be able to retrieve 'hello' key");
        let hey_entry = data
            .get("hey")
            .expect("Should be able to retrieve 'hey' key");
        assert_eq!(hello_entry.value, serde_json::Value::from(1));
        assert_eq!(hey_entry.value, serde_json::Value::from(2));
        assert_eq!(hello_entry.ttl, -1_f64);
        assert_eq!(hey_entry.ttl, 2000_f64);
    }

    #[test]
    fn test_shard_get_length() {
        let mut init_data: HashMap<String, ShardEntry> = HashMap::new();
        init_data.insert(
            "hello".to_string(),
            ShardEntry::new(serde_json::Value::from(1), None),
        );
        init_data.insert(
            "hey".to_string(),
            ShardEntry::new(serde_json::Value::from(2), Some(2_f64)),
        );
        let shard = Shard::new_with_data(init_data);
        assert_eq!(
            shard
                .get_length()
                .expect("should be able to retrieve length"),
            2
        );
        let shard_1 = Shard::new();
        assert_eq!(
            shard_1
                .get_length()
                .expect("should be able to retrieve length"),
            0
        );
    }

    #[test]
    fn test_shard_evict() {
        let mut init_data: HashMap<String, ShardEntry> = HashMap::new();
        init_data.insert(
            "hello".to_string(),
            ShardEntry::new(serde_json::Value::from(1), None),
        );
        init_data.insert(
            "hey".to_string(),
            ShardEntry::new(serde_json::Value::from(2), Some(0.001)), // 1 millisecond
        );
        init_data.insert(
            "bye".to_string(),
            ShardEntry::new(serde_json::Value::from(3), Some(2_f64)), // 2 seconds
        );
        let shard = Shard::new_with_data(init_data);
        assert_eq!(shard.get_length().expect("Should be able to get length"), 3);
        std::thread::sleep(time::Duration::from_millis(5)); // this should discard the 'hey' entry
        shard
            .evict()
            .expect("Should be able to evict expired entries");
        assert_eq!(shard.get_length().expect("Should be able to get length"), 2);
        let data = shard.data.read().expect("Should be able to read data");
        assert_eq!(data.len(), 2);
        let hello_entry = data.get("hello");
        assert!(hello_entry.is_some());
        let bye_entry = data.get("bye");
        assert!(bye_entry.is_some());
        let hey_entry = data.get("hey");
        assert!(hey_entry.is_none());
    }

    #[test]
    fn test_shard_flush() {
        let mut init_data: HashMap<String, ShardEntry> = HashMap::new();
        init_data.insert(
            "hello".to_string(),
            ShardEntry::new(serde_json::Value::from(1), None),
        );
        init_data.insert(
            "hey".to_string(),
            ShardEntry::new(serde_json::Value::from(2), None),
        );
        let shard = Shard::new_with_data(init_data);
        shard
            .flush("shard-0-test".to_string())
            .expect("Should be able to flush to file");

        assert!(fs::exists("shard-0-test").expect("Should be able to check file existence"));
        let content = fs::read_to_string("shard-0-test").expect("Should be able to read file path");
        let lines: Vec<&str> = content.split("\n").collect();
        let integrity_hash_str = lines[lines.len() - 1].to_string();
        let raw_data = lines[0..lines.len() - 1].join("\n");
        let computed_hash = md5::compute(&raw_data.clone().into_bytes());
        let computed_hash_string: String = computed_hash
            .to_vec()
            .iter()
            .map(|c| c.to_string())
            .collect();
        assert_eq!(integrity_hash_str, computed_hash_string);
        let data: HashMap<String, ShardEntry> =
            serde_json::from_str(&raw_data).expect("Should be able to deserialize data");
        assert_eq!(data.len(), 2);
        let hello_entry = data
            .get("hello")
            .expect("Should be able to retrieve 'hello' key");
        let hey_entry = data
            .get("hey")
            .expect("Should be able to retrieve 'hey' key");
        assert_eq!(hello_entry.value, serde_json::Value::from(1));
        assert_eq!(hey_entry.value, serde_json::Value::from(2));
        assert_eq!(hello_entry.ttl, -1_f64);
        assert_eq!(hey_entry.ttl, -1_f64);

        cleanup_test_file("shard-0-test".to_string())
    }

    #[test]
    #[serial]
    fn test_kv_store_init() {
        let kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        assert!(fs::exists(".quache-test/").expect("Should be able to check directory existence"));
        let shard_dimensions = kv_store
            .shard_dimensions
            .read()
            .expect("Should be able to acquire read lock");
        assert_eq!(shard_dimensions.len(), 0);
        assert_eq!(kv_store.shards.len(), 3);

        cleanup_test_directory(".quache-test/".to_string());
    }

    #[test]
    #[serial]
    fn test_kv_store_find_shard() {
        let kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        let shard_num_0 = kv_store.find_shard("notthekindofthingyouwouldfind");
        assert_eq!(shard_num_0, 0);
        let shard_num_1 = kv_store.find_shard("thisisaverylongkey");
        assert_eq!(shard_num_1, 1);
        let shard_num_2 = kv_store.find_shard("this is an interesting key");
        assert_eq!(shard_num_2, 2);

        cleanup_test_directory(".quache-test/".to_string());
    }

    #[test]
    #[serial]
    fn test_kv_store_put() {
        let kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        kv_store
            .put("hey".to_string(), serde_json::Value::from(1), None)
            .expect("Should be able to call .put without errors"); // goes to shard-2
        assert_eq!(
            kv_store.shards[2]
                .get_length()
                .expect("Should be able to get length"),
            1
        );
        assert_eq!(
            kv_store.shards[1]
                .get_length()
                .expect("Should be able to get length"),
            0
        );
        assert_eq!(
            kv_store.shards[0]
                .get_length()
                .expect("Should be able to get length"),
            0
        );
        let data = kv_store.shards[2]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(data.contains_key("hey"));
        cleanup_test_directory(".quache-test/".to_string());
    }

    #[test]
    #[serial]
    fn test_kv_store_get() {
        let kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        kv_store
            .put("hey".to_string(), serde_json::Value::from(1), None)
            .expect("Should be able to call .put without errors"); // goes to shard-2
        let result = kv_store
            .get("hey".to_string())
            .expect("Should be able to get the 'hey' key");
        assert_eq!(result, serde_json::Value::from(1));
        let notfound = kv_store.get("hello".to_string());
        assert_eq!(
            notfound.is_err_and(|e| e.to_string().contains("not found")),
            true
        );

        cleanup_test_directory(".quache-test/".to_string());
    }

    #[test]
    #[serial]
    fn test_kv_store_delete() {
        let kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        kv_store
            .put("hello".to_string(), serde_json::Value::from(1), None)
            .expect("Should be able to call .put without errors"); // goes to shard-2
        kv_store // delete existing key
            .delete("hello".to_string())
            .expect("Should be able to delete key");
        let notfound = kv_store.get("hello".to_string());
        assert_eq!(
            notfound.is_err_and(|e| e.to_string().contains("not found")),
            true
        );
        let delete_not_exist = kv_store.delete("hello".to_string());
        assert!(delete_not_exist.is_ok()); // assert that delete with non-existing key is just a no-op

        cleanup_test_directory(".quache-test/".to_string());
    }

    #[test]
    #[serial]
    fn test_kv_store_cleanup() {
        let kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        kv_store
            .put("hey".to_string(), serde_json::Value::from(1), None)
            .expect("Should be able to call .put without errors"); // goes to shard-2
        kv_store
            .put(
                "thisisaverylongkey".to_string(),
                serde_json::Value::from(1),
                Some(1_f64), // 1 second ttl
            )
            .expect("Should be able to call .put without errors"); // goes to shard-1
        kv_store
            .put(
                "notthekindofthingyouwouldfind".to_string(),
                serde_json::Value::from(3),
                Some(0.001), // 1 millisecond ttl
            )
            .expect("Should be able to call .put without errors"); // goes to shard-0
        std::thread::sleep(time::Duration::from_millis(5)); // should be enough to evict key from shard-0
        kv_store
            .cleanup()
            .expect("Should be able to clean up the KV store");

        assert_eq!(
            kv_store.shards[2]
                .get_length()
                .expect("Should be able to get length"),
            1
        );
        assert_eq!(
            kv_store.shards[1]
                .get_length()
                .expect("Should be able to get length"),
            1
        );
        assert_eq!(
            kv_store.shards[0]
                .get_length()
                .expect("Should be able to get length"),
            0
        );
        let data_2 = kv_store.shards[2]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(data_2.contains_key("hey"));
        let data_1 = kv_store.shards[1]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(data_1.contains_key("thisisaverylongkey"));

        let data_0 = kv_store.shards[0]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(!data_0.contains_key("notthekindofthingyouwouldfind"));

        cleanup_test_directory(".quache-test/".to_string());
    }

    #[test]
    #[serial]
    fn test_kv_store_flush_and_restore_from_memory() {
        let mut kv_store = KVStore::new(3, ".quache-test/".to_string())
            .expect("Should be able to create KV store");
        kv_store
            .put("hey".to_string(), serde_json::Value::from(1), None)
            .expect("Should be able to call .put without errors"); // goes to shard-2
        kv_store
            .put(
                "thisisaverylongkey".to_string(),
                serde_json::Value::from(2),
                None,
            )
            .expect("Should be able to call .put without errors"); // goes to shard-1
        kv_store
            .put(
                "notthekindofthingyouwouldfind".to_string(),
                serde_json::Value::from(3),
                None,
            )
            .expect("Should be able to call .put without errors"); // goes to shard-0
        kv_store.to_disk().expect("Should be able to flush to disk");
        let shard_dimensions = kv_store
            .shard_dimensions
            .read()
            .expect("Should be able to acquire read lock");
        let shard_nums: Vec<usize> = vec![0, 1, 2];
        for i in &shard_nums {
            match shard_dimensions.get(i) {
                Some(d) => {
                    assert_eq!(*d, 1);
                }
                None => {
                    eprintln!("No dimension found for shard {:?}", i);
                    assert!(false); // fail here
                }
            }
        }
        let kv_store_1 = KVStore::new_from_disk(3, ".quache-test/".to_string())
            .expect("Should be able to create the KV Store from disk");

        assert_eq!(
            kv_store_1.shards[2]
                .get_length()
                .expect("Should be able to get length"),
            1
        );
        assert_eq!(
            kv_store_1.shards[1]
                .get_length()
                .expect("Should be able to get length"),
            1
        );
        assert_eq!(
            kv_store_1.shards[0]
                .get_length()
                .expect("Should be able to get length"),
            1
        );
        let data_2 = kv_store_1.shards[2]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(data_2.contains_key("hey"));
        let data_1 = kv_store_1.shards[1]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(data_1.contains_key("thisisaverylongkey"));

        let data_0 = kv_store_1.shards[0]
            .data
            .read()
            .expect("Should be able to acquire read lock");
        assert!(data_0.contains_key("notthekindofthingyouwouldfind"));

        cleanup_test_directory(".quache-test/".to_string());
    }
}
