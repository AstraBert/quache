package core

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"
)

type ShardEntry struct {
	Value     any     `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Ttl       float64 `json:"ttl"`
}

type Shard struct {
	Data map[string]*ShardEntry
	mu   sync.RWMutex
}

type KVStore struct {
	Shards          []*Shard
	Directory       string
	shardDimensions map[int]int
}

type KeyNotFoundError struct {
	key string
}

func (e KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key %s not found", e.key)
}

type ExpiredEntryError struct {
	key     string
	elapsed int64
	ttl     float64
}

func (e ExpiredEntryError) Error() string {
	return fmt.Sprintf("Key %s is expired (requested TTL: %f, elapsed: %d)", e.key, e.ttl, e.elapsed)
}

type UnloadableShardError struct {
	shardNum  int
	errorType string
}

func (e UnloadableShardError) Error() string {
	return fmt.Sprintf("Shard %d could not be loaded because %s", e.shardNum, e.errorType)
}

func NewShardEntry(value any, ttl float64) *ShardEntry {
	ts := time.Now().UnixMilli()
	return &ShardEntry{
		Value:     value,
		Timestamp: ts,
		Ttl:       ttl,
	}
}

func NewShard() *Shard {
	return &Shard{
		Data: make(map[string]*ShardEntry),
	}
}

func NewShardWithData(data map[string]*ShardEntry) *Shard {
	return &Shard{
		Data: data,
	}
}

func NewKVStore(numShards int, directory string) *KVStore {
	c := &KVStore{Shards: make([]*Shard, 0, numShards), Directory: directory, shardDimensions: make(map[int]int)}
	for i := range numShards {
		c.Shards = append(c.Shards, NewShard())
		c.shardDimensions[i] = 0
	}
	return c
}

func NewKVStoreFromDisk(numShards int, directory string) (*KVStore, error) {
	c := &KVStore{Shards: make([]*Shard, 0, numShards), Directory: directory, shardDimensions: make(map[int]int)}
	for i := range numShards {
		fileName := path.Join(directory, fmt.Sprintf("shard-%d", i))
		if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
			fmt.Printf("File associated with shard %d does not exist, creating a new empty shard\n", i)
			c.Shards = append(c.Shards, NewShard())
			c.shardDimensions[i] = 0
			continue
		}
		bData, err := os.ReadFile(fileName)
		if err != nil {
			return nil, NewUnloadableShardError(i, err.Error())
		}
		bDataS := string(bData)
		lines := strings.Split(bDataS, "\n")
		integrityHashS := lines[len(lines)-1]
		integrityHash, err := hex.DecodeString(integrityHashS)
		if err != nil {
			return nil, NewUnloadableShardError(i, err.Error())
		}
		mapData := []byte(strings.Join(lines[:len(lines)-1], "\n"))
		actualHash := md5.Sum(mapData)
		if !slices.Equal(actualHash[:], integrityHash) {
			return nil, NewUnloadableShardError(i, "the computed hash does not match the integrity hash reported in the file")
		}
		var data map[string]*ShardEntry
		err = json.Unmarshal(mapData, &data)
		if err != nil {
			return nil, NewUnloadableShardError(i, err.Error())
		}
		c.shardDimensions[i] = len(data)
		c.Shards = append(c.Shards, NewShardWithData(data))
	}
	return c, nil
}

func NewKeyNotFoundError(key string) KeyNotFoundError {
	return KeyNotFoundError{key: key}
}

func NewExpiredEntryError(key string, ttl float64, elapsed int64) ExpiredEntryError {
	return ExpiredEntryError{key: key, ttl: ttl, elapsed: elapsed}
}

func NewUnloadableShardError(shardNum int, errorType string) UnloadableShardError {
	return UnloadableShardError{shardNum: shardNum, errorType: errorType}
}

func (s *Shard) Evict() {
	currentTime := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.Data) == 0 {
		return
	}
	for key, value := range s.Data {
		if value.Ttl > 0 && float64(currentTime-value.Timestamp) > value.Ttl {
			delete(s.Data, key)
		}
	}
}

func (s *Shard) Flush(fileName string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.Data) == 0 {
		return nil
	}
	data, err := json.Marshal(s.Data)
	if err != nil {
		return err
	}
	hash := md5.Sum(data)
	encoded := hex.EncodeToString(hash[:])
	toWrite := append(data, append([]byte("\n"), []byte(encoded)...)...)
	return os.WriteFile(fileName, toWrite, 0644)
}

func (s *Shard) getLength() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Data)
}

func (kv *KVStore) findShard(key string) int {
	hash := crc32.ChecksumIEEE([]byte(key))
	return int(hash) % len(kv.Shards)
}

func (kv *KVStore) Put(key string, value any, ttl *float64) {
	var actualTtl float64
	switch ttl {
	case nil:
		actualTtl = -1
	default:
		actualTtl = *ttl * 1000
	}
	shardIdx := kv.findShard(key)
	kv.Shards[shardIdx].mu.Lock()
	defer kv.Shards[shardIdx].mu.Unlock()
	kv.Shards[shardIdx].Data[key] = NewShardEntry(value, actualTtl)
}

func (kv *KVStore) Get(key string) (any, error) {
	currentTime := time.Now().UnixMilli()
	shardIdx := kv.findShard(key)
	kv.Shards[shardIdx].mu.RLock()
	defer kv.Shards[shardIdx].mu.RUnlock()
	val, ok := kv.Shards[shardIdx].Data[key]
	if ok {
		if val.Ttl > 0 && float64(currentTime-val.Timestamp) > val.Ttl {
			return nil, NewExpiredEntryError(key, val.Ttl, (currentTime - val.Timestamp))
		}
		return val.Value, nil
	}
	return nil, NewKeyNotFoundError(key)
}

func (kv *KVStore) Delete(key string) {
	shardIdx := kv.findShard(key)
	kv.Shards[shardIdx].mu.Lock()
	defer kv.Shards[shardIdx].mu.Unlock()
	delete(kv.Shards[shardIdx].Data, key)
}

func (kv *KVStore) Cleanup() {
	for i := range kv.Shards {
		kv.Shards[i].Evict()
	}
}

func (kv *KVStore) ToDisk() error {
	for i := range kv.Shards {
		fileName := path.Join(kv.Directory, fmt.Sprintf("shard-%d", i))
		shardLength := kv.Shards[i].getLength()
		if kv.shardDimensions[i] == shardLength {
			// no new content, keep as-is
			continue
		}
		kv.shardDimensions[i] = shardLength
		err := kv.Shards[i].Flush(fileName)
		if err != nil {
			return err
		}
	}
	return nil
}
