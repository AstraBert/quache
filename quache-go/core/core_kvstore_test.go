package core

import (
	"errors"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const TestDirectory string = ".quache-test/"

func makeTestDirectory() error {
	if _, err := os.Stat(TestDirectory); err == nil { // exists
		return nil
	}
	return os.Mkdir(TestDirectory, 0775)
}

func cleanupTestDirectory() error {
	files := []string{path.Join(TestDirectory, "shard-0"), path.Join(TestDirectory, "shard-1"), path.Join(TestDirectory, "shard-2")}
	for _, file := range files {
		if _, err := os.Stat(file); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		err := os.Remove(file)
		if err != nil {
			return err
		}
	}
	return os.Remove(TestDirectory)
}

func TestNewKVStore(t *testing.T) {
	store := NewKVStore(5, TestDirectory)
	assert.Len(t, store.Shards, 5, "Should contain five shards")
	assert.Equal(t, store.shardDimensions[0], 0, "Shards should be empty")
	assert.Equal(t, store.Directory, TestDirectory, "Store should be initialized witht he correct directory")
}

func TestFindShard(t *testing.T) {
	store := NewKVStore(3, TestDirectory)
	assert.Equal(t, store.findShard("notthekindofthingyouwouldfind"), 0, "'notthekindofthingyouwouldfind' should be routed to the 0-th shard")
	assert.Equal(t, store.findShard("thisisaverylongkey"), 1, "'thisisaverylongkey' should be routed to the first shard")
	assert.Equal(t, store.findShard("this is an interesting key"), 2, "'this is an interesting key' should be routed to the second shard")
}

func TestPut(t *testing.T) {
	store := NewKVStore(3, TestDirectory)
	store.Put("thisisaverylongkey", 1, nil)
	assert.Equal(t, store.Shards[1].getLength(), 1, "The first shard should have a dimension of 1")
	store.Put("notthekindofthingyouwouldfind", 2, nil)
	assert.Equal(t, store.Shards[0].getLength(), 1, "The 0-th shard should have a dimension of 1")
	store.Put("this is an interesting key", 3, nil)
	assert.Equal(t, store.Shards[2].getLength(), 1, "The second shard should have a dimension of 1")
	store.Put("thisisaverylongkey", 2, nil)
	assert.Equal(t, store.Shards[1].getLength(), 1, "The first shard should still have a dimension of 1 (updated not appended)")
	store.Put("hey", 4, nil) // hey should be routed to the 2nd shard
	assert.Equal(t, store.Shards[2].getLength(), 2, "The second shard should have a dimension of 2")
}

func TestGet(t *testing.T) {
	store := NewKVStore(3, TestDirectory)
	var ttl float64 = 0.001 // one millisecond
	store.Put("hello", 1, nil)
	store.Put("bye", 2, &ttl)
	time.Sleep(3 * time.Millisecond)
	val, err := store.Get("hello")
	assert.Nil(t, err, "Should be able to retrieve the 'hello' key")
	assert.Equal(t, val, 1, "Value should be equal to 1")
	_, err = store.Get("bye")
	assert.NotNil(t, err, "Error should be non-nil when retrieving 'bye' past-ttl")
	assert.Contains(t, err.Error(), "is expired")
	_, err = store.Get("nonexisting")
	assert.NotNil(t, err, "Error should be non-nil when retrieving a non-existing key")
	assert.Contains(t, err.Error(), "not found")
}

func TestDelete(t *testing.T) {
	store := NewKVStore(3, TestDirectory)
	store.Put("hello", 1, nil)
	store.Delete("hello")
	_, err := store.Get("hello")
	assert.NotNil(t, err, "Error should be non-nil when retrieving a deleted key")
	assert.Contains(t, err.Error(), "not found")
	store.Delete("bye") // does not panic when Delete is called on a non-existing key
}

func TestCleanup(t *testing.T) {
	store := NewKVStore(3, TestDirectory)
	ttl := 0.001                                       // 1 millisecond
	var ttl1 float64 = 1                               // 1 second
	store.Put("notthekindofthingyouwouldfind", 1, nil) // 0-th shard
	store.Put("thisisaverylongkey", 2, &ttl)           // 1st shard
	store.Put("this is an interesting key", 3, &ttl1)  // 2nd shard
	store.Put("hey", 4, &ttl)                          // 2nd shard
	time.Sleep(3 * time.Millisecond)
	store.Cleanup()
	assert.Equal(t, store.Shards[0].getLength(), 1, "Key should have been evicted from shard 0")
	assert.Equal(t, store.Shards[1].getLength(), 0, "Key should not have been evicted shard 1")
	assert.Equal(t, store.Shards[2].getLength(), 1, "Only one key should remain in shard 2")
}

func TestToAndFromDisk(t *testing.T) {
	err := makeTestDirectory()
	if err != nil {
		t.Fatalf("An error occurred while creating the test directory: %s", err.Error())
	}
	store := NewKVStore(3, TestDirectory)
	store.Put("notthekindofthingyouwouldfind", 1, nil) // 0-th shard
	store.Put("thisisaverylongkey", 2, nil)            // 1st shard
	store.Put("this is an interesting key", 3, nil)    // 2nd shard
	store.Put("hey", 4, nil)                           // 2nd shard
	err = store.ToDisk()
	assert.Nil(t, err, "Error should be nil when flushing to disk")
	store1, err := NewKVStoreFromDisk(3, TestDirectory)
	assert.Nil(t, err, "Error should be nil when loading from disk")
	val1, err := store1.Get("notthekindofthingyouwouldfind")
	assert.Nil(t, err, "Error should be nil when retrieving an existing key")
	assert.Equal(t, val1, float64(1))
	val2, err := store1.Get("thisisaverylongkey")
	assert.Nil(t, err, "Error should be nil when retrieving an existing key")
	assert.Equal(t, val2, float64(2))
	val3, err := store1.Get("this is an interesting key")
	assert.Nil(t, err, "Error should be nil when retrieving an existing key")
	assert.Equal(t, val3, float64(3))
	val4, err := store1.Get("hey")
	assert.Nil(t, err, "Error should be nil when retrieving an existing key")
	assert.Equal(t, val4, float64(4))
	_ = cleanupTestDirectory()
}
