package core

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const TestFlushFile string = "shard-0-test"

func cleanupTestFlushFile() error {
	if _, err := os.Stat(TestFlushFile); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return os.Remove(TestFlushFile)
}

func TestNewShard(t *testing.T) {
	s := NewShard()
	assert.Equal(t, len(s.Data), 0, "data should be empty")
}

func TestNewShardWithData(t *testing.T) {
	d := map[string]*ShardEntry{"hello": {Value: 1, Ttl: -1, Timestamp: 123}}
	s := NewShardWithData(d)
	assert.Equal(t, len(s.Data), 1, "data should be non-empty")
	val, ok := s.Data["hello"]
	assert.True(t, ok, "'hello' should be in the shard's data")
	if ok {
		assert.Equal(t, val.Value, 1, "stored value should be 1")
		assert.Equal(t, val.Timestamp, int64(123), "stored timestamp should be 123")
		assert.Equal(t, val.Ttl, float64(-1), "stored ttl should be -1")
	}
}

func TestShardGetLength(t *testing.T) {
	d := map[string]*ShardEntry{"hello": {Value: 1, Ttl: -1, Timestamp: 123}}
	s := NewShardWithData(d)
	assert.Equal(t, s.getLength(), 1, "length should be 1")
	s.Data["bye"] = &ShardEntry{Value: 2, Ttl: -1, Timestamp: 234}
	assert.Equal(t, s.getLength(), 2, "length should be 2")
}

func TestShardEvict(t *testing.T) {
	d := map[string]*ShardEntry{
		"hello": {Value: 1, Ttl: 10, Timestamp: 123},
		"bye":   {Value: 1, Ttl: 10000, Timestamp: time.Now().UnixMilli()},
		"hey":   {Value: 2, Ttl: -1, Timestamp: 123},
	}
	s := NewShardWithData(d)
	s.Evict()
	assert.Equal(t, s.getLength(), 2, "length after eviction should be 2")
	assert.Contains(t, s.Data, "bye", "should contain bye (eviction time not yet reached)")
	assert.Contains(t, s.Data, "hey", "should contain hey (no TTL, permanently stored)")
}

func TestShardFlush(t *testing.T) {
	d := map[string]*ShardEntry{
		"hello": {Value: 1, Ttl: -1, Timestamp: 123},
		"bye":   {Value: 2, Ttl: -1, Timestamp: 456},
		"hey":   {Value: 3, Ttl: -1, Timestamp: 789},
	}
	s := NewShardWithData(d)
	err := s.Flush(TestFlushFile)
	assert.Nil(t, err, "error should be null")
	bData, err := os.ReadFile(TestFlushFile)
	if err != nil {
		t.Fatalf("An error occurred while reading the file: %s", err.Error())
	}
	bDataS := string(bData)
	lines := strings.Split(bDataS, "\n")
	integrityHashS := lines[len(lines)-1]
	integrityHash, err := hex.DecodeString(integrityHashS)
	if err != nil {
		t.Fatalf("An error occurred while loading the integrity hash: %s", err.Error())
	}
	mapData := []byte(strings.Join(lines[:len(lines)-1], "\n"))
	actualHash := md5.Sum(mapData)
	if !slices.Equal(actualHash[:], integrityHash) {
		t.Fatal("Integrity hash is not the same as the flushed one")
	}
	var data map[string]*ShardEntry
	err = json.Unmarshal(mapData, &data)
	if err != nil {
		t.Fatalf("An error occurred while unmarshaling the data: %s", err.Error())
	}
	val1, ok := data["hello"]
	assert.True(t, ok, "Map should contain 'hello'")
	if ok {
		assert.Equal(t, val1.Value, float64(1))
		assert.Equal(t, val1.Timestamp, int64(123))
		assert.Equal(t, val1.Ttl, float64(-1))
	}
	val2, ok := data["bye"]
	assert.True(t, ok, "Map should contain 'bye'")
	if ok {
		assert.Equal(t, val2.Value, float64(2))
		assert.Equal(t, val2.Timestamp, int64(456))
		assert.Equal(t, val2.Ttl, float64(-1))
	}
	val3, ok := data["hey"]
	assert.True(t, ok, "Map should contain 'hey'")
	if ok {
		assert.Equal(t, val3.Value, float64(3))
		assert.Equal(t, val3.Timestamp, int64(789))
		assert.Equal(t, val3.Ttl, float64(-1))
	}
	_ = cleanupTestFlushFile()
}
