package workers

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/AstraBert/quache/quache-go/core"
	"github.com/stretchr/testify/assert"
)

const TestDirectory string = ".quache-workers/"

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

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func TestToDiskWorker(t *testing.T) {
	err := makeTestDirectory()
	if err != nil {
		t.Fatalf("Could not create test directory")
	}

	kvStore := core.NewKVStore(3, TestDirectory)
	// only .quache-workers/shard-0 and .quache-workers/shard-1 should exist
	kvStore.Put("notthekindofthingyouwouldfind", 1, nil) // 0-th shard
	kvStore.Put("thisisaverylongkey", 2, nil)            // 1st shard
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
	defer cancel()
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT)

	// flushes every 1 ms
	ToDiskWorker(kvStore, 1, done, ctx)

	assert.True(t, fileExists(path.Join(TestDirectory, "shard-0")))
	assert.True(t, fileExists(path.Join(TestDirectory, "shard-1")))
	assert.False(t, fileExists(path.Join(TestDirectory, "shard-2")))

	_ = cleanupTestDirectory()
}

func TestCleanupWorker(t *testing.T) {
	kvStore := core.NewKVStore(3, TestDirectory)
	var ttl float64 = 0.001                               // 1 millisecond
	var ttl1 float64 = 1                                  // 1 second
	kvStore.Put("notthekindofthingyouwouldfind", 1, &ttl) // 0-th shard
	kvStore.Put("thisisaverylongkey", 2, &ttl1)           // 1st shard
	kvStore.Put("hey", 3, nil)                            // 2nd shard
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
	defer cancel()
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT)

	// cleans up every 1 ms
	CleanupWorker(kvStore, 1, done, ctx)

	assert.NotContains(t, kvStore.Shards[0].Data, "notthekindofthingyouwouldfind")
	assert.Contains(t, kvStore.Shards[1].Data, "thisisaverylongkey")
	assert.Contains(t, kvStore.Shards[2].Data, "hey")
}
