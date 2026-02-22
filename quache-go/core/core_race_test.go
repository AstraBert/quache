package core

import (
	"fmt"
	"sync"
	"testing"
)

func TestKVStoreRaceConditionPutGetSingleShard(t *testing.T) {
	kvStore := NewKVStore(1, TestDirectory)

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id, j)
				kvStore.Put(key, "value", nil)
			}
		}(i)
	}

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id%5, j)
				kvStore.Get(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestKVStoreRaceConditionPutGetMultipleShards(t *testing.T) {
	kvStore := NewKVStore(3, TestDirectory)

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id, j)
				kvStore.Put(key, "value", nil)
			}
		}(i)
	}

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id%5, j)
				kvStore.Get(key)
			}
		}(i)
	}

	wg.Wait()
}
