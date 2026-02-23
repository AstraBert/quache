package core

import "testing"

func BenchmarkPutGetDelete1Shard(b *testing.B) {
	kvStore := NewKVStore(1, TestDirectory)
	key := "hello"
	value := 1
	b.ResetTimer()
	for b.Loop() {
		kvStore.Put(key, value, nil)
		kvStore.Get(key)
		kvStore.Delete(key)
	}
}

func BenchmarkPutGetDelete10Shards(b *testing.B) {
	kvStore := NewKVStore(10, TestDirectory)
	key := "hello"
	value := 1
	b.ResetTimer()
	for b.Loop() {
		kvStore.Put(key, value, nil)
		kvStore.Get(key)
		kvStore.Delete(key)
	}
}

func BenchmarkPutGetDelete100Shards(b *testing.B) {
	kvStore := NewKVStore(100, TestDirectory)
	key := "hello"
	value := 1
	b.ResetTimer()
	for b.Loop() {
		kvStore.Put(key, value, nil)
		kvStore.Get(key)
		kvStore.Delete(key)
	}
}
