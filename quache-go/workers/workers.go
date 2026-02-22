package workers

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/AstraBert/quache/quache-go/core"
)

func ToDiskWorker(kvStore *core.KVStore, flushingInterval int, done <-chan os.Signal, ctx context.Context) {
	ticker := time.NewTicker(time.Duration(flushingInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			log.Println("Stopping disk flushing worker...")
			return
		case <-ctx.Done():
			log.Println("Stopping disk flushing worker...")
			return
		case <-ticker.C:
			err := kvStore.ToDisk()
			if err != nil {
				log.Printf("\x1b[1;31mERROR\x1b[1;m37%sError while flushing to disk: \n", err.Error())
			}
		}
	}
}

func CleanupWorker(kvStore *core.KVStore, cleanupInterval int, done <-chan os.Signal, ctx context.Context) {
	ticker := time.NewTicker(time.Duration(cleanupInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			log.Println("Stopping cleanup worker...")
			return
		case <-ctx.Done():
			log.Println("Stopping cleanup worker...")
			return
		case <-ticker.C:
			kvStore.Cleanup()
		}
	}
}
