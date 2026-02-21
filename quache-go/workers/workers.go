package workers

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/AstraBert/quache/quache-go/core"
)

func ToDiskWorker(kvStore *core.KVStore, flushingInterval int, done <-chan os.Signal, ctx context.Context) {
	for {
		select {
		case <-done:
			log.Println("Stopping disk flushing worker...")
			return
		case <-ctx.Done():
			log.Println("Stopping disk flushing worker...")
			return
		default:
			err := kvStore.ToDisk()
			if err != nil {
				log.Printf("\x1b[1;31mERROR\x1b[1;m37%sError while flushing to disk: \n", err.Error())
			}
			time.Sleep(time.Duration(flushingInterval * 1000))
		}
	}
}

func CleanupWorker(kvStore *core.KVStore, cleanupInterval int, done <-chan os.Signal, ctx context.Context) {
	for {
		select {
		case <-done:
			log.Println("Stopping cleanup worker...")
			return
		case <-ctx.Done():
			log.Println("Stopping cleanup worker...")
			return
		default:
			kvStore.Cleanup()
			time.Sleep(time.Duration(cleanupInterval * 1000))
		}
	}
}
