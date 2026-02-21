package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AstraBert/quache/quache-go/core"
	"github.com/AstraBert/quache/quache-go/server"
	"github.com/AstraBert/quache/quache-go/workers"
	"github.com/spf13/cobra"
)

const DefaultPort int = 8000
const DefaultHost string = "0.0.0.0"
const DefaultFlushingInterval int = 1000
const DefaultCleanupInterval int = 500
const DefaultDirectory string = ".quache/"
const DefaultShardsNumber int = 5

var port int
var host string
var flushingInterval int
var cleanupInterval int
var directory string
var shardsNumber int
var showHelp bool
var load bool

var rootCmd = &cobra.Command{
	Use:   "quache",
	Short: "quache is an in-memory KV store working as a server",
	Long:  "quache is a single-node in-memory KV store that can be served as an API server",
	Run: func(cmd *cobra.Command, args []string) {
		if showHelp {
			_ = cmd.Help()
			return
		}
		_, err := os.Stat(directory)
		if errors.Is(err, os.ErrNotExist) {
			if load {
				fmt.Println("Cannot load the KV store from the specified directory because it does not exist")
				os.Exit(1)
			}
			os.Mkdir(directory, 0775)
		}
		var kvStore *core.KVStore
		if load {
			fmt.Println("Loading KV store from disk...")
			kvStore, err = core.NewKVStoreFromDisk(shardsNumber, directory)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		} else {
			kvStore = core.NewKVStore(shardsNumber, directory)
		}
		handler := server.CreateServerMux(kvStore)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan os.Signal, 1)
		signal.Notify(done, os.Interrupt, syscall.SIGINT)

		httpServer := &http.Server{
			Addr:    fmt.Sprintf("%s:%d", host, port),
			Handler: handler,
		}

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			workers.ToDiskWorker(kvStore, flushingInterval, done, ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			workers.CleanupWorker(kvStore, cleanupInterval, done, ctx)
		}()

		// Start server in a goroutine
		go func() {
			fmt.Println("starting server on :8000")
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("Server error: %s\n", err)
			}
		}()

		<-done
		fmt.Println("Shutting down server and workers...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("Server shutdown error: %s\n", err)
		}
		cancel()
		wg.Wait()
		fmt.Println("Application stopped")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Oops. An error while executing bsgit '%s'\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().BoolVarP(&showHelp, "help", "h", false, "Show the help message and exit.")
	rootCmd.Flags().StringVarP(&directory, "directory", "d", DefaultDirectory, "Directory which to flush the KV store data to. Defaults to .quache/")
	rootCmd.Flags().IntVarP(&shardsNumber, "shards", "s", DefaultShardsNumber, "Number of shards to use to vertically shard the KV store. Defaults to 5.")
	rootCmd.Flags().BoolVarP(&load, "load", "l", false, "Load the KV store from disk. Does not load from disk by default")
	rootCmd.Flags().IntVarP(&flushingInterval, "flush-interval", "f", DefaultFlushingInterval, "Flushing interval (in ms). Defaults to 1000ms")
	rootCmd.Flags().IntVarP(&cleanupInterval, "cleanup-interval", "c", DefaultCleanupInterval, "Cleanup (of expired entries) interval (in ms). Defaults to 5ß0ms")
	rootCmd.Flags().StringVarP(&host, "bind", "b", DefaultHost, "Host to bind the server to. Defaults to 0.0.0.0")
	rootCmd.Flags().IntVarP(&port, "port", "p", DefaultPort, "Port to which to bind the server to. Defaults to 8000")
}
