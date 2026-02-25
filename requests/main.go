package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type SetRequest struct {
	Key   string   `json:"key"`
	Value any      `json:"value"`
	Ttl   *float64 `json:"ttl"`
}

func sendPostRequest(client *http.Client, successTime *atomic.Int64, failed *atomic.Int32, success *atomic.Int32) {
	key := fmt.Sprintf("key-%d", rand.Intn(100))
	value := rand.Intn(100)
	ttl := rand.Float64()

	requestJSON := SetRequest{Key: key, Value: value, Ttl: &ttl}
	requestBody, err := json.Marshal(requestJSON)
	if err != nil {
		failed.Add(1)
		return
	}

	request, err := http.NewRequest("POST", "http://0.0.0.0:8000/kv", bytes.NewReader(requestBody))
	if err != nil {
		failed.Add(1)
		return
	}
	request.Header.Set("Content-Type", "application/json")

	start := time.Now()
	response, err := client.Do(request)
	if err != nil {
		failed.Add(1)
		return
	}
	defer response.Body.Close()

	elapsed := time.Since(start).Milliseconds()

	if response.StatusCode == 201 {
		successTime.Add(elapsed)
		success.Add(1)
		return
	}

	failed.Add(1)
}

func main() {
	args := os.Args
	if len(args) != 2 {
		log.Fatalf("Expecting exactly one argument from command line")
	}

	howMany, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatalf("Expecting the second argument to represent how many requests have to be sent")
	}

	maxConcurrent := 1000
	semaphore := make(chan struct{}, maxConcurrent)

	transport := &http.Transport{
		MaxIdleConns:        maxConcurrent,
		MaxIdleConnsPerHost: maxConcurrent,
		MaxConnsPerHost:     maxConcurrent,
		IdleConnTimeout:     90 * time.Second,
	}

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	var wg sync.WaitGroup
	var successTime atomic.Int64
	var failed atomic.Int32
	var success atomic.Int32

	startTime := time.Now()

	for range howMany {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore

		go func() {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore
			sendPostRequest(client, &successTime, &failed, &success)
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	successCount := int(success.Load())
	failedCount := int(failed.Load())
	totalTime := successTime.Load()

	var avgTime float64
	if successCount > 0 {
		avgTime = float64(totalTime) / float64(successCount)
	}

	fmt.Printf("Total requests: %d\n", howMany)
	fmt.Printf("Successful requests: %d\n", successCount)
	fmt.Printf("Failed requests: %d\n", failedCount)
	fmt.Printf("Success rate: %.2f%%\n", float64(successCount)/float64(howMany)*100)
	fmt.Printf("Average response time: %.2f ms\n", avgTime)
	fmt.Printf("Total test duration: %v\n", totalDuration)
	fmt.Printf("Requests per second: %.2f\n", float64(howMany)/totalDuration.Seconds())
}
