# Benchmarks

## Run

Use the `send_requests.sh` script and specify a number of requests to send (default is 1000). The script runs [`main.go`](./main.go) which spawns the requested amount of goroutines (with a maximum of 1000 of them running concurrently): each of the goroutines sends a POST request to the KV store server, using a `key-rand(1,100)` as key and `rand(1,100)` as value, and a TTL between 1 and 2 seconds.

Example usage:

```bash
bash send_requests.sh --num-requests 100000
```

The script will output an overview of the server performance, including the successful and failed requests, as well as the average response time, the requests per second and the total duration of the test.

It is important to notice that, as the number of requests increases, the number of potential key substituions increases: it is more likely that two random keys might be the same with 1.000.000 requests than with 1.000, and thus key substitution is more likely to happen instead of key insertion.

## Results

### Go

**1.000 requests**

Total requests: 1000
Successful requests: 1000
Failed requests: 0
Success rate: 100.00%
Average response time: 28.29 ms
Total test duration: 43.260208ms
Requests per second: 23115.93

**10.000 requests**

Total requests: 10000
Successful requests: 10000
Failed requests: 0
Success rate: 100.00%
Average response time: 14.68 ms
Total test duration: 161.525709ms
Requests per second: 61909.65

**100.000 requests**

Total requests: 100000
Successful requests: 100000
Failed requests: 0
Success rate: 100.00%
Average response time: 8.95 ms
Total test duration: 960.156541ms
Requests per second: 104149.68

**1.000.000 requests**

Total requests: 1000000
Successful requests: 1000000
Failed requests: 0
Success rate: 100.00%
Average response time: 8.31 ms
Total test duration: 8.886698708s
Requests per second: 112527.73

### Rust

> The server produced an error: "An error occurred while flushing to disk: Too many open files (os error 24)", this presumably hints at an implementation error on my side.

**1.000 requests**

Total requests: 1000
Successful requests: 930
Failed requests: 70
Success rate: 93.00%
Average response time: 26.84 ms
Total test duration: 10.008902167s
Requests per second: 99.91


**10.000 requests**

Total requests: 10000
Successful requests: 9871
Failed requests: 129
Success rate: 98.71%
Average response time: 12.72 ms
Total test duration: 10.124296625s
Requests per second: 987.72

**100.000 requests**

Total requests: 100000
Successful requests: 99871
Failed requests: 129
Success rate: 99.87%
Average response time: 7.56 ms
Total test duration: 10.0406955s
Requests per second: 9959.47

**1.000.000 requests**

Total requests: 1000000
Successful requests: 999863
Failed requests: 137
Success rate: 99.99%
Average response time: 6.85 ms
Total test duration: 18.033700917s
Requests per second: 55451.73
