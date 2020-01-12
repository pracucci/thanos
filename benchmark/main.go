package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/cacheutil"
)

type result struct {
	getMultiDuration time.Duration
}

func runClient(memcache cacheutil.MemcachedClient, requests, keysPerRequest, minDataSize, maxDataSize int, results chan result) {
	ctx := context.Background()

	for i := 0; i < requests; i++ {
		// Build the keys
		keys := make([]string, 0, keysPerRequest)
		for k := 0; k < keysPerRequest; k++ {
			keys = append(keys, fmt.Sprintf("key-%d-%d", i, k))
		}

		// Set 1 key
		dataSize := minDataSize + rand.Intn(maxDataSize-minDataSize)
		data := strings.Repeat("x", dataSize)
		memcache.SetAsync(ctx, keys[0], []byte(data), 5*time.Minute)

		// Measure the GetMulti
		start := time.Now()
		memcache.GetMulti(ctx, keys)
		elapsed := time.Now().Sub(start)

		results <- result{
			getMultiDuration: elapsed,
		}
	}
}

func main() {
	address := "10.32.21.9:11211"
	clients := 200
	requests := 10000
	keysPerRequest := 100
	minDataSize := 100
	maxDataSize := 200

	config := cacheutil.MemcachedClientConfig{
		Addresses:                 []string{address},
		Timeout:                   250 * time.Millisecond,
		MaxIdleConnections:        clients * 2,
		MaxAsyncConcurrency:       clients,
		MaxAsyncBufferSize:        clients * requests,
		MaxGetMultiConcurrency:    clients,
		MaxGetMultiBatchSize:      0,
		DNSProviderUpdateInterval: 10 * time.Second,
	}

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	memcache, err := cacheutil.NewMemcachedClientWithConfig(logger, "benchmark", config, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Clients:", clients)
	fmt.Println("Reqs / client:", requests)
	fmt.Println("Keys / req:", keysPerRequest)
	fmt.Println("Data size range:", minDataSize, "-", maxDataSize)
	fmt.Println("Config MaxIdleConnections:", config.MaxIdleConnections)
	fmt.Println("")

	// Run all clients
	results := make(chan result, clients*requests)
	workers := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < clients; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runClient(memcache, requests, keysPerRequest, minDataSize, maxDataSize, results)
		}()
	}

	// Wait until all clients completed
	fmt.Println("All clients started. Now waiting to complete.")
	workers.Wait()
	memcache.Stop()
	elapsed := time.Now().Sub(start)

	// Collect results
	close(results)

	getMultiCount := 0
	getMultiSum := int64(0)
	getMultiMax := int64(0)

	for r := range results {
		getMultiCount++
		getMultiSum += r.getMultiDuration.Nanoseconds()

		if r.getMultiDuration.Nanoseconds() > getMultiMax {
			getMultiMax = r.getMultiDuration.Nanoseconds()
		}
	}

	getMultiAvg := float64(getMultiSum) / float64(getMultiCount)

	fmt.Println("SET ops:", getMultiCount) // We run the same number of set operations
	fmt.Println("GETMULTI avg:", time.Duration(getMultiAvg).Milliseconds(), "ms max:", time.Duration(getMultiMax).Milliseconds(), "ms")
	fmt.Println("GETMULTI ops:", getMultiCount, "ops/s:", float64(getMultiCount)/elapsed.Seconds())
	fmt.Println("Total time:", elapsed)
}
