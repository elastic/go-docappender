// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package docappender

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-docappender/v2/docappendertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkIndexerPoolConcurrent(t *testing.T) {
	// This test ensures that the pool can be used concurrently
	// without blocking on Get() under load.
	// Additionally, it ensures that the pool can continue servicing
	// up to the minimum number of indexers per ID even if the maximum
	// number of "leased" indexers is reached.
	// This ensures Quality of Service for the minimum number of indexers
	// per ID.
	type testCfg struct {
		latency time.Duration // Latency between each get
		draw    int           // The number of pool.Get() calls.
	}
	var mu sync.Mutex
	uniques := map[string]struct{}{}
	cfg := BulkIndexerConfig{
		Client: docappendertest.NewMockElasticsearchClient(t, func(http.ResponseWriter, *http.Request) {
			mu.Lock()
			uniques["invalid"] = struct{}{}
			mu.Unlock()
		}),
	}
	require.NoError(t, cfg.Validate())
	test := func(t testing.TB, guaranteed, localMax, total int) {
		pool := NewBulkIndexerPool(guaranteed, localMax, total, cfg)
		parameters := map[string]testCfg{
			// Fast indexer up to the maximum (total) number of indexers
			// allowed in the pool.
			"fast": {latency: time.Nanosecond, draw: localMax},
			// Slow will pull up to the minimum number of indexers.
			"slow": {latency: time.Millisecond / 2, draw: guaranteed},
			// Slower will pull up to the minimum number of indexers.
			"slower": {latency: 5 * time.Millisecond, draw: guaranteed},
		}
		var wg sync.WaitGroup
		for id, p := range parameters {
			wg.Add(1)
			go func(id string, p testCfg) {
				defer wg.Done()
				pool.Register(id)
				for i := 0; i < p.draw; i++ {
					t.Log("start", id, pool.count(id))
					indexer, _ := pool.Get(context.Background(), id)
					indexer.SetClient(docappendertest.NewMockElasticsearchClient(t, func(http.ResponseWriter, *http.Request) {
						mu.Lock()
						defer mu.Unlock()
						uniques[fmt.Sprint(id)] = struct{}{}
					}))
					require.NotNil(t, indexer)
					indexer.Add(BulkIndexerItem{
						Index: "test",
						Body:  strings.NewReader(`{"foo":"bar"}`),
					})
					indexer.Flush(context.Background())
					time.Sleep(p.latency)
					defer func(indexer *BulkIndexer) {
						pool.Put(id, indexer)
						t.Log("put", id, pool.count(id))
					}(indexer)
				}
				t.Log("end", id, pool.count(id))
			}(id, p)
		}
		wg.Wait()
		// Now we deregister all IDs and ensure that the pool is empty.
		for id := range parameters {
			c := pool.Deregister(id)
			require.Equal(t, 0, len(c), id)
			assert.Nil(t, <-c) // Assert nil (closed).
		}
		// Ensure that the indexer client is always reset.
		assert.NotContains(t, uniques, "invalid")
		for id := range parameters {
			assert.Contains(t, uniques, id)
		}
	}

	tc := []struct {
		guaranteed, localMax, total int
	}{
		{1, 2, 2},
		{1, 2, 10},
		{1, 10, 10},
		{1, 10, 100},
		{2, 10, 100},
		{10, 20, 200},
		{100, 200, 500},
	}
	for _, testCase := range tc {
		t.Run("pool "+strings.Join(strings.Fields(fmt.Sprint(testCase)), "_"), func(t *testing.T) {
			test(t, testCase.guaranteed, testCase.localMax, testCase.total)
		})
	}
}

func TestBulkIndexerPool(t *testing.T) {
	cfg := BulkIndexerConfig{
		Client: docappendertest.NewMockElasticsearchClient(t, func(http.ResponseWriter, *http.Request) {}),
	}
	require.NoError(t, cfg.Validate())
	t.Run("NonEmptyIndexer", func(t *testing.T) {
		// This next test ensures that if one of the returned indexers is returned
		// to the pool, it is returned before a new indexer is created / returned.
		guaranteed := 1
		localMax := 2
		total := 2
		pool := NewBulkIndexerPool(guaranteed, localMax, total, cfg)
		key := "slow"
		pool.Register(key)
		bi, _ := pool.Get(context.Background(), key)
		bi.Add(BulkIndexerItem{
			Index: "test",
			Body:  strings.NewReader(`{"foo":"bar"}`),
		})
		prevBI := bi
		pool.Put(key, bi)                           // Return to pool.
		bi, _ = pool.Get(context.Background(), key) // Get the same indexer.
		assert.Equal(t, 1, bi.Items())              // Assert it still has the item.
		assert.Equal(t, prevBI, bi)                 // Assert its the exact same indexer.
		pool.Put(key, bi)                           // Return to pool.

		biC := pool.Deregister(key)
		assert.NotNil(t, biC)
		assert.Len(t, biC, 1)
		nonEmpty := <-biC
		assert.Equal(t, nonEmpty.Items(), 1)
	})
	t.Run("LocalMax", func(t *testing.T) {
		guaranteed := 5
		localMax := 10
		total := 20
		pool := NewBulkIndexerPool(guaranteed, localMax, total, cfg)

		// Test that the local max is respected
		maxOutKey := "test"
		pool.Register(maxOutKey)
		indexers := make(chan *BulkIndexer, localMax)
		for i := 0; i < localMax; i++ {
			indexer, err := pool.Get(context.Background(), maxOutKey)
			require.NoError(t, err)
			require.NotNil(t, indexer)
			indexers <- indexer
		}

		// Ensure that the local max is respected.
		assert.Equal(t, int64(localMax), pool.count(maxOutKey))

		// Ensure other IDs can draw from the pool
		pool.Register("another_test")
		for i := 0; i < localMax; i++ {
			indexer, err := pool.Get(context.Background(), "another_test")
			require.NoError(t, err)
			require.NotNil(t, indexer)
		}

		assert.Equal(t, int64(localMax), pool.count("another_test"))

		done := make(chan struct{})
		started := make(chan struct{})
		go func() {
			defer close(done)
			var once sync.Once
			// Try to draw more indexers with another ID. Ensure
			// that the pool is empty and we block until one is returned.
			for i := 0; i < localMax; i++ {
				once.Do(func() { close(started) })
				indexer, err := pool.Get(context.Background(), maxOutKey)
				require.NoError(t, err)
				require.NotNil(t, indexer)
			}
		}()
		select {
		case <-started:
			// Wait a bit to ensure the goroutine has called Get(). We can't close
			// the channel after pool.Get() because it will block until the indexer
			// is returned.
			time.Sleep(20 * time.Millisecond)
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for goroutine to start")
		}
		// Ensure "test" key is maxed out.
		assert.Equal(t, int64(localMax), pool.count(maxOutKey))
		assert.Equal(t, int64(total), pool.leased.Load())
		// Now put the indexers back into the pool using the same key.
		for i := 0; i < localMax; i++ {
			pool.Put(maxOutKey, <-indexers)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for goroutine to finish")
		}
		assert.Equal(t, int64(localMax), pool.count(maxOutKey))
		assert.Equal(t, int64(total), pool.leased.Load())
	})
}

func TestBulkIndexerPoolWaitUntilFreed(t *testing.T) {
	guaranteed, max := 1, 2
	cfg := BulkIndexerConfig{
		Client: docappendertest.NewMockElasticsearchClient(t, func(http.ResponseWriter, *http.Request) {}),
	}
	require.NoError(t, cfg.Validate())

	pool := NewBulkIndexerPool(guaranteed, max, max, cfg)
	key := "test"
	pool.Register(key)
	c := make(chan *BulkIndexer)
	go func() {
		for idx := range c {
			pool.Put(key, idx)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < max*2; i++ {
		wg.Add(1)
		indexer, err := pool.Get(context.Background(), key)
		require.NoError(t, err)
		require.NotNil(t, indexer)
		go func(idx *BulkIndexer) {
			time.AfterFunc(10*time.Millisecond, func() {
				c <- idx
				wg.Done()
			})
		}(indexer)
	}
	wg.Wait()
	close(c)
	pool.Deregister(key)
}

func TestBulkIndexerPoolFailure(t *testing.T) {
	cfg := BulkIndexerConfig{
		Client: docappendertest.NewMockElasticsearchClient(t, func(http.ResponseWriter, *http.Request) {}),
	}
	t.Run("put empty id", func(t *testing.T) {
		require.NoError(t, cfg.Validate())
		pool := NewBulkIndexerPool(1, 2, 2, cfg)
		assert.NotPanics(t, func() {
			indexer, err := pool.Get(context.Background(), "test")
			assert.Error(t, err)
			pool.Put("", indexer)
		})
	})
	t.Run("put nil indexer", func(t *testing.T) {
		require.NoError(t, cfg.Validate())
		pool := NewBulkIndexerPool(1, 2, 2, cfg)
		assert.NotPanics(t, func() {
			pool.Put("test", nil)
		})
	})
}
