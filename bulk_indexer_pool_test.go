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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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
	cfg := BulkIndexerConfig{}
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
				for i := 0; i < p.draw; i++ {
					t.Log("start", id, pool.count(id))
					indexer := pool.Get(id)
					require.NotNil(t, indexer)
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
			require.Equal(t, len(c), 0, id)
			assert.Nil(t, <-c) // Assert nil (closed).
		}

		// This next test ensures that if one of the returned indexers is returned
		// to the pool, it is returned before a new indexer is created / returned.
		key := "slow"
		bi := pool.Get(key)
		bi.Add(BulkIndexerItem{
			Index: "test",
			Body:  strings.NewReader(`{"foo":"bar"}`),
		})
		prevBI := bi
		pool.Put(key, bi)              // Return to pool.
		bi = pool.Get(key)             // Get the same indexer.
		assert.Equal(t, 1, bi.Items()) // Assert it still has the item.
		assert.Equal(t, prevBI, bi)    // Assert its the exact same indexer.
		pool.Put(key, bi)              // Return to pool.

		biC := pool.Deregister(key)
		assert.NotNil(t, biC)
		assert.Len(t, biC, 1)
		var count int
		for range biC {
			count++
		}
		assert.Equal(t, 1, count)
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

func TestBulkIndexerPoolCorrectness(t *testing.T) {
	cfg := BulkIndexerConfig{}
	require.NoError(t, cfg.Validate())
	guaranteed := 5
	localMax := 10
	total := 20
	pool := NewBulkIndexerPool(guaranteed, localMax, total, cfg)

	// Test that the local max is respected
	maxOutKey := "test"
	indexers := make(chan *BulkIndexer, localMax)
	for i := 0; i < localMax; i++ {
		indexer := pool.Get(maxOutKey)
		require.NotNil(t, indexer)
		indexers <- indexer
	}

	// Ensure that the local max is respected.
	// This should block until the indexer is returned to the pool.
	assert.Equal(t, int64(localMax), pool.count(maxOutKey))

	// Ensure other IDs can draw from the pool
	for i := 0; i < localMax; i++ {
		indexer := pool.Get("another_test")
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
			indexer := pool.Get(maxOutKey)
			require.NotNil(t, indexer)
		}
	}()
	select {
	case <-started:
		time.Sleep(20 * time.Millisecond) // Wait a bit.
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for goroutine to start")
	}
	// Ensure "test" key is maxed out.
	assert.Equal(t, int64(localMax), pool.count(maxOutKey))
	assert.Equal(t, pool.leased.Load(), int64(total))
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
	assert.Equal(t, pool.leased.Load(), int64(total))
}

func TestBulkIndexerPoolWaitUntilFreed(t *testing.T) {
	guaranteed, max := 1, 2
	cfg := BulkIndexerConfig{}
	require.NoError(t, cfg.Validate())

	pool := NewBulkIndexerPool(guaranteed, max, max, cfg)
	key := "test"
	c := make(chan *BulkIndexer)
	go func() {
		for idx := range c {
			pool.Put(key, idx)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < max*2; i++ {
		wg.Add(1)
		indexer := pool.Get(key)
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
}

func TestBulkIndexerPoolFailure(t *testing.T) {
	t.Run("put empty id", func(t *testing.T) {
		cfg := BulkIndexerConfig{}
		require.NoError(t, cfg.Validate())
		pool := NewBulkIndexerPool(1, 2, 2, cfg)
		assert.NotPanics(t, func() {
			pool.Put("", pool.Get("test"))
		})
	})
	t.Run("put nil indexer", func(t *testing.T) {
		cfg := BulkIndexerConfig{}
		require.NoError(t, cfg.Validate())
		pool := NewBulkIndexerPool(1, 2, 2, cfg)
		assert.NotPanics(t, func() {
			pool.Put("test", nil)
		})
	})
}
