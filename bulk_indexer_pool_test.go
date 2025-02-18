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
	min, max := 5, 20
	cfg := BulkIndexerConfig{}
	require.NoError(t, cfg.Validate())
	pool := NewBulkIndexerPool(min, max, cfg)
	parameters := map[string]testCfg{
		// Fast indexer up to the maximum (total) number of indexers
		// allowed in the pool.
		"fast": {latency: time.Nanosecond, draw: max},
		// Slow will pull up to the minimum number of indexers.
		"slow": {latency: time.Millisecond, draw: min},
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

func TestBulkIndexerPoolWaitUntilFreed(t *testing.T) {
	min, max := 1, 2
	cfg := BulkIndexerConfig{}
	require.NoError(t, cfg.Validate())

	pool := NewBulkIndexerPool(min, max, cfg)
	key := "test"
	c := make(chan *BulkIndexer)
	go func() {
		for idx := range c {
			pool.Put(key, idx)
		}
	}()

	for i := 0; i < max*2; i++ {
		indexer := pool.Get(key)
		require.NotNil(t, indexer)
		go func(idx *BulkIndexer) {
			time.AfterFunc(10*time.Millisecond, func() {
				c <- idx
			})
		}(indexer)
	}

}
