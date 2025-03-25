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
	"sync"
	"sync/atomic"
)

// BulkIndexerPool is a pool of BulkIndexer instances. It is designed to be
// used in a concurrent environment where multiple goroutines may need to
// acquire and release indexers.
//
// The pool allows a minimum number of BulkIndexers to be guaranteed per ID, and
// maximum number of indexers to be "leased" overall. This is useful to ensure
// the pool does not grow too large, even if some IDs are not releasing indexers.
//
// The overall number of leased indexers is not guaranteed to configured maximum
// when each of the IDs is below the minimum. This guarantees minimum capacity
// for each ID, potentially leading to a larger number of indexers overall.
type BulkIndexerPool struct {
	indexers chan *BulkIndexer
	entries  map[string]idEntry
	mu       sync.RWMutex
	cond     *sync.Cond   // To wait/signal when a slot is available.
	leased   atomic.Int64 // Total number of leased indexers across all IDs.

	// Read only fields.
	min, max, total int64
	config          BulkIndexerConfig
}

type idEntry struct {
	nonEmpty chan *BulkIndexer
	count    *atomic.Int64
}

// NewBulkIndexerPool returns a new BulkIndexerPool with:
// - The specified guaranteed indexers per ID
// - The maximum number of concurrent BulkIndexers per ID
// - A total (max) number of indexers to be leased per pool.
// - The BulkIndexerConfig to use when creating new indexers.
func NewBulkIndexerPool(guaranteed, max, total int, c BulkIndexerConfig) *BulkIndexerPool {
	p := BulkIndexerPool{
		indexers: make(chan *BulkIndexer, total),
		entries:  make(map[string]idEntry),
		min:      int64(guaranteed),
		max:      int64(max),
		total:    int64(total),
		config:   c,
	}
	p.cond = sync.NewCond(&p.mu)
	return &p
}

// Get returns a BulkIndexer for the specified ID. If there aren't any existing
// indexers for the ID, a new one is created.
//
// If the overall limit of indexers has been reached, it will wait until a slot
// is available. This is a blocking operation.
func (p *BulkIndexerPool) Get(id string) *BulkIndexer {
	// Acquire the lock to ensure that we are not racing with other goroutines
	// that may be trying to acquire or release indexers.
	// Even though this looks like it would prevent other Get() operations from
	// proceeding, the lock is only held for a short time while we check the
	// count and overall limits. The lock is released by:
	// - p.cond.Wait() releases the lock while waiting for a signal.
	// - p.mu.Unlock() releases the lock after the indexer is returned.
	p.mu.Lock()
	entry, exists := p.entries[id]
	if !exists {
		defer p.mu.Unlock()
		entry, exists = p.entries[id]
		if !exists {
			entry = idEntry{
				nonEmpty: make(chan *BulkIndexer, p.max),
				count:    new(atomic.Int64),
			}
			entry.count.Add(1)
			p.entries[id] = entry
			p.leased.Add(1)
			return newBulkIndexer(p.config)
		}
	}

	for {
		// Always allow minimum indexers to be dispensed, regardless of the
		// overall limit. This ensures that the minimum number of indexers
		// are always available for each ID.
		underGuaranteed := entry.count.Load() < p.min
		if underGuaranteed {
			return p.get(entry)
		}
		// Only allow indexers to be dispensed if both the local and overall
		// limits have not been reached.
		underLocalMax := entry.count.Load() < p.max
		underTotal := p.leased.Load() < p.total
		if underTotal && underLocalMax {
			return p.get(entry)
		}
		// Waits until a Put() is called, which will signal this condition.
		// This allows waiting for a slot to become available without busy
		// waiting.
		// When Wait() is called, the mutex is unlocked while waiting.
		// After Wait() returns, the mutex is automatically locked.
		p.cond.Wait()
	}
}

// get returns a BulkIndexer for the specified ID. It is assumed that the
// caller has already acquired the lock and checked the count and overall
// limits. This function is called by Get() when the ID is already registered
// and the overall limits have not been reached.
func (p *BulkIndexerPool) get(entry idEntry) *BulkIndexer {
	entry.count.Add(1)
	p.leased.Add(1)
	// Unlock the mutex right after atomic counts are updated.
	p.mu.Unlock()
	// First, try to return an existing non-empty indexer (if any). Try a few
	// times since the mutex is unlocked and an indexer may have been returned
	// in the meantime.
	for i := 0; i < 3; i++ {
		select {
		case idx := <-entry.nonEmpty:
			return idx
		default:
		}
	}
	// If there aren't any non-empty indexers, either return an
	// existing indexer or create a new one if none are available.
	select {
	case idx := <-p.indexers:
		return idx
	default:
		return newBulkIndexer(p.config)
	}
}

// Put returns the BulkIndexer to the pool. If the indexer is non-empty, it is
// stored in the non-empty channel for the ID. Otherwise, it is returned to the
// general pool.
// After calling Put() no references to the indexer should be stored, since
// doing so may lead to undefined behavior and unintended memory sharing.
func (p *BulkIndexerPool) Put(id string, indexer *BulkIndexer) {
	if indexer == nil {
		return // No indexer to store, nothing to do.
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	entry, exists := p.entries[id]
	if !exists {
		return // unknown id, discard indexer
	}
	defer func() { // Always decrement the count and signal the condition.
		entry.count.Add(-1)
		p.leased.Add(-1)
		p.cond.Signal() // Signal waiting goroutines
	}()
	if indexer.Items() > 0 {
		entry.nonEmpty <- indexer // Never discard non-empty indexers.
		return
	}
	select {
	case p.indexers <- indexer: // Return to the pool for later reuse.
	default:
		indexer = nil // If the pool is full, discard the indexer.
	}
}

// Deregister removes the id from the pool and returns a closed BulkIndexer
// channel with all the non-empty indexers associated with the ID.
func (p *BulkIndexerPool) Deregister(id string) <-chan *BulkIndexer {
	if id == "" {
		return nil // No ID to deregister.
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.entries[id]
	if !ok { // To handle when the ID is not found.
		entry.nonEmpty = make(chan *BulkIndexer)
	}
	delete(p.entries, id)
	close(entry.nonEmpty)
	return entry.nonEmpty
}

func (p *BulkIndexerPool) count(id string) int64 {
	if id == "" {
		return -1
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if entry, exists := p.entries[id]; exists {
		return entry.count.Load()
	}
	return -1
}
