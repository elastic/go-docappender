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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// BulkIndexerPool is a pool of BulkIndexer instances. It is designed to be
// used in a concurrent environment where multiple goroutines may need to
// acquire and release indexers.
//
// The pool allows a minimum and maximum number of indexers to be "leased"
// for each ID. The overall number of indexers across all IDs is also tracked
// and limited. This is useful for ensuring that the pool does not grow too
// large, even if some IDs are not releasing indexers.
type BulkIndexerPool struct {
	indexers chan *BulkIndexer
	nonEmpty map[string]chan *BulkIndexer
	counts   map[string]*atomic.Int64
	mu       sync.RWMutex
	omax     atomic.Int64

	// Read only fields.
	min, max int64
	overall  int64
	config   BulkIndexerConfig
}

// NewBulkIndexerPool returns a new BulkIndexerPool with the specified minimum
// and maximum number of indexers per ID, and the BulkIndexerConfig to use when
// creating new indexers.
func NewBulkIndexerPool(min, poolMax int, config BulkIndexerConfig) *BulkIndexerPool {
	p := BulkIndexerPool{
		indexers: make(chan *BulkIndexer, poolMax),
		nonEmpty: make(map[string]chan *BulkIndexer),
		counts:   make(map[string]*atomic.Int64),
		min:      int64(min),
		max:      int64(poolMax),
		overall:  int64(poolMax),
		config:   config,
	}
	return &p
}

// Get returns a BulkIndexer for the specified ID. If there aren't any existing
// indexers for the ID, a new one is created.
//
// If the overall limit of indexers has been reached, it will wait until a slot
// is available. This is a blocking operation.
func (p *BulkIndexerPool) Get(id string) *BulkIndexer {
	p.mu.RLock()
	count, exists := p.counts[id]
	p.mu.RUnlock()
	if !exists {
		p.mu.Lock()
		defer p.mu.Unlock()
		count, exists = p.counts[id]
		if !exists {
			p.nonEmpty[id] = make(chan *BulkIndexer, p.overall)
			p.counts[id] = new(atomic.Int64)
			p.counts[id].Add(1)
			p.omax.Add(1)
			return newBulkIndexer(p.config)
		}
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for {
		local := count.Load()
		overall := p.omax.Load()
		// If the local or overall limit has been reached, wait for a slot to
		// become available. This is a simple backoff mechanism to avoid busy
		// waiting.
		if local < p.min || overall < p.overall {
			if swapped := count.CompareAndSwap(local, local+1); swapped {
				p.omax.Add(1)
				// First, try to return an existing non-empty indexer.
				for i := 0; i < 3; i++ {
					select {
					case idx := <-p.nonEmpty[id]:
						return idx
					default:
					}
				}
				// If there aren't any non-empty indexers, either return an
				// existing indexer or create a new one.
				select {
				case idx := <-p.indexers:
					return idx
				default:
					return newBulkIndexer(p.config)
				}
			}
		}
		<-time.After( // Wait until retrying for a slot up to 1ms.
			time.Duration(rand.IntN(1000)) * time.Nanosecond,
		)
	}
}

// Put returns the BulkIndexer to the pool. If the indexer is non-empty, it is
// stored in the non-empty channel for the ID. Otherwise, it is returned to the
// general pool.
func (p *BulkIndexerPool) Put(id string, indexer *BulkIndexer) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count, exists := p.counts[id]
	if !exists {
		return // unknown id, discard indexer
	}
	if indexer.Items() > 0 {
		p.putNonEmpty(id, indexer)
		return
	}
	select {
	case p.indexers <- indexer:
	case <-time.After(time.Nanosecond):
		// If the pool is full, discard the indexer.
	}
	count.Add(-1)
	p.omax.Add(-1)
}

func (p *BulkIndexerPool) putNonEmpty(id string, indexer *BulkIndexer) {
	c, exists := p.nonEmpty[id]
	if !exists {
		return // unknown id, discard indexer
	}
	select {
	case c <- indexer: // Never discard non-empty indexers.
		// Update counts.
		count, _ := p.counts[id]
		count.Add(-1)
		p.omax.Add(-1)
	}
}

// Deregister removes the id from the pool and returns a closed BulkIndexer
// channel with all the non-empty indexers associated with the ID.
func (p *BulkIndexerPool) Deregister(id string) <-chan *BulkIndexer {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.counts, id)
	c, ok := p.nonEmpty[id]
	delete(p.nonEmpty, id)
	if !ok {
		c = make(chan *BulkIndexer)
	}
	close(c)
	return c
}

func (p *BulkIndexerPool) count(id string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count, exists := p.counts[id]
	if !exists {
		return -1
	}
	return count.Load()
}
