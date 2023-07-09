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
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"go.elastic.co/apm/module/apmzap/v2"
	"go.elastic.co/apm/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrClosed is returned from methods of closed Indexers.
	ErrClosed = errors.New("model indexer closed")

	errMissingIndex = errors.New("missing index name")
	errMissingBody  = errors.New("missing document body")
)

// Appender provides an append-only API for bulk indexing documents into Elasticsearch.
//
// Appender buffers documents in their JSON encoding until either the accumulated buffer
// reaches `config.FlushBytes`, or `config.FlushInterval` elapses.
//
// Appender fills a single bulk request buffer at a time to ensure bulk requests are optimally
// sized, avoiding sparse bulk requests as much as possible. After a bulk request is flushed,
// the next document added will wait for the next available bulk request buffer and repeat the
// process.
//
// Up to `config.MaxRequests` bulk requests may be flushing/active concurrently, to allow the
// server to make progress encoding while Elasticsearch is busy servicing flushed bulk requests.
type Appender struct {
	bulkRequests          int64
	docsAdded             int64
	docsActive            int64
	docsFailed            int64
	docsFailedClient      int64
	docsFailedServer      int64
	docsIndexed           int64
	tooManyRequests       int64
	bytesTotal            int64
	availableBulkRequests int64
	activeCreated         int64
	activeDestroyed       int64

	scalingInfo atomic.Value

	config                Config
	available             chan *bulkIndexer
	bulkItems             chan bulkIndexerItem
	errgroup              errgroup.Group
	errgroupContext       context.Context
	cancelErrgroupContext context.CancelFunc

	mu     sync.Mutex
	closed chan struct{}
}

// New returns a new Appender that indexes documents into Elasticsearch.
func New(client *elasticsearch.Client, cfg Config) (*Appender, error) {
	if cfg.CompressionLevel < -1 || cfg.CompressionLevel > 9 {
		return nil, fmt.Errorf(
			"expected CompressionLevel in range [-1,9], got %d",
			cfg.CompressionLevel,
		)
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = 10
	}
	if cfg.FlushBytes <= 0 {
		cfg.FlushBytes = 1 * 1024 * 1024
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 30 * time.Second
	}
	if cfg.DocumentBufferSize <= 0 {
		cfg.DocumentBufferSize = 1024
	}
	if !cfg.Scaling.Disabled {
		if cfg.Scaling.ScaleDown.Threshold == 0 {
			cfg.Scaling.ScaleDown.Threshold = 30
		}
		if cfg.Scaling.ScaleDown.CoolDown <= 0 {
			cfg.Scaling.ScaleDown.CoolDown = 30 * time.Second
		}
		if cfg.Scaling.ScaleUp.Threshold == 0 {
			cfg.Scaling.ScaleUp.Threshold = 60
		}
		if cfg.Scaling.ScaleUp.CoolDown <= 0 {
			cfg.Scaling.ScaleUp.CoolDown = time.Minute
		}
		if cfg.Scaling.IdleInterval <= 0 {
			cfg.Scaling.IdleInterval = 30 * time.Second
		}
	}
	available := make(chan *bulkIndexer, cfg.MaxRequests)
	for i := 0; i < cfg.MaxRequests; i++ {
		available <- newBulkIndexer(client, cfg.CompressionLevel)
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	indexer := &Appender{
		availableBulkRequests: int64(len(available)),
		config:                cfg,
		available:             available,
		closed:                make(chan struct{}),
		bulkItems:             make(chan bulkIndexerItem, cfg.DocumentBufferSize),
	}

	// We create a cancellable context for the errgroup.Group for unblocking
	// flushes when Close returns. We intentionally do not use errgroup.WithContext,
	// because one flush failure should not cause the context to be cancelled.
	indexer.errgroupContext, indexer.cancelErrgroupContext = context.WithCancel(
		context.Background(),
	)
	indexer.scalingInfo.Store(scalingInfo{activeIndexers: 1})
	indexer.errgroup.Go(func() error {
		indexer.runActiveIndexer()
		return nil
	})
	return indexer, nil
}

// Close closes the indexer, first flushing any queued items.
//
// Close returns an error if any flush attempts during the indexer's
// lifetime returned an error. If ctx is cancelled, Close returns and
// any ongoing flush attempts are cancelled.
func (a *Appender) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	select {
	case <-a.closed:
	default:
		close(a.closed)

		// Cancel ongoing flushes when ctx is cancelled.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			defer a.cancelErrgroupContext()
			<-ctx.Done()
		}()
	}
	return a.errgroup.Wait()
}

// Stats returns the bulk indexing stats.
func (a *Appender) Stats() Stats {
	return Stats{
		Added:                 atomic.LoadInt64(&a.docsAdded),
		Active:                atomic.LoadInt64(&a.docsActive),
		BulkRequests:          atomic.LoadInt64(&a.bulkRequests),
		Failed:                atomic.LoadInt64(&a.docsFailed),
		FailedClient:          atomic.LoadInt64(&a.docsFailedClient),
		FailedServer:          atomic.LoadInt64(&a.docsFailedServer),
		Indexed:               atomic.LoadInt64(&a.docsIndexed),
		TooManyRequests:       atomic.LoadInt64(&a.tooManyRequests),
		BytesTotal:            atomic.LoadInt64(&a.bytesTotal),
		AvailableBulkRequests: atomic.LoadInt64(&a.availableBulkRequests),
		IndexersActive:        a.scalingInformation().activeIndexers,
		IndexersCreated:       atomic.LoadInt64(&a.activeCreated),
		IndexersDestroyed:     atomic.LoadInt64(&a.activeDestroyed),
	}
}

// Add enqueues document for appending to index.
//
// The document body will be copied to a buffer using io.Copy, and document may
// implement io.WriterTo to reduce overhead of copying.
//
// The document io.Reader will be accessed after Add returns, and must remain
// accessible until its Read method returns EOF, or its WriterTo method returns.
func (a *Appender) Add(ctx context.Context, index string, document io.Reader) error {
	if index == "" {
		return errMissingIndex
	}
	if document == nil {
		return errMissingBody
	}

	// Send the bulkIndexerItem to the internal channel, allowing individual
	// documents to be processed by an active bulk indexer in a dedicated
	// goroutine, improving data locality and minimising lock contention.
	item := bulkIndexerItem{
		Index: index,
		Body:  document,

		// Appender is append-only, hence the action is always "create".
		Action: "create",
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closed:
		return ErrClosed
	case a.bulkItems <- item:
	}
	atomic.AddInt64(&a.docsAdded, 1)
	atomic.AddInt64(&a.docsActive, 1)
	return nil
}

func (a *Appender) flush(ctx context.Context, bulkIndexer *bulkIndexer) error {
	n := bulkIndexer.Items()
	if n == 0 {
		return nil
	}
	defer atomic.AddInt64(&a.docsActive, -int64(n))
	defer atomic.AddInt64(&a.bulkRequests, 1)

	logger := a.config.Logger
	if a.tracingEnabled() {
		tx := a.config.Tracer.StartTransaction("docappender.flush", "output")
		defer tx.End()
		ctx = apm.ContextWithTransaction(ctx, tx)

		// Add trace IDs to logger, to associate any per-item errors
		// below with the trace.
		logger = logger.With(apmzap.TraceContext(ctx)...)
	}

	resp, err := bulkIndexer.Flush(ctx)
	// Record the bulkIndexer buffer's length as the bytesTotal metric after
	// the request has been flushed.
	if flushed := bulkIndexer.BytesFlushed(); flushed > 0 {
		atomic.AddInt64(&a.bytesTotal, int64(flushed))
	}
	if err != nil {
		atomic.AddInt64(&a.docsFailed, int64(n))
		logger.Error("bulk indexing request failed", zap.Error(err))
		if a.tracingEnabled() {
			apm.CaptureError(ctx, err).Send()
		}

		var errTooMany errorTooManyRequests
		// 429 may be returned as errors from the bulk indexer.
		if errors.As(err, &errTooMany) {
			atomic.AddInt64(&a.tooManyRequests, int64(n))
		}
		return err
	}
	var docsFailed, docsIndexed, tooManyRequests, clientFailed, serverFailed int64
	for _, info := range resp.Items {
		if info.Error.Type != "" || info.Status > 201 {
			docsFailed++
			if info.Status >= 400 && info.Status < 500 {
				if info.Status == http.StatusTooManyRequests {
					tooManyRequests++
				} else {
					clientFailed++
				}
			}
			if info.Status >= 500 {
				serverFailed++
			}
			// NOTE(axw) error type and reason are included
			// in the error message so we can observe different
			// error types/reasons when logging is rate limited.
			logger.Error(fmt.Sprintf(
				"failed to index document in '%s' (%s): %s",
				info.Index, info.Error.Type, info.Error.Reason,
			))

			if a.tracingEnabled() {
				apm.CaptureError(ctx, errors.New(info.Error.Reason)).Send()
			}
		} else {
			docsIndexed++
		}
	}
	if docsFailed > 0 {
		atomic.AddInt64(&a.docsFailed, docsFailed)
	}
	if docsIndexed > 0 {
		atomic.AddInt64(&a.docsIndexed, docsIndexed)
	}
	if tooManyRequests > 0 {
		atomic.AddInt64(&a.tooManyRequests, tooManyRequests)
	}
	if clientFailed > 0 {
		atomic.AddInt64(&a.docsFailedClient, clientFailed)
	}
	if serverFailed > 0 {
		atomic.AddInt64(&a.docsFailedServer, serverFailed)
	}
	logger.Debug(
		"bulk request completed",
		zap.Int64("docs_indexed", docsIndexed),
		zap.Int64("docs_failed", docsFailed),
		zap.Int64("docs_rate_limited", tooManyRequests),
	)
	return nil
}

// runActiveIndexer starts a new active indexer which pulls items from the
// bulkItems channel. The more active indexers there are, the faster items
// will be pulled out of the queue, but also the more likely it is that the
// outgoing Elasticsearch bulk requests are flushed due to the idle timer,
// rather than due to being full.
func (a *Appender) runActiveIndexer() {
	var closed bool
	var active *bulkIndexer
	var timedFlush uint
	var fullFlush uint
	flushTimer := time.NewTimer(a.config.FlushInterval)
	if !flushTimer.Stop() {
		<-flushTimer.C
	}
	handleBulkItem := func(item bulkIndexerItem) {
		if active == nil {
			active = <-a.available
			atomic.AddInt64(&a.availableBulkRequests, -1)
			flushTimer.Reset(a.config.FlushInterval)
		}
		if err := active.add(item); err != nil {
			a.config.Logger.Error("failed to add item to bulk indexer", zap.Error(err))
		}
	}
	for !closed {
		select {
		case <-flushTimer.C:
			timedFlush++
			fullFlush = 0
		default:
			// When there's no active indexer and queue utilization is below 5%,
			// reset the flushTimer with IdleInterval so excess active indexers
			// that remain idle can be scaled down.
			if !a.config.Scaling.Disabled && active == nil {
				if a.scalingInformation().activeIndexers > 1 &&
					float64(len(a.bulkItems))/float64(cap(a.bulkItems)) <= 0.05 {
					flushTimer.Reset(a.config.Scaling.IdleInterval)
				}
			}
			select {
			case <-a.closed:
				// Consume whatever bulk items have been buffered,
				// and then flush a last time below.
				for len(a.bulkItems) > 0 {
					select {
					case item := <-a.bulkItems:
						handleBulkItem(item)
					default:
						// Another goroutine took the item.
					}
				}
				closed = true
			case <-flushTimer.C:
				timedFlush++
				fullFlush = 0
			case item := <-a.bulkItems:
				handleBulkItem(item)
				if active.Len() < a.config.FlushBytes {
					continue
				}
				fullFlush++
				timedFlush = 0
				// The active indexer is at or exceeds the configured FlushBytes
				// threshold, so flush it.
				if !flushTimer.Stop() {
					<-flushTimer.C
				}
			}
		}
		if active != nil {
			indexer := active
			active = nil
			a.errgroup.Go(func() error {
				err := a.flush(a.errgroupContext, indexer)
				indexer.Reset()
				a.available <- indexer
				atomic.AddInt64(&a.availableBulkRequests, 1)
				return err
			})
		}
		if a.config.Scaling.Disabled {
			continue
		}
		now := time.Now()
		info := a.scalingInformation()
		if a.maybeScaleDown(now, info, &timedFlush) {
			atomic.AddInt64(&a.activeDestroyed, 1)
			return
		}
		if a.maybeScaleUp(now, info, &fullFlush) {
			atomic.AddInt64(&a.activeCreated, 1)
			a.errgroup.Go(func() error {
				a.runActiveIndexer()
				return nil
			})
		}
	}
	// Decrement the active bulk requests when Appender is closed.
	for {
		info := a.scalingInformation()
		if a.scalingInfo.CompareAndSwap(info, scalingInfo{
			lastAction:     time.Now(),
			activeIndexers: info.activeIndexers - 1,
		}) {
			return
		}
	}
}

// maybeScaleDown returns true if the caller (assumed to be active indexer) needs
// to be scaled down. It automatically updates the scaling information with a
// decremented `activeBulkRequests` and timestamp of the action when true.
func (a *Appender) maybeScaleDown(now time.Time, info scalingInfo, timedFlush *uint) bool {
	// Only downscale when there is more than 1 active indexer.
	if info.activeIndexers == 1 {
		return false
	}
	// If the CPU quota changes and there is more than 1 indexer, downscale an
	// active indexer. This downscaling action isn't subject to the downscaling
	// cooldown, since doing so would result in using much more CPU for longer.
	// Loop until the CompareAndSwap operation succeeds (there may be more than)
	// since a single active indexer trying to down scale itself, or the active
	// indexer variable is in check.
	limit := activeLimit()
	for info.activeIndexers > limit {
		// Avoid having more than 1 concurrent downscale, by using a compare
		// and swap operation.
		if newInfo := info.ScaleDown(now); a.scalingInfo.CompareAndSwap(info, newInfo) {
			a.config.Logger.Info(
				"active indexers exceeds limit, scaling down",
				zap.Int64("old_active_indexer_count", info.activeIndexers),
				zap.Int64("new_active_indexer_count", newInfo.activeIndexers),
				zap.Int64("active_indexer_limit", limit),
			)
			return true
		}
		info = a.scalingInformation() // refresh scaling info if CAS failed.
	}
	if info.withinCoolDown(a.config.Scaling.ScaleDown.CoolDown, now) {
		return false
	}
	// If more than 1% of the requests result in 429, scale down the current
	// active indexer.
	if a.indexFailureRate() >= 0.01 {
		if newInfo := info.ScaleDown(now); a.scalingInfo.CompareAndSwap(info, newInfo) {
			a.config.Logger.Info(
				"elasticsearch 429 response rate exceeded 1%, scaling down",
				zap.Int64("old_active_indexer_count", info.activeIndexers),
				zap.Int64("new_active_indexer_count", newInfo.activeIndexers),
			)
			return true
		}
		return false
	}
	if *timedFlush < a.config.Scaling.ScaleDown.Threshold {
		return false
	}
	// Reset timedFlush after it has exceeded the threshold
	// it avoids unnecessary precociousness to scale down.
	*timedFlush = 0
	if newInfo := info.ScaleDown(now); a.scalingInfo.CompareAndSwap(info, newInfo) {
		a.config.Logger.Info(
			"timed flush threshold exceeded, scaling down",
			zap.Int64("old_active_indexer_count", info.activeIndexers),
			zap.Int64("new_active_indexer_count", newInfo.activeIndexers),
		)
		return true
	}
	return false
}

// maybeScaleUp returns true if the caller (assumed to be active indexer) needs
// to scale up and create another active indexer goroutine. It automatically
// updates the scaling information with an incremented `activeBulkRequests` and
// timestamp of the action when true.
func (a *Appender) maybeScaleUp(now time.Time, info scalingInfo, fullFlush *uint) bool {
	if *fullFlush < a.config.Scaling.ScaleUp.Threshold {
		return false
	}
	if info.activeIndexers >= activeLimit() {
		return false
	}
	// Reset fullFlush after it has exceeded the threshold
	// it avoids unnecessary precociousness to scale up.
	*fullFlush = 0
	// If more than 1% of the requests result in 429, do not scale up.
	if a.indexFailureRate() >= 0.01 {
		return false
	}
	if info.withinCoolDown(a.config.Scaling.ScaleUp.CoolDown, now) {
		return false
	}
	// Avoid having more than 1 concurrent upscale, by using a compare
	// and swap operation.
	if newInfo := info.ScaleUp(now); a.scalingInfo.CompareAndSwap(info, newInfo) {
		a.config.Logger.Info(
			"full flush threshold exceeded, scaling up",
			zap.Int64("old_active_indexer_count", info.activeIndexers),
			zap.Int64("new_active_indexer_count", newInfo.activeIndexers),
		)
		return true
	}
	return false
}

func (a *Appender) scalingInformation() scalingInfo {
	return a.scalingInfo.Load().(scalingInfo)
}

// indexFailureRate returns the decimal percentage of 429 / total docs.
func (a *Appender) indexFailureRate() float64 {
	return float64(atomic.LoadInt64(&a.tooManyRequests)) /
		float64(atomic.LoadInt64(&a.docsAdded))
}

// tracingEnabled checks whether we should be doing tracing
func (a *Appender) tracingEnabled() bool {
	return a.config.Tracer != nil && a.config.Tracer.Recording()
}

// activeLimit returns the value of GOMAXPROCS / 4. Which should limit the
// maximum number of active indexers to 25% of GOMAXPROCS.
//
// NOTE: There is also a sweet spot between Config.MaxRequests and the number
// of available indexers, where having N number of available bulk requests per
// active bulk indexer is required for optimal performance.
func activeLimit() int64 {
	if limit := float64(runtime.GOMAXPROCS(0)) / float64(4); limit > 1 {
		return int64(math.RoundToEven(limit))
	}
	return 1
}

// scalingInfo contains the number of active indexers and the timestamp of the
// latest time a scale action was performed. This structure is used within the
// Appender to coordinate scale actions with a CompareAndSwap operation.
type scalingInfo struct {
	lastAction     time.Time
	activeIndexers int64
}

func (s scalingInfo) ScaleDown(t time.Time) scalingInfo {
	return scalingInfo{lastAction: t, activeIndexers: s.activeIndexers - 1}
}

func (s scalingInfo) ScaleUp(t time.Time) scalingInfo {
	return scalingInfo{lastAction: t, activeIndexers: s.activeIndexers + 1}
}

func (s scalingInfo) withinCoolDown(cooldown time.Duration, now time.Time) bool {
	return s.lastAction.Add(cooldown).After(now)
}

// Stats holds bulk indexing statistics.
type Stats struct {
	// Active holds the active number of items waiting in the indexer's queue.
	Active int64

	// Added holds the number of items added to the indexer.
	Added int64

	// BulkRequests holds the number of bulk requests completed.
	BulkRequests int64

	// Failed holds the number of indexing operations that failed. It includes
	// all failures.
	Failed int64

	// FailedClient holds the number of indexing operations that failed with a
	// status_code >= 400 < 500, but not 429.
	FailedClient int64

	// FailedClient holds the number of indexing operations that failed with a
	// status_code >= 500.
	FailedServer int64

	// Indexed holds the number of indexing operations that have completed
	// successfully.
	Indexed int64

	// TooManyRequests holds the number of indexing operations that failed due
	// to Elasticsearch responding with 429 Too many Requests.
	TooManyRequests int64

	// BytesTotal represents the total number of bytes written to the request
	// body that is sent in the outgoing _bulk request to Elasticsearch.
	// The number of bytes written will be smaller when compression is enabled.
	// This implementation differs from the previous number reported by libbeat
	// which counts bytes at the transport level.
	BytesTotal int64

	// AvailableBulkRequests represents the number of bulk indexers
	// available for making bulk index requests.
	AvailableBulkRequests int64

	// IndexersActive represents the number of active bulk indexers that are
	// concurrently processing batches.
	IndexersActive int64

	// IndexersCreated represents the number of times new active indexers were
	// created.
	IndexersCreated int64

	// Downscales represents the number of times an active indexer was destroyed.
	IndexersDestroyed int64
}
