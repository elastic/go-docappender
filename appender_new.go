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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/paulbellamy/ratecounter"
	"go.elastic.co/apm/module/apmzap/v2"
	"go.elastic.co/apm/v2"
	"go.elastic.co/fastjson"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func NewAppenderV2(client *elasticsearch.Client, cfg Config) (*AppenderV2, error) {
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

	minFlushBytes := 16 * 1024 // 16kb
	if cfg.CompressionLevel != 0 && cfg.FlushBytes < minFlushBytes {
		return nil, fmt.Errorf(
			"flush bytes config value (%d) is too small and will be ignored with compression enabled. Use at least %d",
			cfg.FlushBytes, minFlushBytes,
		)
	}

	cfg.FlushBytes = 800 * 1024

	fmt.Printf("CONFIG: %#v\n", cfg)

	ms, err := newMetrics(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}

	res := &AppenderV2{
		fullFlush:                  make(chan struct{}, 1),
		bulkItems:                  make(chan bulkIndexerItem, cfg.DocumentBufferSize),
		config:                     cfg,
		minSingleIndexRequestBytes: 200 * 1024,
		maxMultiIndexRequestBytes:  cfg.FlushBytes,
		maxSingleIndexRequestBytes: cfg.FlushBytes,
		maxBytes:                   int64(2 * cfg.MaxRequests * cfg.FlushBytes),
		client:                     client,
		buffersFull:                make(chan *AppenderBuffer, 2*cfg.MaxRequests),
		doneAddingDocuments:        make(chan struct{}),
		indexer:                    newBulkIndexerV2(client, cfg.MaxDocumentRetries),
		metrics:                    &ms,
		requestsSem:                semaphore.NewWeighted(int64(cfg.MaxRequests)),
		freeBytes:                  make(chan struct{}, 1),
		limiter:                    ratelimit.NewUnlimited(),
		counter:                    ratecounter.NewRateCounter(60 * time.Second),
	}

	res.errgroup.Go(func() error {
		res.flushRoutine()
		return nil
	})
	res.errgroup.Go(func() error {
		res.asyncAppendDocuments()
		return nil
	})
	return res, nil
}

type AppenderV2 struct {
	client *elasticsearch.Client

	count       atomic.Int64
	buffers     sync.Map
	buffersLock sync.Mutex // hold when adding new buffers to the map
	bulkItems   chan bulkIndexerItem

	// An element is enqueued into fullFlush when everything must be sent to Elasticsearch, for
	// example when the total memory size exceeds the configured limit.
	fullFlush chan struct{}

	// Buffers are enqueued into buffersFull when they exceed the maximum bulk request size
	buffersFull chan *AppenderBuffer
	errgroup    errgroup.Group

	totalBytesMux sync.Mutex
	totalBytes    int64
	bufferedBytes atomic.Int64
	maxBytes      int64
	freeBytes     chan struct{}
	itemsAdded    atomic.Int64

	flushing atomic.Bool

	doneAddingDocuments chan struct{}
	closing             atomic.Bool

	indexer *bulkIndexerV2
	metrics *metrics

	requestsSem *semaphore.Weighted

	// **************************************
	// ** Candidates to be added to Config **
	// Minimum request bytes for a bulk request that spans a single index.
	minSingleIndexRequestBytes int

	// Maximum request bytes for a bulk request that spans a single index.
	maxSingleIndexRequestBytes int

	// Maximum request bytes for a bulk request that spans multiple indices.
	// This should be <= than maxSingleIndexRequestBytes.
	maxMultiIndexRequestBytes int
	// **************************************

	limiter ratelimit.Limiter
	counter *ratecounter.RateCounter

	config Config

	// legacy metrics for Stats()
	bulkRequests     int64
	docsAdded        int64
	docsActive       int64
	docsFailed       int64
	docsFailedClient int64
	docsFailedServer int64
	docsIndexed      int64
	tooManyRequests  int64
	bytesTotal       int64
}

func (a *AppenderV2) updateTotalBytes(delta int64) int64 {
	var res int64

	// Horrible implementation, but it does the job
	a.totalBytesMux.Lock()

	before := a.totalBytes
	a.totalBytes += delta
	res = a.totalBytes

	if before >= a.maxBytes && a.totalBytes < a.maxBytes {
		a.freeBytes <- struct{}{}
	}
	/*
		if before < a.maxBytes && a.totalBytes >= a.maxBytes {
			fmt.Printf("BLOCKED : %v -> %v\n", before, res)
		}
	*/

	a.totalBytesMux.Unlock()
	return res
}

// 3000/01/01
var farInTheFuture = time.UnixMilli(32503708800000)

type AppenderBuffer struct {
	buf           *bytes.Buffer
	writer        io.Writer
	gzipWriter    *gzip.Writer
	inceptionTime time.Time
	lastResetTime time.Time
	lock          sync.Mutex
	docsAdded     int
	totalBytes    int64
	retryCounts   map[int]int
	index         string
	retired       bool
}

// GetBuffer returns the buffer associated with a key.
// The buffer is returned without its lock held. The caller must check if the buffer is deleted
// prior to using it.
func (a *AppenderV2) GetBuffer(key string) *AppenderBuffer {
	r, found := a.buffers.Load(key)

	if found {
		return r.(*AppenderBuffer)
	}

	// Not found. Retry with the lock and create buffer if necessary.
	a.buffersLock.Lock()
	defer a.buffersLock.Unlock()

	r, found = a.buffers.Load(key)
	if found {
		// Another thread added it before we grabbed the lock
		return r.(*AppenderBuffer)
	}

	buffer := a.createNewBuffer()
	buffer.index = key
	a.buffers.Store(key, buffer)

	return buffer
}

func (a *AppenderV2) createNewBuffer() *AppenderBuffer {
	// Let's not worry about garbage collection for now
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))

	var writer io.Writer = buf
	var gzipWriter *gzip.Writer

	if a.config.CompressionLevel != gzip.NoCompression {
		// No error returned if the compression level is valid.
		gzipWriter, _ = gzip.NewWriterLevel(buf, a.config.CompressionLevel)
		writer = gzipWriter
	}
	buffer := &AppenderBuffer{
		buf:           buf,
		writer:        writer,
		gzipWriter:    gzipWriter,
		lastResetTime: time.Now(),
		retryCounts:   make(map[int]int),
	}
	return buffer
}

func (a *AppenderV2) Add(ctx context.Context, index string, document io.WriterTo) (err error) {
	if index == "" {
		return errMissingIndex
	}
	if document == nil {
		return errMissingBody
	}

	// We use a shared channel to buffer incoming documents that can't be immediately serialized.
	item := bulkIndexerItem{
		Index:     index,
		Body:      document,
		Timestamp: time.Now(),
	}

	a.bulkItems <- item
	return nil
}

func (a *AppenderV2) asyncAppendDocuments() {
	for item := range a.bulkItems {
		// We use the index as key, but we can use anything:
		// * Use a dedicated key for sparse indices? (10m / 60m indices)
		// * Use a dedicated key for 1m indices?
		// * etc etc
		buf := a.getLockedBuffer(item.Index)
		totalBytes := a.writeDocToBuf(buf, item.Index, item.DocumentID, item.Body, time.Now())
		buf.lock.Unlock()

		// Not great code, but oh well. Stop writing into buffers until some requests made it through
		if totalBytes >= a.maxBytes {
			<-a.freeBytes
		}
	}
	a.doneAddingDocuments <- struct{}{}
}

func (a *AppenderV2) getLockedBuffer(index string) *AppenderBuffer {
	var buf *AppenderBuffer
	for buf == nil {
		buf = a.GetBuffer(index)
		buf.lock.Lock()
		if buf.retired {
			buf.lock.Unlock()
			buf = nil
		}
	}
	return buf
}

func (a *AppenderV2) finalizeBuffer(buffer *AppenderBuffer) {
	if a.config.CompressionLevel != gzip.NoCompression {
		buffer.gzipWriter.Close()
	}
}

func (a *AppenderV2) Close(ctx context.Context) error {
	if a.closing.CompareAndSwap(false, true) {
		// fmt.Println("CLOSING")
		select {
		case a.fullFlush <- struct{}{}:
		default:
		}
		close(a.bulkItems)
	}
	return a.errgroup.Wait()
}

var concurrentRequests atomic.Int64

func (a *AppenderV2) flushBuffers(buffers []*AppenderBuffer) {
	// fmt.Printf("flushing %d buffers - %v\n", len(buffers), time.Now())
	for i := range buffers {
		if !buffers[i].retired { // enqueued as buffer too large
			a.buffers.Delete(buffers[i].index)
			buffers[i].retired = true
			a.bufferedBytes.Add(-buffers[i].totalBytes)
		}
		buffers[i].lock.Unlock()
		a.finalizeBuffer(buffers[i])
	}

	// Sort buffers from smallest to largest
	sort.Slice(buffers, func(i, j int) bool {
		return buffers[i].buf.Len() < buffers[j].buf.Len()
	})

	requestSize := 0
	var requestBuffers []*AppenderBuffer

	flushAndResetBuffers := func() {
		if len(requestBuffers) == 0 {
			return
		}
		if err := a.requestsSem.Acquire(context.Background(), 1); err != nil {
			return
		}

		var bufs []*AppenderBuffer
		bufs = append(bufs, requestBuffers...)
		reqSize := requestSize
		_ = reqSize

		a.errgroup.Go(func() error {
			defer a.requestsSem.Release(1)
			concurrency := concurrentRequests.Add(1)
			a.limiter.Take()
			err := a.flush(context.Background(), bufs)
			for i := range bufs {
				a.updateTotalBytes(-bufs[i].totalBytes)
				a.itemsAdded.Add(int64(-bufs[i].docsAdded))
			}
			fmt.Printf("Concurrency: %v\n", concurrency)
			concurrentRequests.Add(-1)
			return err
		})

		requestSize = 0
		requestBuffers = requestBuffers[:0]
	}

	for i := range buffers {
		bufLen := buffers[i].buf.Len()
		if bufLen > a.minSingleIndexRequestBytes {
			if requestSize > 0 {
				// Send what we had accumulated so far
				flushAndResetBuffers()
			}
			// Request for a single index
			requestSize = bufLen
			requestBuffers = append(requestBuffers, buffers[i])
			flushAndResetBuffers()
			continue
		}
		if requestSize+bufLen > a.maxMultiIndexRequestBytes {
			flushAndResetBuffers()
		}
		requestSize += bufLen
		requestBuffers = append(requestBuffers, buffers[i])
	}

	flushAndResetBuffers()
}

func (a *AppenderV2) getBuffersToFlush(condition func(buffer *AppenderBuffer) bool) []*AppenderBuffer {
	var res []*AppenderBuffer
	a.buffers.Range(func(key, value any) bool {
		buffer := value.(*AppenderBuffer)
		buffer.lock.Lock()
		if buffer.retired {
			buffer.lock.Unlock()
			return true
		}
		if condition(buffer) {
			res = append(res, buffer)
		} else {
			buffer.lock.Unlock()
		}
		return true
	})
	return res
}

func (a *AppenderV2) getNextFlushTime() time.Duration {
	now := time.Now()

	nextWakeUpTime := now.Add(a.config.FlushInterval)
	a.buffers.Range(func(key, value any) bool {
		buffer := value.(*AppenderBuffer)
		bufferFlushTime := buffer.inceptionTime.Add(a.config.FlushInterval)
		if bufferFlushTime.Before(nextWakeUpTime) {
			nextWakeUpTime = bufferFlushTime
		}
		return true
	})
	// Allow 10% FlushInterval between flushes to avoid waking up for a single buffer, and to avoid
	// clock precision issues
	minTimeBetweenFlushes := a.config.FlushInterval / 10
	nextWakeUpTime = nextWakeUpTime.Add(minTimeBetweenFlushes)

	return nextWakeUpTime.Sub(now)
}

func (a *AppenderV2) flushRoutine() {
	var flushDurationMillis = int64(a.config.FlushInterval.Milliseconds())
	flushTimer := time.NewTimer(time.Duration(flushDurationMillis) * time.Millisecond)
	lastFlush := false
	for {
		select {
		case <-flushTimer.C:
			fmt.Println("** Flush timer **")
			if a.closing.Load() {
				continue
			}
			startTime := time.Now()
			limit := startTime.Add(-a.config.FlushInterval)

			buffersToFlush := a.getBuffersToFlush(func(buffer *AppenderBuffer) bool {
				if buffer.inceptionTime.Before(limit) {
					return true
				}
				return false
			})
			a.flushBuffers(buffersToFlush)
			flushTimer.Reset(a.getNextFlushTime())
		case b := <-a.buffersFull:
			fmt.Printf("** Buffer full: %v (%v)\n", b.index, b.buf.Len())
			b.lock.Lock()
			a.flushBuffers([]*AppenderBuffer{b})
		case <-a.fullFlush:
			fmt.Println("** Full flush **")
			var totalSize int64
			buffersToFlush := a.getBuffersToFlush(func(buffer *AppenderBuffer) bool {
				totalSize += buffer.totalBytes
				return true
			})
			if lastFlush {
				done := false
				for !done {
					select {
					case b := <-a.buffersFull:
						b.lock.Lock()
						buffersToFlush = append(buffersToFlush, b)
					default:
						done = true
					}
				}
			}

			if lastFlush || totalSize >= int64(a.config.FlushBytes) {
				a.flushBuffers(buffersToFlush)
			} else {
				for i := range buffersToFlush {
					buffersToFlush[i].lock.Unlock()
				}
			}

			a.flushing.Store(false)
			if lastFlush {
				// We're done
				return
			}

			if !flushTimer.Stop() {
				<-flushTimer.C
			}
			flushTimer.Reset(a.getNextFlushTime())
		case <-a.doneAddingDocuments:
			fmt.Println("** Last flush **")
			lastFlush = true
			a.flushing.Store(true)
			select {
			case a.fullFlush <- struct{}{}:
			default:
			}
		}
	}
}

func (a *AppenderV2) flush(ctx context.Context, buffers []*AppenderBuffer) error {
	n := 0
	b := 0
	flushedBytes := 0
	oldestDoc := time.UnixMilli(32503708800000) // year 3000
	var buffNames []string
	for i := range buffers {
		if buffers[i].docsAdded == 0 {
			continue
		}
		buffNames = append(buffNames, buffers[i].index)
		n += buffers[i].docsAdded
		b += buffers[i].buf.Len()
		flushedBytes += buffers[i].buf.Len()
		if buffers[i].inceptionTime.Before(oldestDoc) {
			oldestDoc = buffers[i].inceptionTime
		}
	}

	if n == 0 {
		return nil
	}

	logger := a.config.Logger
	if a.tracingEnabled() {
		tx := a.config.Tracer.StartTransaction("docappender.flush", "output")
		tx.Context.SetLabel("documents", n)
		defer tx.End()
		ctx = apm.ContextWithTransaction(ctx, tx)

		// Add trace IDs to logger, to associate any per-item errors
		// below with the trace.
		logger = logger.With(apmzap.TraceContext(ctx)...)
	}

	var flushCtx context.Context

	if a.config.FlushTimeout != 0 {
		var flushCancel context.CancelFunc
		flushCtx, flushCancel = context.WithTimeout(ctx, a.config.FlushTimeout)
		defer flushCancel()
	} else {
		flushCtx = ctx
	}

	bulkType := "multi"
	if len(buffers) == 1 {
		bulkType = "single"
		fmt.Printf("SINGLE %s | docs=%d | bytes=%d\n", strings.Join(buffNames, ","), n, b)
	} else {
		fmt.Printf("MULTI(%d) %s | docs=%d | bytes=%d\n", len(buffers), strings.Join(buffNames, ","), n, b)
	}

	var resp BulkIndexerResponseStat
	var err error
	took := timeFunc(func() {
		resp, err = a.indexer.Flush(flushCtx, a.config.CompressionLevel != gzip.NoCompression, buffers)
	})
	a.addCount(1, &a.bulkRequests, a.metrics.bulkRequests, metric.WithAttributes(attribute.String("type", bulkType)))

	attrs := metric.WithAttributeSet(a.config.MetricAttributes)
	a.metrics.flushDuration.Record(context.Background(),
		took.Seconds(), attrs)
	a.metrics.bufferDuration.Record(context.Background(),
		time.Since(oldestDoc).Seconds(), attrs)

	if flushedBytes > 0 {
		a.addCount(int64(flushedBytes), &a.bytesTotal, a.metrics.bytesTotal)
	}
	if err != nil {
		atomic.AddInt64(&a.docsFailed, int64(n))
		logger.Error("bulk indexing request failed", zap.Error(err))
		if a.tracingEnabled() {
			apm.CaptureError(ctx, err).Send()
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			a.addCount(int64(n),
				nil,
				a.metrics.docsIndexed,
				metric.WithAttributes(attribute.String("status", "Timeout")),
			)
		}

		var errTooMany errorTooManyRequests
		// 429 may be returned as errors from the bulk indexer.
		if errors.As(err, &errTooMany) {
			a.addCount(int64(n),
				&a.tooManyRequests,
				a.metrics.docsIndexed,
				metric.WithAttributes(attribute.String("status", "TooMany")),
			)
		}
		return err
	}
	var docsFailed, docsIndexed, tooManyRequests, clientFailed, serverFailed int64
	docsIndexed = resp.Indexed
	var failedCount map[BulkIndexerResponseItem]int
	if len(resp.FailedDocs) > 0 {
		failedCount = make(map[BulkIndexerResponseItem]int, len(resp.FailedDocs))
	}
	for _, info := range resp.FailedDocs {
		info.Position = 0 // reset position so that the response item can be used as key in the map
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
			failedCount[info]++
			if a.tracingEnabled() {
				apm.CaptureError(ctx, errors.New(info.Error.Reason)).Send()
			}
		} else {
			docsIndexed++
		}
	}
	for key, count := range failedCount {
		logger.Error(fmt.Sprintf("failed to index documents in '%s' (%s): %s",
			key.Index, key.Error.Type, key.Error.Reason,
		), zap.Int("documents", count))
	}
	if docsFailed > 0 {
		atomic.AddInt64(&a.docsFailed, docsFailed)
	}
	if docsIndexed > 0 {
		a.addCount(docsIndexed,
			&a.docsIndexed,
			a.metrics.docsIndexed,
			metric.WithAttributes(attribute.String("status", "Success")),
		)
		a.counter.Incr(docsIndexed)
		// Gives feedback quicker than apmbench
		fmt.Printf("RATE: %v\n", a.counter.Rate()/60)
	}
	if tooManyRequests > 0 {
		a.addCount(tooManyRequests,
			&a.tooManyRequests,
			a.metrics.docsIndexed,
			metric.WithAttributes(attribute.String("status", "TooMany")),
		)
	}
	if clientFailed > 0 {
		a.addCount(clientFailed,
			&a.docsFailedClient,
			a.metrics.docsIndexed,
			metric.WithAttributes(attribute.String("status", "FailedClient")),
		)
	}
	if serverFailed > 0 {
		a.addCount(serverFailed,
			&a.docsFailedServer,
			a.metrics.docsIndexed,
			metric.WithAttributes(attribute.String("status", "FailedServer")),
		)
	}
	logger.Debug(
		"bulk request completed",
		zap.Int64("docs_indexed", docsIndexed),
		zap.Int64("docs_failed", docsFailed),
		zap.Int64("docs_rate_limited", tooManyRequests),
	)

	return nil
}

// writeDocToBuf expects buffer to be locked
func (a *AppenderV2) writeDocToBuf(b *AppenderBuffer, index, documentID string, document io.WriterTo, enqueuedTime time.Time) int64 {
	beforeBytes := b.buf.Len()
	writer := b.writer

	// Might need to use a shared object
	var jsonw fastjson.Writer

	jsonw.RawString(`{"create":{`)
	if documentID != "" {
		jsonw.RawString(`"_id":`)
		jsonw.String(documentID)
	}
	if index != "" {
		if documentID != "" {
			jsonw.RawByte(',')
		}
		jsonw.RawString(`"_index":`)
		jsonw.String(index)
	}
	jsonw.RawString("}}\n")
	writer.Write(jsonw.Bytes())

	document.WriteTo(writer)
	writer.Write([]byte("\n"))

	b.docsAdded++
	a.itemsAdded.Add(1)

	afterBytes := b.buf.Len()

	// This is an approximation as the gzip writer is buffering data.
	newBytes := int64(afterBytes - beforeBytes)
	totalBytes := a.updateTotalBytes(newBytes)
	a.bufferedBytes.Add(newBytes)
	b.totalBytes += newBytes

	if b.inceptionTime.IsZero() {
		b.inceptionTime = enqueuedTime
	}

	if b.buf.Len() >= a.maxSingleIndexRequestBytes {
		a.buffers.Delete(b.index)
		b.retired = true
		a.bufferedBytes.Add(-b.totalBytes)
		a.buffersFull <- b
		return totalBytes
	}

	// This is for compatibility with unit tests only. It makes no real sense to do a full flush
	// based on buffered bytes
	// Need to update unit tests to use maxSingleIndexRequestBytes as appropriate if they need to force a per-buffer flush
	/*
		if a.bufferedBytes.Load() > int64(a.config.FlushBytes) {
			if a.flushing.CompareAndSwap(false, true) {
				a.fullFlush <- struct{}{}
			}
		}
	*/

	return totalBytes
}

func (a *AppenderV2) addCount(delta int64, lm *int64, m metric.Int64Counter, opts ...metric.AddOption) {
	// legacy metric
	if lm != nil {
		atomic.AddInt64(lm, delta)
	}

	attrs := metric.WithAttributeSet(a.config.MetricAttributes)
	m.Add(context.Background(), delta, append(opts, attrs)...)
}

func (a *AppenderV2) addUpDownCount(delta int64, lm *int64, m metric.Int64UpDownCounter, opts ...metric.AddOption) {
	// legacy metric
	if lm != nil {
		atomic.AddInt64(lm, delta)
	}

	attrs := metric.WithAttributeSet(a.config.MetricAttributes)
	m.Add(context.Background(), delta, append(opts, attrs)...)
}

// Stats returns the bulk indexing stats.
func (a *AppenderV2) Stats() Stats {
	return Stats{
		Added:           atomic.LoadInt64(&a.docsAdded),
		Active:          atomic.LoadInt64(&a.docsActive),
		BulkRequests:    atomic.LoadInt64(&a.bulkRequests),
		Failed:          atomic.LoadInt64(&a.docsFailed),
		FailedClient:    atomic.LoadInt64(&a.docsFailedClient),
		FailedServer:    atomic.LoadInt64(&a.docsFailedServer),
		Indexed:         atomic.LoadInt64(&a.docsIndexed),
		TooManyRequests: atomic.LoadInt64(&a.tooManyRequests),
		BytesTotal:      atomic.LoadInt64(&a.bytesTotal),
	}
}

// tracingEnabled checks whether we should be doing tracing
func (a *AppenderV2) tracingEnabled() bool {
	return a.config.Tracer != nil && a.config.Tracer.Recording()
}
