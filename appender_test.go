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

package docappender_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/apm/v2/apmtest"
	"go.elastic.co/apm/v2/model"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/elastic/go-docappender"
	"github.com/elastic/go-docappender/docappendertest"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func TestAppender(t *testing.T) {
	var bytesTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result := docappendertest.DecodeBulkRequest(r)
		result.HasErrors = true
		// Respond with an error for the first two items, with one indicating
		// "too many requests". These will be recorded as failures in indexing
		// stats.
		for i := range result.Items {
			if i > 2 {
				break
			}
			status := http.StatusInternalServerError
			switch i {
			case 1:
				status = http.StatusTooManyRequests
			case 2:
				status = http.StatusUnauthorized
			}
			for action, item := range result.Items[i] {
				item.Status = status
				result.Items[i][action] = item
			}
		}
		json.NewEncoder(w).Encode(result)
	})

	rdr := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		},
	))

	indexerAttrs := attribute.NewSet(
		attribute.String("a", "b"), attribute.String("c", "d"),
	)

	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval:    time.Minute,
		MeterProvider:    sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr)),
		MetricAttributes: indexerAttrs,
	})

	require.NoError(t, err)
	defer indexer.Close(context.Background())

	available := indexer.Stats().AvailableBulkRequests
	const N = 10
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			// Because the internal channel is buffered to increase performance,
			// the available indexer may not take documents right away, loop until
			// the available bulk requests has been lowered.
			if indexer.Stats().AvailableBulkRequests < available {
				break loop
			}
		case <-timeout:
			t.Fatalf("timed out waiting for the active bulk indexer to pull from the available queue")
		}
	}
	// Appender has not been flushed, there is one active bulk indexer.
	assert.Equal(t, docappender.Stats{Added: N, Active: N, AvailableBulkRequests: 9, IndexersActive: 1}, indexer.Stats())

	// Closing the indexer flushes enqueued documents.
	err = indexer.Close(context.Background())
	require.NoError(t, err)
	stats := indexer.Stats()
	failed := int64(3)
	assert.Equal(t, docappender.Stats{
		Added:                 N,
		Active:                0,
		BulkRequests:          1,
		Failed:                failed,
		FailedClient:          1,
		FailedServer:          1,
		Indexed:               N - failed,
		TooManyRequests:       1,
		AvailableBulkRequests: 10,
		BytesTotal:            bytesTotal,
	}, stats)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(context.Background(), &rm))
	var asserted int
	assertCounter := func(metric metricdata.Metrics, count int64, attrs attribute.Set) {
		asserted++
		counter := metric.Data.(metricdata.Sum[int64])
		for _, dp := range counter.DataPoints {
			assert.Equal(t, count, dp.Value)
			assert.Equal(t, attrs, dp.Attributes)
		}
	}
	// check the set of names and then check the counter or histogram
	unexpectedMetrics := []string{}
	for _, metric := range rm.ScopeMetrics[0].Metrics {
		switch metric.Name {
		case "elasticsearch.events.count":
			assertCounter(metric, stats.Added, indexerAttrs)
		case "elasticsearch.events.queued":
			assertCounter(metric, stats.Active, indexerAttrs)
		case "elasticsearch.bulk_requests.count":
			assertCounter(metric, stats.BulkRequests, indexerAttrs)
		case "elasticsearch.failed.count":
			assertCounter(metric, stats.Failed, indexerAttrs)
		case "elasticsearch.failed.client.count":
			assertCounter(metric, stats.FailedClient, indexerAttrs)
		case "elasticsearch.failed.server.count":
			assertCounter(metric, stats.FailedServer, indexerAttrs)
		case "elasticsearch.events.processed":
			assertCounter(metric, stats.Indexed, indexerAttrs)
		case "elasticsearch.failed.too_many_reqs":
			assertCounter(metric, stats.TooManyRequests, indexerAttrs)
		case "elasticsearch.bulk_requests.available":
			assertCounter(metric, stats.AvailableBulkRequests, indexerAttrs)
		case "elasticsearch.flushed.bytes":
			assertCounter(metric, stats.BytesTotal, indexerAttrs)
		case "elasticsearch.buffer.latency":
			// expect this metric name but no assertions done
			// as it's histogram and it's checked elsewhere
		case "elasticsearch.flushed.latency":
			// expect this metric name but no assertions done
			// as it's histogram and it's checked elsewhere
		default:
			unexpectedMetrics = append(unexpectedMetrics, metric.Name)
		}
	}
	assert.Empty(t, unexpectedMetrics)
	assert.Equal(t, 10, asserted)
}

func TestAppenderAvailableAppenders(t *testing.T) {
	unblockRequests := make(chan struct{})
	receivedFlush := make(chan struct{})
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		receivedFlush <- struct{}{}
		// Wait until signaled to service requests
		<-unblockRequests
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{FlushInterval: time.Minute, FlushBytes: 1})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	const N = 10
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for i := 0; i < N; i++ {
		select {
		case <-receivedFlush:
		case <-timeout.C:
			t.Fatalf("timed out waiting for %d, received %d", N, i)
		}
	}
	stats := indexer.Stats()
	// FlushBytes is set arbitrarily low, forcing a flush on each new
	// document. There should be no available bulk indexers.
	assert.Equal(t, docappender.Stats{Added: N, Active: N, AvailableBulkRequests: 0, IndexersActive: 1}, stats)

	close(unblockRequests)
	err = indexer.Close(context.Background())
	require.NoError(t, err)
	stats = indexer.Stats()
	stats.BytesTotal = 0 // Asserted elsewhere.
	assert.Equal(t, docappender.Stats{
		Added:                 N,
		BulkRequests:          N,
		Indexed:               N,
		AvailableBulkRequests: 10,
	}, stats)
}

func TestAppenderEncoding(t *testing.T) {
	var indexed [][]byte
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		var result esutil.BulkIndexerResponse
		indexed, result = docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval: time.Minute,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	err = indexer.Add(context.Background(), "logs-foo-testing", newJSONReader(map[string]any{
		"@timestamp":            time.Unix(123, 456789111).UTC().Format(docappendertest.TimestampFormat),
		"data_stream.type":      "logs",
		"data_stream.dataset":   "foo",
		"data_stream.namespace": "testing",
	}))
	require.NoError(t, err)

	// Closing the indexer flushes enqueued documents.
	err = indexer.Close(context.Background())
	require.NoError(t, err)

	require.Len(t, indexed, 1)
	var decoded map[string]interface{}
	err = json.Unmarshal(indexed[0], &decoded)
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"@timestamp":            "1970-01-01T00:02:03.456Z",
		"data_stream.type":      "logs",
		"data_stream.dataset":   "foo",
		"data_stream.namespace": "testing",
	}, decoded)
}

func TestAppenderCompressionLevel(t *testing.T) {
	var bytesTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{
		CompressionLevel: gzip.BestSpeed,
		FlushInterval:    time.Minute,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	// Closing the indexer flushes enqueued documents.
	err = indexer.Close(context.Background())
	require.NoError(t, err)
	stats := indexer.Stats()
	assert.Equal(t, docappender.Stats{
		Added:                 1,
		Active:                0,
		BulkRequests:          1,
		Failed:                0,
		Indexed:               1,
		TooManyRequests:       0,
		AvailableBulkRequests: 10,
		BytesTotal:            bytesTotal,
	}, stats)
}

func TestAppenderFlushInterval(t *testing.T) {
	requests := make(chan struct{}, 1)
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case requests <- struct{}{}:
		}
	})
	indexer, err := docappender.New(client, docappender.Config{
		// Default flush bytes is 5MB
		FlushInterval: time.Millisecond,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	select {
	case <-requests:
		t.Fatal("unexpected request, no documents buffered")
	case <-time.After(50 * time.Millisecond):
	}

	addMinimalDoc(t, indexer, "logs-foo-testing")

	select {
	case <-requests:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for request, flush interval elapsed")
	}
}

func TestAppenderFlushMetric(t *testing.T) {
	requests := make(chan esutil.BulkIndexerResponse)
	client := docappendertest.NewMockElasticsearchClient(t, func(_ http.ResponseWriter, r *http.Request) {
		_, items := docappendertest.DecodeBulkRequest(r)
		select {
		case <-r.Context().Done():
		case requests <- items:
		}
	})

	rdr := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		},
	))

	indexerAttrs := attribute.NewSet(
		attribute.String("a", "b"), attribute.String("c", "d"),
	)
	indexer, err := docappender.New(client, docappender.Config{
		FlushBytes:       1,
		MeterProvider:    sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr)),
		MetricAttributes: indexerAttrs,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	select {
	case <-requests:
		t.Fatal("unexpected request, no documents buffered")
	case <-time.After(50 * time.Millisecond):
	}

	docs := 10
	for i := 0; i < docs; i++ {
		addMinimalDoc(t, indexer, fmt.Sprintf("logs-foo-testing-%d", i))
	}

	timeout := time.After(time.Second)
	for i := 0; i < docs; i++ {
		select {
		case res := <-requests:
			assert.Len(t, res.Items, 1)
		case <-timeout:
			t.Fatal("timed out waiting for request, flush interval elapsed")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(ctx, &rm))

	var asserted int
	assertHistogram := func(latencyMetric metricdata.Metrics, count int, ignoreCount bool, attrs attribute.Set) {
		asserted++
		assert.Equal(t, "s", latencyMetric.Unit)
		histo := latencyMetric.Data.(metricdata.Histogram[float64])
		for _, dp := range histo.DataPoints {
			if !ignoreCount {
				assert.Equal(t, count, int(dp.Count))
			} else {
				assert.Greater(t, int(dp.Count), count)
			}
			assert.Positive(t, dp.Sum)
			assert.Equal(t, attrs, dp.Attributes)
		}
	}
	for _, metric := range rm.ScopeMetrics[0].Metrics {
		switch metric.Name {
		case "elasticsearch.buffer.latency":
			assertHistogram(metric, docs, false, indexerAttrs)
		case "elasticsearch.flushed.latency":
			assertHistogram(metric, 2, true, indexerAttrs)
		}
	}
	assert.Equal(t, 2, asserted)
}

func TestAppenderFlushBytes(t *testing.T) {
	requests := make(chan struct{}, 1)
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- struct{}{}:
		default:
		}
	})
	indexer, err := docappender.New(client, docappender.Config{
		FlushBytes: 1024,
		// Default flush interval is 30 seconds
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	select {
	case <-requests:
		t.Fatal("unexpected request, flush bytes not exceeded")
	case <-time.After(50 * time.Millisecond):
	}

	for i := 0; i < 100; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	select {
	case <-requests:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for request, flush bytes exceeded")
	}
}

func TestAppenderServerError(t *testing.T) {
	var bytesTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		w.WriteHeader(http.StatusInternalServerError)
	})
	indexer, err := docappender.New(client, docappender.Config{FlushInterval: time.Minute})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	// Closing the indexer flushes enqueued documents.
	err = indexer.Close(context.Background())
	require.EqualError(t, err, "flush failed: [500 Internal Server Error] ")
	stats := indexer.Stats()
	assert.Equal(t, docappender.Stats{
		Added:                 1,
		Active:                0,
		BulkRequests:          1,
		Failed:                1,
		AvailableBulkRequests: 10,
		BytesTotal:            bytesTotal,
	}, stats)
}

func TestAppenderServerErrorTooManyRequests(t *testing.T) {
	var bytesTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		// Set the r.ContentLength rather than sum it since 429s will be
		// retried by the go-elasticsearch transport.
		bytesTotal = r.ContentLength
		w.WriteHeader(http.StatusTooManyRequests)
	})
	indexer, err := docappender.New(client, docappender.Config{FlushInterval: time.Minute})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	// Closing the indexer flushes enqueued documents.
	err = indexer.Close(context.Background())
	require.EqualError(t, err, "flush failed: [429 Too Many Requests] ")
	stats := indexer.Stats()
	assert.Equal(t, docappender.Stats{
		Added:                 1,
		Active:                0,
		BulkRequests:          1,
		Failed:                1,
		TooManyRequests:       1,
		AvailableBulkRequests: 10,
		BytesTotal:            bytesTotal,
	}, stats)
}

func TestAppenderIndexFailedLogging(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		for i, item := range result.Items {
			itemResp := item["create"]
			itemResp.Index = "an_index"
			itemResp.Error.Type = "error_type"
			if i%2 == 0 {
				itemResp.Error.Reason = "error_reason_even"
			} else {
				itemResp.Error.Reason = "error_reason_odd"
			}
			item["create"] = itemResp
		}
		result.HasErrors = true
		json.NewEncoder(w).Encode(result)
	})

	core, observed := observer.New(zap.NewAtomicLevelAt(zapcore.DebugLevel))
	indexer, err := docappender.New(client, docappender.Config{
		FlushBytes: 500,
		Logger:     zap.New(core),
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	const N = 4
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}
	err = indexer.Close(context.Background())
	assert.NoError(t, err)

	entries := observed.FilterMessageSnippet("failed to index document").TakeAll()
	require.Len(t, entries, N)
	assert.Equal(t, "failed to index document in 'an_index' (error_type): error_reason_even", entries[0].Message)
	assert.Equal(t, "failed to index document in 'an_index' (error_type): error_reason_odd", entries[1].Message)
	assert.Equal(t, "failed to index document in 'an_index' (error_type): error_reason_even", entries[2].Message)
	assert.Equal(t, "failed to index document in 'an_index' (error_type): error_reason_odd", entries[3].Message)
}

func TestAppenderCloseFlushContext(t *testing.T) {
	srvctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-srvctx.Done():
		case <-r.Context().Done():
		}
	})
	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval: time.Millisecond,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	errch := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		errch <- indexer.Close(ctx)
	}()

	// Should be blocked in flush.
	select {
	case err := <-errch:
		t.Fatalf("unexpected return from indexer.Close: %s", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errch:
		assert.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for flush to unblock")
	}
}

func TestAppenderCloseInterruptAdd(t *testing.T) {
	srvctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-srvctx.Done():
		case <-r.Context().Done():
		}
	})
	documentBufferSize := 100
	indexer, err := docappender.New(client, docappender.Config{
		// Set FlushBytes to 1 so a single document causes a flush.
		FlushBytes:         1,
		DocumentBufferSize: documentBufferSize,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	// Fill up all the bulk requests and the buffered channel.
	for n := indexer.Stats().AvailableBulkRequests + int64(documentBufferSize); n >= 0; n-- {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	// Call Add again; this should block, as all bulk requests are blocked and the buffered channel is full.
	readInvoked := make(chan struct{})
	added := make(chan error, 1)
	addContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		added <- indexer.Add(addContext, "logs-foo-testing", readerFunc(func(p []byte) (int, error) {
			fmt.Println("hello?")
			close(readInvoked)
			return copy(p, "{}"), nil
		}))
	}()

	// Add should block, and the Reader should not be invoked.
	select {
	case err := <-added:
		t.Fatal("Add returned unexpectedly", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-readInvoked:
		t.Fatal("Reader invoked unexpectedly", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Close should block waiting for the enqueued documents to be flushed, but
	// must honour the given context and not block forever.
	closed := make(chan error, 1)
	closeContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		closed <- indexer.Close(closeContext)
	}()
	select {
	case err := <-closed:
		t.Fatal("Add returned unexpectedly", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-closed:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Close to return")
	}
	select {
	case err := <-added:
		assert.ErrorIs(t, err, docappender.ErrClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Add to return")
	}
}

type readerFunc func([]byte) (n int, err error)

func (f readerFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

func TestAppenderFlushGoroutineStopped(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})

	indexer, err := docappender.New(client, docappender.Config{FlushBytes: 1})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	before := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	var after int
	deadline := time.Now().Add(10 * time.Second)
	for after > before && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		after = runtime.NumGoroutine()
	}
	assert.GreaterOrEqual(t, before, after, "Leaked %d goroutines", after-before)
}

func TestAppenderUnknownResponseFields(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ingest_took":123}`))
	})
	indexer, err := docappender.New(client, docappender.Config{})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	err = indexer.Close(context.Background())
	assert.NoError(t, err)
}

func TestAppenderCloseBusyIndexer(t *testing.T) {
	// This test ensures that all the channel items are consumed and indexed
	// when the indexer is closed.
	var bytesTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { indexer.Close(context.Background()) })

	const N = 5000
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	assert.NoError(t, indexer.Close(context.Background()))

	assert.Equal(t, docappender.Stats{
		Added:                 N,
		Indexed:               N,
		BulkRequests:          1,
		BytesTotal:            bytesTotal,
		AvailableBulkRequests: 10,
		IndexersActive:        0}, indexer.Stats())
}

func TestAppenderScaling(t *testing.T) {
	newIndexer := func(t *testing.T, cfg docappender.Config) *docappender.Appender {
		t.Helper()
		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			_, result := docappendertest.DecodeBulkRequest(r)
			json.NewEncoder(w).Encode(result)
		})
		indexer, err := docappender.New(client, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { indexer.Close(context.Background()) })
		return indexer
	}
	sendDocuments := func(t *testing.T, indexer *docappender.Appender, docs int) {
		for i := 0; i < docs; i++ {
			err := indexer.Add(context.Background(), "logs-foo-testing", newJSONReader(map[string]any{
				"@timestamp":            time.Now().Format(docappendertest.TimestampFormat),
				"data_stream.type":      "logs",
				"data_stream.dataset":   "foo",
				"data_stream.namespace": "namespace",
			}))
			require.NoError(t, err)
		}
	}
	waitForScaleUp := func(t *testing.T, indexer *docappender.Appender, n int64) {
		timeout := time.NewTimer(5 * time.Second)
		stats := indexer.Stats()
		limit := int64(runtime.GOMAXPROCS(0) / 4)
		for stats.IndexersActive < n {
			stats = indexer.Stats()
			require.LessOrEqual(t, stats.IndexersActive, limit)
			select {
			case <-time.After(10 * time.Millisecond):
			case <-timeout.C:
				stats = indexer.Stats()
				require.GreaterOrEqual(t, stats.IndexersActive, n, "stats: %+v", stats)
			}
		}
		stats = indexer.Stats()
		assert.Greater(t, stats.IndexersCreated, int64(0), "No upscales took place: %+v", stats)
	}
	waitForScaleDown := func(t *testing.T, indexer *docappender.Appender, n int64) {
		timeout := time.NewTimer(5 * time.Second)
		stats := indexer.Stats()
		for stats.IndexersActive > n {
			stats = indexer.Stats()
			require.Greater(t, stats.IndexersActive, int64(0))
			select {
			case <-time.After(10 * time.Millisecond):
			case <-timeout.C:
				stats = indexer.Stats()
				require.LessOrEqual(t, stats.IndexersActive, n, "stats: %+v", stats)
			}
		}
		stats = indexer.Stats()
		assert.Greater(t, stats.IndexersDestroyed, int64(0), "No downscales took place: %+v", stats)
		assert.Equal(t, stats.IndexersActive, int64(n), "%+v", stats)
	}
	waitForBulkRequests := func(t *testing.T, indexer *docappender.Appender, n int64) {
		timeout := time.After(time.Second)
		for indexer.Stats().BulkRequests < n {
			select {
			case <-time.After(time.Millisecond):
			case <-timeout:
				t.Fatalf("timed out while waiting for documents to be indexed: %+v", indexer.Stats())
			}
		}
	}
	t.Run("DownscaleIdle", func(t *testing.T) {
		// Override the default GOMAXPROCS, ensuring the active indexers can scale up.
		setGOMAXPROCS(t, 12)
		indexer := newIndexer(t, docappender.Config{
			FlushInterval: time.Millisecond,
			FlushBytes:    1,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 1, CoolDown: 1,
				},
				ScaleDown: docappender.ScaleActionConfig{
					Threshold: 2, CoolDown: time.Millisecond,
				},
				IdleInterval: 50 * time.Millisecond,
			},
		})
		docs := int64(20)
		sendDocuments(t, indexer, int(docs))
		waitForScaleUp(t, indexer, 3)
		waitForScaleDown(t, indexer, 1)
		stats := indexer.Stats()
		stats.BytesTotal = 0
		assert.Equal(t, docappender.Stats{
			Active:                0,
			Added:                 docs,
			Indexed:               docs,
			BulkRequests:          docs,
			IndexersCreated:       2,
			IndexersDestroyed:     2,
			AvailableBulkRequests: 10,
			IndexersActive:        1,
		}, stats)
	})
	t.Run("DownscaleActiveLimit", func(t *testing.T) {
		// Override the default GOMAXPROCS, ensuring the active indexers can scale up.
		setGOMAXPROCS(t, 12)
		indexer := newIndexer(t, docappender.Config{
			FlushInterval: time.Millisecond,
			FlushBytes:    1,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 5, CoolDown: 1,
				},
				ScaleDown: docappender.ScaleActionConfig{
					Threshold: 100, CoolDown: time.Minute,
				},
				IdleInterval: 100 * time.Millisecond,
			},
		})
		docs := int64(14)
		sendDocuments(t, indexer, int(docs))
		waitForScaleUp(t, indexer, 3)
		// Set the gomaxprocs to 4, which should result in an activeLimit of 1.
		setGOMAXPROCS(t, 4)
		// Wait for the indexers to scale down from 3 to 1. The downscale cool
		// down of `1m` isn't respected, since the active limit is breached with
		// the gomaxprocs change.
		waitForScaleDown(t, indexer, 1)
		// Wait for all the documents to be indexed.
		waitForBulkRequests(t, indexer, docs)

		stats := indexer.Stats()
		stats.BytesTotal = 0
		assert.Equal(t, docappender.Stats{
			Active:                0,
			Added:                 docs,
			Indexed:               docs,
			BulkRequests:          docs,
			AvailableBulkRequests: 10,
			IndexersActive:        1,
			IndexersCreated:       2,
			IndexersDestroyed:     2,
		}, stats)
	})
	t.Run("UpscaleCooldown", func(t *testing.T) {
		// Override the default GOMAXPROCS, ensuring the active indexers can scale up.
		setGOMAXPROCS(t, 12)
		indexer := newIndexer(t, docappender.Config{
			FlushInterval: time.Millisecond,
			FlushBytes:    1,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 10,
					CoolDown:  time.Minute,
				},
				ScaleDown: docappender.ScaleActionConfig{
					Threshold: 10,
					CoolDown:  time.Minute,
				},
				IdleInterval: 100 * time.Millisecond,
			},
		})
		docs := int64(50)
		sendDocuments(t, indexer, int(docs))
		waitForScaleUp(t, indexer, 2)
		// Wait for all the documents to be indexed.
		waitForBulkRequests(t, indexer, docs)

		assert.Equal(t, int64(2), indexer.Stats().IndexersActive)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, indexer.Close(ctx))
		stats := indexer.Stats()
		stats.BytesTotal = 0
		assert.Equal(t, docappender.Stats{
			Active:                0,
			Added:                 docs,
			Indexed:               docs,
			BulkRequests:          docs,
			AvailableBulkRequests: 10,
			IndexersActive:        0,
			IndexersCreated:       1,
			IndexersDestroyed:     0,
		}, stats)
	})
	t.Run("Downscale429Rate", func(t *testing.T) {
		// Override the default GOMAXPROCS, ensuring the active indexers can scale up.
		setGOMAXPROCS(t, 12)
		var mu sync.RWMutex
		var tooMany bool // must be accessed with the mutex held.
		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			_, result := docappendertest.DecodeBulkRequest(r)
			mu.RLock()
			tooManyResp := tooMany
			mu.RUnlock()
			if tooManyResp {
				result.HasErrors = true
				for i := 0; i < len(result.Items); i++ {
					item := result.Items[i]
					resp := item["create"]
					resp.Status = http.StatusTooManyRequests
					item["create"] = resp
				}
			}
			json.NewEncoder(w).Encode(result)
		})
		indexer, err := docappender.New(client, docappender.Config{
			FlushInterval: time.Millisecond,
			FlushBytes:    1,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 5, CoolDown: 1,
				},
				ScaleDown: docappender.ScaleActionConfig{
					Threshold: 100, CoolDown: 100 * time.Millisecond,
				},
				IdleInterval: 100 * time.Millisecond,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { indexer.Close(context.Background()) })
		docs := int64(20)
		sendDocuments(t, indexer, int(docs))
		waitForScaleUp(t, indexer, 3)
		waitForBulkRequests(t, indexer, docs)

		// Make the mocked elasticsaerch return 429 responses and wait for the
		// active indexers to be scaled down to the minimum.
		mu.Lock()
		tooMany = true
		mu.Unlock()
		docs += 5
		sendDocuments(t, indexer, 5)
		waitForScaleDown(t, indexer, 1)
		waitForBulkRequests(t, indexer, docs)

		// index 600 documents and ensure that scale ups happen to the maximum after
		// the threshold is exceeded.
		mu.Lock()
		tooMany = false
		mu.Unlock()
		docs += 600
		sendDocuments(t, indexer, 600)
		waitForScaleUp(t, indexer, 3)
		waitForBulkRequests(t, indexer, docs)

		stats := indexer.Stats()
		assert.Equal(t, int64(3), stats.IndexersActive)
		assert.Equal(t, int64(4), stats.IndexersCreated)
		assert.Equal(t, int64(2), stats.IndexersDestroyed)
	})
}

func setGOMAXPROCS(t *testing.T, new int) {
	t.Helper()
	old := runtime.GOMAXPROCS(0)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(old)
	})
	runtime.GOMAXPROCS(new)
}

func TestAppenderTracing(t *testing.T) {
	testAppenderTracing(t, 200, "success")
	testAppenderTracing(t, 400, "failure")
}

func testAppenderTracing(t *testing.T, statusCode int, expectedOutcome string) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})

	core, observed := observer.New(zap.NewAtomicLevelAt(zapcore.DebugLevel))
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()
	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval: time.Minute,
		Logger:        zap.New(core),
		Tracer:        tracer.Tracer,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	const N = 100
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	// Closing the indexer flushes enqueued documents.
	_ = indexer.Close(context.Background())

	tracer.Flush(nil)
	payloads := tracer.Payloads()
	require.Len(t, payloads.Transactions, 1)
	require.Len(t, payloads.Spans, 1)

	assert.Equal(t, expectedOutcome, payloads.Transactions[0].Outcome)
	assert.Equal(t, "output", payloads.Transactions[0].Type)
	assert.Equal(t, model.IfaceMapItem{Key: "documents", Value: float64(N)},
		payloads.Transactions[0].Context.Tags[0],
	)
	assert.Equal(t, "docappender.flush", payloads.Transactions[0].Name)
	assert.Equal(t, "Elasticsearch: POST _bulk", payloads.Spans[0].Name)
	assert.Equal(t, "db", payloads.Spans[0].Type)
	assert.Equal(t, "elasticsearch", payloads.Spans[0].Subtype)

	correlatedLogs := observed.FilterFieldKey("transaction.id").All()
	assert.NotEmpty(t, correlatedLogs)
	for _, entry := range correlatedLogs {
		fields := entry.ContextMap()
		assert.Equal(t, fmt.Sprintf("%x", payloads.Transactions[0].ID), fields["transaction.id"])
		assert.Equal(t, fmt.Sprintf("%x", payloads.Transactions[0].TraceID), fields["trace.id"])
	}
}

func addMinimalDoc(t testing.TB, indexer *docappender.Appender, index string) {
	err := indexer.Add(context.Background(), "logs-foo-testing", newJSONReader(map[string]any{
		"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
	}))
	require.NoError(t, err)
}

func newJSONReader(v any) *bytes.Reader {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(data)
}
