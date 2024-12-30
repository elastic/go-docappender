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
	"io"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/apm/v2/apmtest"
	"go.elastic.co/apm/v2/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/elastic/go-docappender/v2"
	"github.com/elastic/go-docappender/v2/docappendertest"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func TestAppender(t *testing.T) {
	var bytesTotal int64
	var bytesUncompressed int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result, stat := docappendertest.DecodeBulkRequestWithStats(r)
		bytesUncompressed += stat.UncompressedBytes
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
		Added:                  N,
		Active:                 0,
		BulkRequests:           1,
		Failed:                 failed,
		FailedClient:           1,
		FailedServer:           1,
		Indexed:                N - failed,
		TooManyRequests:        1,
		AvailableBulkRequests:  10,
		BytesTotal:             bytesTotal,
		BytesUncompressedTotal: bytesUncompressed,
	}, stats)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(context.Background(), &rm))
	var asserted atomic.Int64
	assertCounter := docappendertest.NewAssertCounter(t, &asserted)

	var processedAsserted int
	assertProcessedCounter := func(metric metricdata.Metrics, attrs attribute.Set) {
		asserted.Add(1)
		counter := metric.Data.(metricdata.Sum[int64])
		for _, dp := range counter.DataPoints {
			metricdatatest.AssertHasAttributes[metricdata.DataPoint[int64]](t, dp, attrs.ToSlice()...)
			status, exist := dp.Attributes.Value(attribute.Key("status"))
			assert.True(t, exist)
			switch status.AsString() {
			case "Success":
				processedAsserted++
				assert.Equal(t, stats.Indexed, dp.Value)
			case "FailedClient":
				processedAsserted++
				assert.Equal(t, stats.FailedClient, dp.Value)
			case "FailedServer":
				processedAsserted++
				assert.Equal(t, stats.FailedServer, dp.Value)
			case "TooMany":
				processedAsserted++
				assert.Equal(t, stats.TooManyRequests, dp.Value)
			default:
				assert.FailNow(t, "Unexpected metric with status: "+status.AsString())
			}
		}
	}
	// check the set of names and then check the counter or histogram
	unexpectedMetrics := []string{}
	docappendertest.AssertOTelMetrics(t, rm.ScopeMetrics[0].Metrics, func(m metricdata.Metrics) {
		switch m.Name {
		case "elasticsearch.events.count":
			assertCounter(m, stats.Added, indexerAttrs)
		case "elasticsearch.events.queued":
			assertCounter(m, stats.Active, indexerAttrs)
		case "elasticsearch.bulk_requests.count":
			assertCounter(m, stats.BulkRequests, indexerAttrs)
		case "elasticsearch.events.processed":
			assertProcessedCounter(m, indexerAttrs)
		case "elasticsearch.bulk_requests.available":
			assertCounter(m, stats.AvailableBulkRequests, indexerAttrs)
		case "elasticsearch.flushed.bytes":
			assertCounter(m, stats.BytesTotal, indexerAttrs)
		case "elasticsearch.flushed.uncompressed.bytes":
			assertCounter(m, stats.BytesUncompressedTotal, indexerAttrs)
		case "elasticsearch.buffer.latency", "elasticsearch.flushed.latency":
			// expect this metric name but no assertions done
			// as it's histogram and it's checked elsewhere
		default:
			unexpectedMetrics = append(unexpectedMetrics, m.Name)
		}
	})

	assert.Empty(t, unexpectedMetrics)
	assert.Equal(t, int64(7), asserted.Load())
	assert.Equal(t, 4, processedAsserted)
}

func TestAppenderRetry(t *testing.T) {
	var bytesTotal int64
	var bytesUncompressed int64
	var first atomic.Bool
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result, stat := docappendertest.DecodeBulkRequestWithStats(r)
		bytesUncompressed += stat.UncompressedBytes
		if first.CompareAndSwap(false, true) {
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
		FlushInterval:      time.Minute,
		FlushBytes:         750, // this is enough to flush after 9 documents
		MaxRequests:        1,   // to ensure the test is stable
		MaxDocumentRetries: 1,   // to test the document retry logic
		MeterProvider:      sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr)),
		MetricAttributes:   indexerAttrs,
	})

	require.NoError(t, err)
	defer indexer.Close(context.Background())

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
			if indexer.Stats().BulkRequests == 1 {
				break loop
			}
		case <-timeout:
			t.Fatalf("timed out waiting for the active bulk indexer to send one bulk request")
		}
	}

	stats := indexer.Stats()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(context.Background(), &rm))
	var asserted atomic.Int64
	assertCounter := docappendertest.NewAssertCounter(t, &asserted)

	var processedAsserted int
	assertProcessedCounter := func(metric metricdata.Metrics, attrs attribute.Set) {
		asserted.Add(1)
		counter := metric.Data.(metricdata.Sum[int64])
		for _, dp := range counter.DataPoints {
			metricdatatest.AssertHasAttributes(t, dp, attrs.ToSlice()...)
			status, exist := dp.Attributes.Value(attribute.Key("status"))
			assert.True(t, exist)
			switch status.AsString() {
			case "Success":
				processedAsserted++
				assert.Equal(t, stats.Indexed, dp.Value)
			case "FailedClient":
				processedAsserted++
				assert.Equal(t, stats.FailedClient, dp.Value)
			case "FailedServer":
				processedAsserted++
				assert.Equal(t, stats.FailedServer, dp.Value)
			case "TooMany":
				processedAsserted++
				assert.Equal(t, stats.TooManyRequests, dp.Value)
			default:
				assert.FailNow(t, "Unexpected metric with status: "+status.AsString())
			}
		}
	}
	// check the set of names and then check the counter or histogram
	unexpectedMetrics := []string{}
	docappendertest.AssertOTelMetrics(t, rm.ScopeMetrics[0].Metrics, func(m metricdata.Metrics) {
		switch m.Name {
		case "elasticsearch.events.count":
			assertCounter(m, stats.Added, indexerAttrs)
		case "elasticsearch.events.queued":
			assertCounter(m, stats.Active, indexerAttrs)
		case "elasticsearch.bulk_requests.count":
			assertCounter(m, stats.BulkRequests, indexerAttrs)
		case "elasticsearch.events.processed":
			assertProcessedCounter(m, indexerAttrs)
		case "elasticsearch.events.retried":
			assertCounter(m, 1, attribute.NewSet(
				attribute.String("a", "b"),
				attribute.String("c", "d"),
				attribute.Int("greatest_retry", 1),
			))
		case "elasticsearch.bulk_requests.available":
			assertCounter(m, stats.AvailableBulkRequests, indexerAttrs)
		case "elasticsearch.flushed.bytes":
			assertCounter(m, stats.BytesTotal, indexerAttrs)
		case "elasticsearch.flushed.uncompressed.bytes":
			assertCounter(m, stats.BytesUncompressedTotal, indexerAttrs)
		case "elasticsearch.buffer.latency", "elasticsearch.flushed.latency":
			// expect this metric name but no assertions done
			// as it's histogram and it's checked elsewhere
		default:
			unexpectedMetrics = append(unexpectedMetrics, m.Name)
		}
	})

	assert.Empty(t, unexpectedMetrics)
	assert.Equal(t, int64(8), asserted.Load())
	assert.Equal(t, 3, processedAsserted)

	// Closing the indexer flushes enqueued documents.
	err = indexer.Close(context.Background())
	require.NoError(t, err)
	stats = indexer.Stats()
	failed := int64(2)
	assert.Equal(t, docappender.Stats{
		Added:                  N,
		Active:                 0,
		BulkRequests:           2,
		Failed:                 failed,
		FailedClient:           1,
		FailedServer:           1,
		Indexed:                N - failed,
		TooManyRequests:        0,
		AvailableBulkRequests:  1,
		BytesTotal:             bytesTotal,
		BytesUncompressedTotal: bytesUncompressed,
	}, stats)
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
	stats.BytesTotal = 0             // Asserted elsewhere.
	stats.BytesUncompressedTotal = 0 // Asserted elsewhere.
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
	var bytesUncompressedTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result, stat := docappendertest.DecodeBulkRequestWithStats(r)
		bytesUncompressedTotal += stat.UncompressedBytes
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
		Added:                  1,
		Active:                 0,
		BulkRequests:           1,
		Failed:                 0,
		Indexed:                1,
		TooManyRequests:        0,
		AvailableBulkRequests:  10,
		BytesTotal:             bytesTotal,
		BytesUncompressedTotal: bytesUncompressedTotal,
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

func TestAppenderFlushTimeout(t *testing.T) {
	done := make(chan struct{}, 1)
	defer close(done)

	client := docappendertest.NewMockElasticsearchClient(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-done:
		}
	})
	rdr := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		},
	))
	indexer, err := docappender.New(client, docappender.Config{
		// Default flush bytes is 5MB
		FlushBytes:    1,
		FlushTimeout:  50 * time.Millisecond,
		MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr)),
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addMinimalDoc(t, indexer, "logs-foo-testing")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	indexer.Close(ctx)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(context.Background(), &rm))

	var asserted atomic.Int64
	assertCounter := docappendertest.NewAssertCounter(t, &asserted)
	docappendertest.AssertOTelMetrics(t, rm.ScopeMetrics[0].Metrics, func(m metricdata.Metrics) {
		switch m.Name {
		case "elasticsearch.events.processed":
			assertCounter(m, 1, attribute.NewSet(attribute.String("status", "Timeout")))
		}
	})
	assert.Equal(t, int64(1), asserted.Load())
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

	docs := 12
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
				assert.GreaterOrEqual(t, int(dp.Count), count)
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

func TestAppenderFlushRequestError(t *testing.T) {
	// This test ensures that the appender correctly categorizes and quantifies
	// failed requests with different failure scenarios. Since a bulk request
	// contains N documents, the appender should increment the categorized
	// failure by the same number of documents in the request.
	test := func(t *testing.T, sc int, errMsg string) {
		var bytesTotal int64
		var bytesUncompressedTotal int64
		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			bytesTotal += r.ContentLength
			_, _, stat := docappendertest.DecodeBulkRequestWithStats(r)
			bytesUncompressedTotal += stat.UncompressedBytes
			w.WriteHeader(sc)
			w.Write([]byte(`{"error": {"root_cause": [{"type": "x_content_parse_exception","reason": "reason"}],"type": "x_content_parse_exception","reason": "reason","caused_by": {"type": "json_parse_exception","reason": "reason"}},"status": 400}`))
		})

		rdr := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
			func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
				return metricdata.DeltaTemporality
			},
		))

		indexer, err := docappender.New(client, docappender.Config{
			FlushInterval: time.Minute,
			MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr)),
		})
		require.NoError(t, err)
		defer indexer.Close(context.Background())

		// Send 3 docs, ensure that metrics are always in the unit of documents, not requests.
		docs := 3
		for i := 0; i < docs; i++ {
			addMinimalDoc(t, indexer, "logs-foo-testing")
		}

		// Closing the indexer flushes enqueued documents.
		err = indexer.Close(context.Background())
		require.EqualError(t, err, errMsg)
		stats := indexer.Stats()
		wantStats := docappender.Stats{
			Added:                  int64(docs),
			Active:                 0,
			BulkRequests:           1, // Single bulk request
			Failed:                 int64(docs),
			AvailableBulkRequests:  10,
			BytesTotal:             bytesTotal,
			BytesUncompressedTotal: bytesUncompressedTotal,
		}
		var status string
		switch {
		case sc == 429:
			status = "TooMany"
			wantStats.TooManyRequests = int64(docs)
		case sc >= 500:
			status = "FailedServer"
			wantStats.FailedServer = int64(docs)
		case sc >= 400 && sc != 429:
			status = "FailedClient"
			wantStats.FailedClient = int64(docs)
		}
		assert.Equal(t, wantStats, stats)

		var rm metricdata.ResourceMetrics
		assert.NoError(t, rdr.Collect(context.Background(), &rm))

		var asserted atomic.Int64
		assertCounter := docappendertest.NewAssertCounter(t, &asserted)
		docappendertest.AssertOTelMetrics(t, rm.ScopeMetrics[0].Metrics, func(m metricdata.Metrics) {
			switch m.Name {
			case "elasticsearch.events.processed":
				assertCounter(m, 3, attribute.NewSet(attribute.String("status", status), semconv.HTTPResponseStatusCode(sc)))
			}
		})
		assert.Equal(t, int64(1), asserted.Load())

	}
	t.Run("400", func(t *testing.T) {
		test(t, http.StatusBadRequest, "flush failed (400): {\"error\":{\"type\":\"x_content_parse_exception\",\"caused_by\":{\"type\":\"json_parse_exception\"}}}")
	})
	t.Run("403", func(t *testing.T) {
		test(t, http.StatusForbidden, "flush failed (403): {\"error\":{\"type\":\"x_content_parse_exception\",\"caused_by\":{\"type\":\"json_parse_exception\"}}}")
	})
	t.Run("429", func(t *testing.T) {
		test(t, http.StatusTooManyRequests, "flush failed (429): {\"error\":{\"type\":\"x_content_parse_exception\",\"caused_by\":{\"type\":\"json_parse_exception\"}}}")
	})
	t.Run("500", func(t *testing.T) {
		test(t, http.StatusInternalServerError, "flush failed (500): {\"error\":{\"type\":\"x_content_parse_exception\",\"caused_by\":{\"type\":\"json_parse_exception\"}}}")
	})
	t.Run("503", func(t *testing.T) {
		test(t, http.StatusServiceUnavailable, "flush failed (503): {\"error\":{\"type\":\"x_content_parse_exception\",\"caused_by\":{\"type\":\"json_parse_exception\"}}}")
	})
	t.Run("504", func(t *testing.T) {
		test(t, http.StatusGatewayTimeout, "flush failed (504): {\"error\":{\"type\":\"x_content_parse_exception\",\"caused_by\":{\"type\":\"json_parse_exception\"}}}")
	})
}

func TestAppenderIndexFailedLogging(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		for i, item := range result.Items {
			itemResp := item["create"]
			itemResp.Index = "an_index"
			switch i % 5 {
			case 0:
				itemResp.Error.Type = "error_type"
				itemResp.Error.Reason = "error_reason_even. Preview of field's value: 'abc def ghi'"
			case 1:
				itemResp.Error.Type = "error_type"
				itemResp.Error.Reason = "error_reason_odd. Preview of field's value: some field value"
			case 2:
				itemResp.Error.Type = "unavailable_shards_exception"
				itemResp.Error.Reason = "this reason should not be logged"
			case 3:
				itemResp.Error.Type = "x_content_parse_exception"
				itemResp.Error.Reason = "this reason should not be logged"
			case 4:
				itemResp.Error.Type = "document_parsing_exception"
				itemResp.Error.Reason = "this reason should not be logged"
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

	const N = 5 * 2
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}
	err = indexer.Close(context.Background())
	assert.NoError(t, err)

	entries := observed.FilterMessageSnippet("failed to index").TakeAll()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Message < entries[j].Message
	})
	require.Len(t, entries, N/2)
	assert.Equal(t, "failed to index documents in 'an_index' (document_parsing_exception): ", entries[0].Message)
	assert.Equal(t, int64(2), entries[0].Context[0].Integer)
	assert.Equal(t, "failed to index documents in 'an_index' (error_type): error_reason_even", entries[1].Message)
	assert.Equal(t, int64(2), entries[1].Context[0].Integer)
	assert.Equal(t, "failed to index documents in 'an_index' (error_type): error_reason_odd", entries[2].Message)
	assert.Equal(t, int64(2), entries[2].Context[0].Integer)
	assert.Equal(t, "failed to index documents in 'an_index' (unavailable_shards_exception): ", entries[3].Message)
	assert.Equal(t, int64(2), entries[3].Context[0].Integer)
	assert.Equal(t, "failed to index documents in 'an_index' (x_content_parse_exception): ", entries[4].Message)
	assert.Equal(t, int64(2), entries[4].Context[0].Integer)
}

func TestAppenderRetryLimit(t *testing.T) {
	var failedCount atomic.Int32
	var done atomic.Bool
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		switch failedCount.Add(1) {
		case 1:
			// Fail logs-foo-testing1 to ensure retries are working
			require.Len(t, result.Items, 2)
			for i, item := range result.Items {
				if i == 1 {
					itemResp := item["create"]
					itemResp.Status = http.StatusTooManyRequests
					item["create"] = itemResp
				}
			}
			json.NewEncoder(w).Encode(result)
			return
		case 2:
			// Fail logs-foo-testing2 to test the max retries setting
			require.Len(t, result.Items, 2, result.Items)
			assert.Equal(t, "logs-foo-testing1", result.Items[0]["create"].Index)
			assert.Equal(t, "logs-foo-testing2", result.Items[1]["create"].Index)

			for i, item := range result.Items {
				if i == 1 {
					itemResp := item["create"]
					itemResp.Status = http.StatusTooManyRequests
					item["create"] = itemResp
				}
			}
			json.NewEncoder(w).Encode(result)
			return
		case 3:
			// Fail all events
			// logs-foo-testing2 should not be retried because above the max setting
			// logs-foo-testing3 should be retried normally in the next request
			require.Len(t, result.Items, 2)
			assert.Equal(t, "logs-foo-testing2", result.Items[0]["create"].Index)
			assert.Equal(t, "logs-foo-testing3", result.Items[1]["create"].Index)

			for _, item := range result.Items {
				itemResp := item["create"]
				itemResp.Status = http.StatusTooManyRequests
				item["create"] = itemResp
			}
			json.NewEncoder(w).Encode(result)
			return
		}
		// logs-foo-testing2 dropped
		// logs-foo-testing3 retried
		// logs-foo-testing4 new event
		require.Len(t, result.Items, 2)
		assert.Equal(t, "logs-foo-testing3", result.Items[0]["create"].Index)
		assert.Equal(t, "logs-foo-testing4", result.Items[1]["create"].Index)
		json.NewEncoder(w).Encode(result)
		done.Store(true)
	})

	indexer, err := docappender.New(client, docappender.Config{
		MaxRequests:        1,
		MaxDocumentRetries: 1,
		FlushInterval:      100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addDoc(t, indexer, "logs-foo-testing0")
	addDoc(t, indexer, "logs-foo-testing1")

	require.Eventually(t, func() bool {
		return failedCount.Load() == 1
	}, 2*time.Second, 50*time.Millisecond, "timed out waiting for first flush request to fail")

	addMinimalDoc(t, indexer, "logs-foo-testing2")

	require.Eventually(t, func() bool {
		return failedCount.Load() == 2
	}, 2*time.Second, 50*time.Millisecond, "timed out waiting for second flush request to fail")

	addMinimalDoc(t, indexer, "logs-foo-testing3")

	require.Eventually(t, func() bool {
		return failedCount.Load() == 3
	}, 2*time.Second, 50*time.Millisecond, "timed out waiting for third flush request to fail")

	addMinimalDoc(t, indexer, "logs-foo-testing4")

	require.Eventually(t, func() bool {
		return done.Load()
	}, 2*time.Second, 50*time.Millisecond, "timed out waiting for last flush request to finish")

	err = indexer.Close(context.Background())
	assert.NoError(t, err)
}

func TestAppenderRetryFlushOnClose(t *testing.T) {
	var failedCount atomic.Int32
	var done atomic.Bool
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		switch failedCount.Add(1) {
		case 1:
			// Fail logs-foo-testing1 to ensure retries are working
			require.Len(t, result.Items, 2)
			for i, item := range result.Items {
				if i == 1 {
					itemResp := item["create"]
					itemResp.Status = http.StatusTooManyRequests
					item["create"] = itemResp
				}
			}
			json.NewEncoder(w).Encode(result)
			return
		}
		require.Len(t, result.Items, 1)
		assert.Equal(t, "logs-foo-testing1", result.Items[0]["create"].Index)
		json.NewEncoder(w).Encode(result)
		done.Store(true)
	})

	indexer, err := docappender.New(client, docappender.Config{
		MaxRequests:        10,
		MaxDocumentRetries: 1,
		FlushInterval:      100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	addDoc(t, indexer, "logs-foo-testing0")
	addDoc(t, indexer, "logs-foo-testing1")

	require.Eventually(t, func() bool {
		return failedCount.Load() == 1
	}, 2*time.Second, 50*time.Millisecond, "timed out waiting for first flush request to fail")

	assert.NoError(t, indexer.Close(context.Background()))

	require.Eventually(t, func() bool {
		return done.Load()
	}, 2*time.Second, 50*time.Millisecond, "timed out waiting for last flush request to finish")
}

func TestAppenderRetryDocument(t *testing.T) {
	testCases := map[string]struct {
		cfg docappender.Config
	}{
		"nocompression": {
			cfg: docappender.Config{
				MaxRequests:        1,
				MaxDocumentRetries: 100,
				FlushInterval:      100 * time.Millisecond,
			},
		},
		"gzip": {
			cfg: docappender.Config{
				MaxRequests:        1,
				MaxDocumentRetries: 100,
				FlushInterval:      100 * time.Millisecond,
				CompressionLevel:   gzip.BestCompression,
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			rdr := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
				func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
					return metricdata.DeltaTemporality
				},
			))
			var rm metricdata.ResourceMetrics

			var failedCount atomic.Int32
			var done atomic.Bool
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result := docappendertest.DecodeBulkRequest(r)
				switch failedCount.Add(1) {
				case 1:
					require.Len(t, result.Items, 10)
					for i, item := range result.Items {
						switch i {
						case 0, 4, 5, 6, 9:
							itemResp := item["create"]
							itemResp.Status = http.StatusTooManyRequests
							item["create"] = itemResp
						}
					}
					json.NewEncoder(w).Encode(result)
					return
				case 2:
					require.Len(t, result.Items, 7)
					assert.Equal(t, "logs-foo-testing0", result.Items[0]["create"].Index)
					assert.Equal(t, "logs-foo-testing4", result.Items[1]["create"].Index)
					assert.Equal(t, "logs-foo-testing5", result.Items[2]["create"].Index)
					assert.Equal(t, "logs-foo-testing6", result.Items[3]["create"].Index)
					assert.Equal(t, "logs-foo-testing9", result.Items[4]["create"].Index)
					assert.Equal(t, "logs-foo-testing10", result.Items[5]["create"].Index)
					assert.Equal(t, "logs-foo-testing11", result.Items[6]["create"].Index)

					for i, item := range result.Items {
						switch i {
						case 0, 1, 3, 5, 6:
							itemResp := item["create"]
							itemResp.Status = http.StatusTooManyRequests
							item["create"] = itemResp
						}
					}
					json.NewEncoder(w).Encode(result)
					return

				}
				require.Len(t, result.Items, 6)
				assert.Equal(t, "logs-foo-testing0", result.Items[0]["create"].Index)
				assert.Equal(t, "logs-foo-testing4", result.Items[1]["create"].Index)
				assert.Equal(t, "logs-foo-testing6", result.Items[2]["create"].Index)
				assert.Equal(t, "logs-foo-testing10", result.Items[3]["create"].Index)
				assert.Equal(t, "logs-foo-testing11", result.Items[4]["create"].Index)
				assert.Equal(t, "logs-foo-testing12", result.Items[5]["create"].Index)
				json.NewEncoder(w).Encode(result)
				done.Store(true)
			})

			tc.cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))
			indexer, err := docappender.New(client, tc.cfg)
			require.NoError(t, err)
			defer indexer.Close(context.Background())

			const N = 10
			for i := 0; i < N; i++ {
				addDoc(t, indexer, "logs-foo-testing"+strconv.Itoa(i))
			}

			require.Eventually(t, func() bool {
				return failedCount.Load() == 1
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for first flush request to fail")

			assert.NoError(t, rdr.Collect(context.Background(), &rm))

			var asserted atomic.Int64
			assertCounter := docappendertest.NewAssertCounter(t, &asserted)
			docappendertest.AssertOTelMetrics(t, rm.ScopeMetrics[0].Metrics, func(m metricdata.Metrics) {
				switch m.Name {
				case "elasticsearch.events.retried":
					assertCounter(m, 5, attribute.NewSet(
						attribute.Int("greatest_retry", 1),
					))
				}
			})
			assert.Equal(t, int64(1), asserted.Load())

			addMinimalDoc(t, indexer, "logs-foo-testing10")
			addMinimalDoc(t, indexer, "logs-foo-testing11")

			require.Eventually(t, func() bool {
				return failedCount.Load() == 2
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for first flush request to fail")

			assert.NoError(t, rdr.Collect(context.Background(), &rm))

			assertCounter = docappendertest.NewAssertCounter(t, &asserted)
			docappendertest.AssertOTelMetrics(t, rm.ScopeMetrics[0].Metrics, func(m metricdata.Metrics) {
				switch m.Name {
				case "elasticsearch.events.retried":
					assertCounter(m, 5, attribute.NewSet(
						attribute.Int("greatest_retry", 2),
					))
				}
			})
			assert.Equal(t, int64(2), asserted.Load())

			addMinimalDoc(t, indexer, "logs-foo-testing12")

			require.Eventually(t, func() bool {
				return done.Load()
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for second flush request to finish")

			err = indexer.Close(context.Background())
			assert.NoError(t, err)
		})
	}
}

func TestAppenderRetryDocument_RetryOnDocumentStatus(t *testing.T) {
	testCases := map[string]struct {
		status                int
		expectedDocsInRequest []int // at index i stores the number of documents in the i-th request
		cfg                   docappender.Config
	}{
		"should retry": {
			status:                500,
			expectedDocsInRequest: []int{1, 2, 1}, // 3rd request is triggered by indexer close
			cfg: docappender.Config{
				MaxRequests:           1,
				MaxDocumentRetries:    1,
				FlushInterval:         100 * time.Millisecond,
				RetryOnDocumentStatus: []int{429, 500},
			},
		},
		"should not retry": {
			status:                500,
			expectedDocsInRequest: []int{1, 1},
			cfg: docappender.Config{
				MaxRequests:           1,
				MaxDocumentRetries:    1,
				FlushInterval:         100 * time.Millisecond,
				RetryOnDocumentStatus: []int{429},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var failedCount atomic.Int32
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result := docappendertest.DecodeBulkRequest(r)
				attempt := failedCount.Add(1) - 1
				require.Len(t, result.Items, tc.expectedDocsInRequest[attempt])
				for _, item := range result.Items {
					itemResp := item["create"]
					itemResp.Status = tc.status
					item["create"] = itemResp
				}
				json.NewEncoder(w).Encode(result)
			})

			indexer, err := docappender.New(client, tc.cfg)
			require.NoError(t, err)
			defer indexer.Close(context.Background())

			addMinimalDoc(t, indexer, "logs-foo-testing1")

			require.Eventually(t, func() bool {
				return failedCount.Load() == 1
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for first flush request to fail")

			addMinimalDoc(t, indexer, "logs-foo-testing2")

			require.Eventually(t, func() bool {
				return failedCount.Load() == 2
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for first flush request to fail")

			err = indexer.Close(context.Background())
			assert.NoError(t, err)
		})
	}
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
		added <- indexer.Add(addContext, "logs-foo-testing", readerFunc(func(w io.Writer) (int64, error) {
			close(readInvoked)
			n, err := w.Write([]byte("{}"))
			return int64(n), err
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

type readerFunc func(io.Writer) (n int64, err error)

func (f readerFunc) WriteTo(p io.Writer) (n int64, err error) {
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
	var bytesUncompressedTotal int64
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		bytesTotal += r.ContentLength
		_, result, stat := docappendertest.DecodeBulkRequestWithStats(r)
		bytesUncompressedTotal = stat.UncompressedBytes
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
		Added:                  N,
		Indexed:                N,
		BulkRequests:           1,
		BytesTotal:             bytesTotal,
		BytesUncompressedTotal: bytesUncompressedTotal,
		AvailableBulkRequests:  10,
		IndexersActive:         0}, indexer.Stats())
}

func TestAppenderPipeline(t *testing.T) {
	const expected = "my_pipeline"
	var actual string
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		actual = r.URL.Query().Get("pipeline")
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval: time.Minute,
		Pipeline:      expected,
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

	assert.Equal(t, expected, actual)
}

func TestAppenderRequireDataStream(t *testing.T) {
	const expected = "true"
	var actual string
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		actual = r.URL.Query().Get("require_data_stream")
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval:     time.Minute,
		RequireDataStream: true,
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

	assert.Equal(t, expected, actual)
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
		stats.BytesUncompressedTotal = 0
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
		stats.BytesUncompressedTotal = 0
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
		stats.BytesUncompressedTotal = 0
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
	err := indexer.Add(context.Background(), index, newJSONReader(map[string]any{
		"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
	}))
	require.NoError(t, err)
}

func addDoc(t testing.TB, indexer *docappender.Appender, index string) {
	err := indexer.Add(context.Background(), index, newJSONReader(map[string]any{
		"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		"agent":      map[string]any{"name": strings.Repeat("foobar", 1000)},
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

func TestAppenderOtelTracing(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		testTracedAppend(t, 200, sdktrace.Status{
			Code:        codes.Ok,
			Description: "",
		})
	})
	t.Run("failure", func(t *testing.T) {
		testTracedAppend(t, 400, sdktrace.Status{
			Code:        codes.Error,
			Description: "bulk indexing request failed",
		})
	})
}

func testTracedAppend(t *testing.T, responseCode int, status sdktrace.Status) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(responseCode)
		_, result := docappendertest.DecodeBulkRequest(r)
		json.NewEncoder(w).Encode(result)
	})

	core, observed := observer.New(zap.NewAtomicLevelAt(zapcore.DebugLevel))

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	indexer, err := docappender.New(client, docappender.Config{
		FlushInterval: time.Minute,
		Logger:        zap.New(core),
		// NOTE: Tracer must be nil to use otel tracing
		Tracer:         nil,
		TracerProvider: tp,
	})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	const N = 100
	for i := 0; i < N; i++ {
		addMinimalDoc(t, indexer, "logs-foo-testing")
	}

	// Closing the indexer flushes enqueued documents.
	_ = indexer.Close(context.Background())

	spans := exp.GetSpans()
	assert.NotEmpty(t, spans)

	gotSpan := spans[0]
	assert.Equal(t, "docappender.flush", gotSpan.Name)
	assert.Equal(t, status, gotSpan.Status)

	for _, a := range gotSpan.Attributes {
		if a.Key == "documents" {
			assert.Equal(t, int64(N), a.Value.AsInt64())
		}
	}

	correlatedLogs := observed.FilterFieldKey("traceId").All()
	assert.NotEmpty(t, correlatedLogs)

	log := correlatedLogs[0]
	expectedTraceID := gotSpan.SpanContext.TraceID().String()
	assert.Equal(t, expectedTraceID, log.ContextMap()["traceId"])
	expectedSpanID := gotSpan.SpanContext.SpanID().String()
	assert.Equal(t, expectedSpanID, log.ContextMap()["spanId"])
}
