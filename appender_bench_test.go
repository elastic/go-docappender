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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/fastjson"
	"go.uber.org/zap"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-docappender/v2"
	"github.com/elastic/go-docappender/v2/docappendertest"
)

func BenchmarkAppender(b *testing.B) {
	b.Run("NoCompression", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.NoCompression,
			Scaling:          docappender.ScalingConfig{Disabled: true},
		})
	})
	b.Run("NoCompressionScaling", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.NoCompression,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 1, // Scale immediately
					CoolDown:  1,
				},
			},
		})
	})
	b.Run("BestSpeed", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.BestSpeed,
			Scaling:          docappender.ScalingConfig{Disabled: true},
		})
	})
	b.Run("BestSpeedScaling", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.BestSpeed,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 1, // Scale immediately
					CoolDown:  1,
				},
			},
		})
	})
	b.Run("DefaultCompression", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.DefaultCompression,
			Scaling:          docappender.ScalingConfig{Disabled: true},
		})
	})
	b.Run("DefaultCompressionScaling", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.DefaultCompression,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 1, // Scale immediately
					CoolDown:  1,
				},
			},
		})
	})
	b.Run("BestCompression", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.BestCompression,
			Scaling:          docappender.ScalingConfig{Disabled: true},
		})
	})
	b.Run("BestCompressionScaling", func(b *testing.B) {
		benchmarkAppender(b, docappender.Config{
			CompressionLevel: gzip.BestCompression,
			Scaling: docappender.ScalingConfig{
				ScaleUp: docappender.ScaleActionConfig{
					Threshold: 1, // Scale immediately
					CoolDown:  1,
				},
			},
		})
	})
}

func BenchmarkAppenderError(b *testing.B) {
	client := docappendertest.NewMockElasticsearchClient(b, func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, result := docappendertest.DecodeBulkRequest(r)
		for i, item := range result.Items {
			itemResp := item["create"]
			itemResp.Index = "an_index"
			itemResp.Error.Type = "error_type"
			if i%2 == 0 {
				itemResp.Error.Reason = "error_reason_even. Preview of field's value: 'abc def ghi'"
			} else {
				itemResp.Error.Reason = "error_reason_odd. Preview of field's value: some field value"
			}
			item["create"] = itemResp
		}
		result.HasErrors = true
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.New(client, docappender.Config{
		Logger: zap.NewNop(),
	})
	require.NoError(b, err)
	b.Cleanup(func() { indexer.Close(context.Background()) })

	documentBody := newDocumentBody()
	b.SetBytes(int64(documentBody.Len())) // bytes processed each iteration

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			documentBodyCopy := *documentBody
			if err := indexer.Add(ctx, "logs-foo-testing", &documentBodyCopy); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Closing the indexer flushes enqueued events.
	if err := indexer.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func benchmarkAppender(b *testing.B, cfg docappender.Config) {
	var indexed int64
	client := newESClient(b, &indexed)
	cfg.FlushInterval = time.Second
	indexer, err := docappender.New(client, cfg)
	require.NoError(b, err)
	defer indexer.Close(context.Background())

	documentBody := newDocumentBody()
	b.SetBytes(int64(documentBody.Len())) // bytes processed each iteration

	// We can't pass context.Background() to the indexer because it's using
	// a nil channel under the hood, speeding up the select statement.
	// That creates misleading benchmark results as they do not reflect
	// production usage.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			documentBodyCopy := *documentBody
			if err := indexer.Add(ctx, "logs-foo-testing", &documentBodyCopy); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Closing the indexer flushes enqueued events.
	if err := indexer.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	assert.Equal(b, int64(b.N), indexed)
}

func BenchmarkAppenderPool(b *testing.B) {
	// Run benchmarks with different pool configurations
	benchConfigs := []struct {
		name           string
		appenderCount  int
		guaranteed     int
		maxPerID       int
		totalPool      int
		docBufferSize  int
		flushThreshold int
	}{
		{
			name:           "SmallPool_2Appenders",
			appenderCount:  2,
			guaranteed:     1,
			maxPerID:       2,
			totalPool:      5,
			docBufferSize:  1000,
			flushThreshold: 256 * 1024,
		},
		{
			name:           "MediumPool_4Appenders",
			appenderCount:  4,
			guaranteed:     1,
			maxPerID:       3,
			totalPool:      10,
			docBufferSize:  1000,
			flushThreshold: 256 * 1024,
		},
		{
			name:           "LargePool_8Appenders",
			appenderCount:  8,
			guaranteed:     2,
			maxPerID:       4,
			totalPool:      20,
			docBufferSize:  1000,
			flushThreshold: 256 * 1024,
		},
		{
			name:           "HighContention_8Appenders",
			appenderCount:  8,
			guaranteed:     1,
			maxPerID:       2,
			totalPool:      10,
			docBufferSize:  1000,
			flushThreshold: 256 * 1024,
		},
		{
			name:           "HighContention_20Appenders",
			appenderCount:  20,
			guaranteed:     4,
			maxPerID:       10,
			totalPool:      50,
			docBufferSize:  1000,
			flushThreshold: 256 * 1024,
		},
		{
			name:           "ExtraLargePool_50Appenders",
			appenderCount:  40,
			guaranteed:     10,
			maxPerID:       20,
			totalPool:      100,
			docBufferSize:  1000,
			flushThreshold: 256 * 1024,
		},
	}

	for _, cfg := range benchConfigs {
		b.Run(cfg.name, func(b *testing.B) {
			benchmarkAppenderPool(b,
				cfg.appenderCount,
				cfg.guaranteed,
				cfg.maxPerID,
				cfg.totalPool,
				cfg.docBufferSize,
				cfg.flushThreshold,
			)
		})
	}
}

func benchmarkAppenderPool(b *testing.B, appenderCount, guaranteed, maxPerID, totalPool, docBufferSize, flushThreshold int) {
	documentBody := newDocumentBody()
	b.SetBytes(int64(documentBody.Len() * appenderCount)) // bytes processed each iteration across all appenders

	// Create a shared mock client
	var indexed int64
	client := newESClient(b, &indexed)

	// Create a shared BulkIndexerPool
	bulkIndexerConfig := docappender.BulkIndexerConfig{
		CompressionLevel: gzip.BestSpeed,
		Client:           client,
	}
	pool := docappender.NewBulkIndexerPool(guaranteed, maxPerID, totalPool, bulkIndexerConfig)

	// Create multiple appenders sharing the pool
	appenders := make([]*docappender.Appender, appenderCount)
	for i := 0; i < appenderCount; i++ {
		cfg := docappender.Config{
			Scaling:            docappender.ScalingConfig{Disabled: true},
			FlushBytes:         flushThreshold,
			FlushInterval:      time.Second,
			DocumentBufferSize: docBufferSize,
			CompressionLevel:   gzip.BestSpeed,
			BulkIndexerPool:    pool,
			Logger:             zap.NewNop(),
		}

		appender, err := docappender.New(client, cfg)
		require.NoError(b, err)
		appenders[i] = appender
		defer appender.Close(context.Background())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		appenderIndex := rand.IntN(appenderCount) % appenderCount // Randomly select an appender to simulate multiple clients
		for pb.Next() {
			docCopy := *documentBody
			if err := appenders[appenderIndex].Add(ctx, "logs-foo-testing", &docCopy); err != nil {
				b.Fatal(err)
			}
			// Rotate through appenders to simulate multiple clients
			appenderIndex = (appenderIndex + 1) % appenderCount
		}
	})

	// Close all appenders to flush queued documents
	for _, appender := range appenders {
		if err := appender.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	}

	// Verify all documents were indexed
	assert.Equal(b, int64(b.N), indexed)
}

func newDocumentBody() *bytes.Reader {
	return newJSONReader(map[string]any{
		"@timestamp":            time.Now().Format(docappendertest.TimestampFormat),
		"data_stream.type":      "logs",
		"data_stream.dataset":   "foo",
		"data_stream.namespace": "testing",
	})
}

func newESClient(b testing.TB, indexed *int64) *elastictransport.Client {
	return docappendertest.NewMockElasticsearchClient(b, func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			r, err := gzip.NewReader(body)
			if err != nil {
				panic(err)
			}
			defer r.Close()
			body = r
		}

		var n int64
		var jsonw fastjson.Writer
		jsonw.RawString(`{"items":[`)
		first := true
		scanner := bufio.NewScanner(body)
		for scanner.Scan() {
			// Action is always "create", skip decoding to avoid
			// inflating allocations in benchmark.
			if !scanner.Scan() {
				panic("expected source")
			}
			if first {
				first = false
			} else {
				jsonw.RawByte(',')
			}
			jsonw.RawString(`{"create":{"status":201}}`)
			n++
		}
		require.NoError(b, scanner.Err())
		jsonw.RawString(`]}`)
		w.Write(jsonw.Bytes())
		atomic.AddInt64(indexed, n)
	})
}
