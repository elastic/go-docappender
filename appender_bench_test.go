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
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/fastjson"
	"go.uber.org/zap"

	"github.com/elastic/go-docappender"
	"github.com/elastic/go-docappender/docappendertest"
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
	client := docappendertest.NewMockElasticsearchClient(b, func(w http.ResponseWriter, r *http.Request) {
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
		atomic.AddInt64(&indexed, n)
	})
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

func newDocumentBody() *bytes.Reader {
	return newJSONReader(map[string]any{
		"@timestamp":            time.Now().Format(docappendertest.TimestampFormat),
		"data_stream.type":      "logs",
		"data_stream.dataset":   "foo",
		"data_stream.namespace": "testing",
	})
}
