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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/go-docappender/v2"
	"github.com/elastic/go-docappender/v2/docappendertest"
)

func TestBulkIndexer(t *testing.T) {
	for _, tc := range []struct {
		Name             string
		CompressionLevel int
	}{
		{Name: "no_compression", CompressionLevel: gzip.NoCompression},
		{Name: "default_compression", CompressionLevel: gzip.DefaultCompression},
		{Name: "most_compression", CompressionLevel: gzip.BestCompression},
		{Name: "speed_compression", CompressionLevel: gzip.BestSpeed},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var esFailing atomic.Bool
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result := docappendertest.DecodeBulkRequest(r)
				if esFailing.Load() {
					for _, itemsMap := range result.Items {
						for k, item := range itemsMap {
							result.HasErrors = true
							item.Status = http.StatusTooManyRequests
							item.Error.Type = "simulated_es_error"
							item.Error.Reason = "for testing"
							itemsMap[k] = item
						}
					}
				}
				json.NewEncoder(w).Encode(result)
			})
			indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:                client,
				MaxDocumentRetries:    100_000, // infinite for testing purpose
				RetryOnDocumentStatus: []int{http.StatusTooManyRequests},
				CompressionLevel:      tc.CompressionLevel,
			})
			require.NoError(t, err)

			generateLoad := func(count int) {
				for i := 0; i < count; i++ {
					require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
						Index: "testidx",
						Body: newJSONReader(map[string]any{
							"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
						}),
					}))
				}
			}

			itemCount := 1_000
			generateLoad(itemCount)

			// All items should be successfully flushed
			uncompressed := indexer.UncompressedLen()
			uncompressedDocSize := uncompressed / itemCount
			stat, err := indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(itemCount), stat.Indexed)
			require.Equal(t, uncompressed, indexer.BytesUncompressedFlushed())

			// nothing is in the buffer if all succeeded
			require.Equal(t, 0, indexer.Len())
			require.Equal(t, 0, indexer.UncompressedLen())

			// Simulate ES failure, all items should be enqueued for retries
			esFailing.Store(true)
			generateLoad(itemCount)
			require.Equal(t, itemCount, indexer.Items())

			for i := 0; i < 10; i++ {
				stat, err := indexer.Flush(context.Background())
				require.NoError(t, err)
				require.Equal(t, int64(0), stat.Indexed)
				require.Len(t, stat.FailedDocs, 0)
				require.Equal(t, int64(itemCount), stat.RetriedDocs)

				// all the flushed bytes are now in the buffer again to be retried
				require.Equal(t, indexer.UncompressedLen(), indexer.BytesUncompressedFlushed())
				// Generate more load, all these items should be enqueued for retries
				generateLoad(10)
				itemCount += 10
				require.Equal(t, itemCount, indexer.Items())
				expectedBufferedSize := indexer.BytesUncompressedFlushed() + (10 * uncompressedDocSize)
				require.Equal(t, expectedBufferedSize, indexer.UncompressedLen())
			}

			uncompressedSize := indexer.UncompressedLen()
			// Recover ES and ensure all items are indexed
			esFailing.Store(false)
			stat, err = indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(itemCount), stat.Indexed)
			require.Equal(t, uncompressedSize, indexer.BytesUncompressedFlushed())
			// no documents to retry so buffer should be empty
			require.Equal(t, 0, indexer.Len())
			require.Equal(t, 0, indexer.UncompressedLen())
		})
	}
}

func TestDynamicTemplates(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result, _, dynamicTemplates := docappendertest.DecodeBulkRequestWithStatsAndDynamicTemplates(r)
		require.Equal(t, []map[string]string{
			{"one": "two", "three": "four"},
			{"five": "six", "seven": "eight"},
		}, dynamicTemplates)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client: client,
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index: "testidx",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
		DynamicTemplates: map[string]string{"one": "two", "three": "four"},
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index: "testidx",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
		DynamicTemplates: map[string]string{"five": "six", "seven": "eight"},
	})
	require.NoError(t, err)

	stat, err := indexer.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), stat.Indexed)
}

func TestPipeline(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result, _, _, pipelines := docappendertest.DecodeBulkRequestWithStatsAndDynamicTemplatesAndPipelines(r)
		err := json.NewEncoder(w).Encode(result)
		require.NoError(t, err)
		for _, p := range pipelines {
			require.Contains(t, p, "test-pipeline", "test-pipeline should have been present")
		}
		require.Equal(t, 2, len(pipelines), "2 pipelines should have been returned")
	})
	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client: client,
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index:    "testidx",
		Pipeline: "test-pipeline1",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index:    "testidx",
		Pipeline: "test-pipeline2",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	require.NoError(t, err)

	stat, err := indexer.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), stat.Indexed)
}

func TestAction(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		err := json.NewEncoder(w).Encode(result)
		require.NoError(t, err)

		actions := []string{}
		for _, itemsMap := range result.Items {
			for a := range itemsMap {
				actions = append(actions, a)
			}
		}

		require.Equal(t, []string{"create", "update", "delete"}, actions)
	})
	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client: client,
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index: "testidx",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index:  "testidx",
		Action: "update",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index:    "testidx",
		Action:   "delete",
		Pipeline: "test-pipeline2",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index:    "testidx",
		Action:   "foobar",
		Pipeline: "test-pipeline2",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	assert.Error(t, err)

	stat, err := indexer.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), stat.Indexed)
}

func TestItemRequireDataStream(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, meta, result, _ := docappendertest.DecodeBulkRequestWithStatsAndMeta(r)
		require.Len(t, meta, 2)
		assert.False(t, meta[0].RequireDataStream)
		assert.True(t, meta[1].RequireDataStream)
		json.NewEncoder(w).Encode(result)
	})
	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client: client,
	})
	require.NoError(t, err)

	for _, required := range []bool{false, true} {
		err := indexer.Add(docappender.BulkIndexerItem{
			Index:             strconv.FormatBool(required),
			Body:              strings.NewReader(`{}`),
			RequireDataStream: required,
		})
		require.NoError(t, err)
	}

	stat, err := indexer.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), stat.Indexed)
}

func TestPopulateFailedItemSource(t *testing.T) {
	for _, compressionLevel := range []int{0, 1} {
		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			_, result := docappendertest.DecodeBulkRequest(r)
			for _, itemsMap := range result.Items {
				for k, item := range itemsMap {
					if item.Index == "ok" {
						continue
					}
					result.HasErrors = true
					item.Status = http.StatusBadRequest
					item.Error.Type = "validation_error"
					item.Error.Reason = "for testing"
					itemsMap[k] = item
				}
			}
			json.NewEncoder(w).Encode(result)
		})
		t.Run(fmt.Sprintf("compression_level=%d", compressionLevel), func(t *testing.T) {
			indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:                   client,
				PopulateFailedItemSource: true,
			})
			require.NoError(t, err)

			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "foo",
				Body:  strings.NewReader(`{"1":"2"}`),
			})
			require.NoError(t, err)
			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "ok",
				Body:  strings.NewReader(`{"3":"4"}`),
			})
			require.NoError(t, err)
			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "bar",
				Body:  strings.NewReader(`{"5":"6"}`),
			})
			require.NoError(t, err)

			stat, err := indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Len(t, stat.FailedDocs, 2)
			assert.Equal(t, []docappender.BulkIndexerResponseItem{
				{
					Index:    "foo",
					Status:   http.StatusBadRequest,
					Position: 0,
					Error: struct {
						Type   string `json:"type"`
						Reason string `json:"reason"`
					}{Type: "validation_error", Reason: "for testing"},
					Source: `{"create":{"_index":"foo"}}
{"1":"2"}
`,
				},
				{
					Index:    "bar",
					Status:   http.StatusBadRequest,
					Position: 2,
					Error: struct {
						Type   string `json:"type"`
						Reason string `json:"reason"`
					}{Type: "validation_error", Reason: "for testing"},
					Source: `{"create":{"_index":"bar"}}
{"5":"6"}
`,
				},
			}, stat.FailedDocs)
		})
	}
}
