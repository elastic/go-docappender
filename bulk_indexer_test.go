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

type reqStat struct {
	headerEventCount         int64
	actualEventCount         int64
	headerUncompressedLength int64
	actualUncompressedLength int64
}

// collectTelemetryHeaders extracts telemetry headers from the HTTP request and
// fills the given `reqStat` struct with the header values.
// The actual bulk metrics remain untouched and need to be filled elsewhere.
func collectTelemetryHeaders(t *testing.T, r *http.Request, stats docappendertest.RequestStats) (out reqStat) {
	var err error

	// headers
	ec := r.Header.Get(docappender.HeaderEventCount)
	require.NotEmpty(t, ec, "Request must have the %s header", docappender.HeaderEventCount)
	out.headerEventCount, err = strconv.ParseInt(ec, 10, 64)
	require.NoError(t, err)

	ul := r.Header.Get(docappender.HeaderUncompressedLength)
	require.NotEmpty(t, ul, "Request must have the %s header", docappender.HeaderUncompressedLength)
	out.headerUncompressedLength, err = strconv.ParseInt(ul, 10, 64)
	require.NoError(t, err)

	// actual values
	out.actualEventCount = stats.EventCount
	out.actualUncompressedLength = stats.UncompressedBytes

	return out
}

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
			var (
				reqStats  []reqStat
				esFailing atomic.Bool
			)
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result, stats := docappendertest.DecodeBulkRequestWithStats(r)
				rs := collectTelemetryHeaders(t, r, stats)
				reqStats = append(reqStats, rs)
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

			for _, stats := range reqStats {
				assert.Equal(t, stats.headerUncompressedLength, stats.actualUncompressedLength, "%s header does not match the actual value", docappender.HeaderUncompressedLength)
				assert.Equal(t, stats.headerEventCount, stats.actualEventCount, "%s header does not match the actual value", docappender.HeaderEventCount)
			}
		})
	}
}

func TestBulkIndexer_Codec(t *testing.T) {
	for _, tc := range []struct {
		name string

		compressionLevel int
		itemsCount       int
	}{
		{
			name:             "no_items_uncompressed",
			compressionLevel: gzip.NoCompression,
			itemsCount:       0,
		},
		{
			name:             "no_items_compressed",
			compressionLevel: gzip.BestCompression,
			itemsCount:       0,
		},
		{
			name:             "uncompressed",
			compressionLevel: gzip.NoCompression,
			itemsCount:       101,
		},
		{
			name:             "compressed",
			compressionLevel: gzip.BestCompression,
			itemsCount:       1001,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result, _ := docappendertest.DecodeBulkRequestWithStats(r)
				json.NewEncoder(w).Encode(result)
			})
			idx1, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 0, // disable retry
				CompressionLevel:   tc.compressionLevel,
			})
			require.NoError(t, err)
			for i := 0; i < tc.itemsCount; i++ {
				require.NoError(t, idx1.Add(docappender.BulkIndexerItem{
					Index: "testidx",
					Body: newJSONReader(map[string]any{
						"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
					}),
				}))
			}
			b, err := idx1.AppendBinary(nil)
			require.NoError(t, err)
			expectedSize := idx1.Size(docappender.BytesSizer)

			idx2, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 0, // disable retry
				CompressionLevel:   tc.compressionLevel,
			})
			require.NoError(t, err)
			_, err = idx2.UnmarshalBinary(b)
			require.NoError(t, err)
			assert.Equal(t, tc.itemsCount, idx2.Items())
			assert.Equal(t, expectedSize, idx2.Size(docappender.BytesSizer))

			// Add another item to the new indexer
			require.NoError(t, idx2.Add(docappender.BulkIndexerItem{
				Index: "testidx",
				Body: newJSONReader(map[string]any{
					"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
				}),
			}))
			stat, err := idx2.Flush(context.Background())
			require.NoError(t, err)
			assert.Equal(t, int64(tc.itemsCount)+1, stat.Indexed)
		})
	}
}

func TestBulkIndexer_Merge(t *testing.T) {
	for _, tc := range []struct {
		name string

		sourceCompressionLevel int
		sourceIndexerItems     int

		targetCompressionLevel int
		targetIndexerItems     int

		expectedErr string
	}{
		{
			name:                   "src_target_empty",
			sourceCompressionLevel: gzip.NoCompression,
			sourceIndexerItems:     0,
			targetCompressionLevel: gzip.NoCompression,
			targetIndexerItems:     0,
		},
		{
			name:                   "src_empty",
			sourceCompressionLevel: gzip.NoCompression,
			sourceIndexerItems:     0,
			targetCompressionLevel: gzip.NoCompression,
			targetIndexerItems:     100,
		},
		{
			name:                   "target_empty",
			sourceCompressionLevel: gzip.NoCompression,
			sourceIndexerItems:     100,
			targetCompressionLevel: gzip.NoCompression,
			targetIndexerItems:     0,
		},
		{
			name:                   "merge_with_both_data",
			sourceCompressionLevel: gzip.NoCompression,
			sourceIndexerItems:     100,
			targetCompressionLevel: gzip.NoCompression,
			targetIndexerItems:     20,
		},
		{
			name:                   "compressed",
			sourceCompressionLevel: gzip.BestCompression,
			sourceIndexerItems:     100,
			targetCompressionLevel: gzip.BestCompression,
			targetIndexerItems:     20,
		},
		{
			name:                   "different_compression",
			sourceCompressionLevel: gzip.BestCompression,
			sourceIndexerItems:     100,
			targetCompressionLevel: gzip.BestSpeed,
			targetIndexerItems:     20,
			expectedErr:            "only same compression level merge is supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result, _ := docappendertest.DecodeBulkRequestWithStats(r)
				json.NewEncoder(w).Encode(result)
			})
			sourceIndexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 0, // disable retry
				CompressionLevel:   tc.sourceCompressionLevel,
			})
			require.NoError(t, err)
			targetIndexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 0, // disable retry
				CompressionLevel:   tc.targetCompressionLevel,
			})
			require.NoError(t, err)
			generateLoad := func(items int, indexer *docappender.BulkIndexer) {
				for indexer.Items() != items {
					require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
						Index: "testidx",
						Body: newJSONReader(map[string]any{
							"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
						}),
					}))
				}
			}

			generateLoad(tc.sourceIndexerItems, sourceIndexer)
			generateLoad(tc.targetIndexerItems, targetIndexer)

			totalItems := tc.sourceIndexerItems + tc.targetIndexerItems
			err = targetIndexer.Merge(sourceIndexer)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				assert.Equal(t, totalItems, targetIndexer.Items())
				stat, err := targetIndexer.Flush(context.Background())
				require.NoError(t, err)
				assert.Equal(t, totalItems, int(stat.Indexed))
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}

func TestBulkIndexer_Split(t *testing.T) {
	for _, tc := range []struct {
		name string

		sourceIndexerMinSize   int // used to populate test data
		sourceCompressionLevel int

		maxSize   int
		sizerType docappender.SizerType

		expectedErr string
	}{
		{
			name:                   "empty",
			maxSize:                10,
			sizerType:              docappender.ItemsCountSizer,
			sourceIndexerMinSize:   0,
			sourceCompressionLevel: gzip.BestCompression,
		},
		{
			name:                   "uncompressed_items_split",
			maxSize:                100,
			sizerType:              docappender.ItemsCountSizer,
			sourceIndexerMinSize:   205,
			sourceCompressionLevel: gzip.NoCompression,
		},
		{
			name:                   "uncompressed_bytes_split",
			maxSize:                10_000,
			sizerType:              docappender.BytesSizer,
			sourceIndexerMinSize:   1_000_005,
			sourceCompressionLevel: gzip.NoCompression,
		},
		{
			name:                   "compressed_items_split",
			maxSize:                100,
			sizerType:              docappender.ItemsCountSizer,
			sourceIndexerMinSize:   205,
			sourceCompressionLevel: gzip.BestCompression,
		},
		{
			name:                   "compressed_bytes_split",
			maxSize:                10_000,
			sizerType:              docappender.BytesSizer,
			sourceIndexerMinSize:   1_000_005,
			sourceCompressionLevel: gzip.BestCompression,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result, _ := docappendertest.DecodeBulkRequestWithStats(r)
				json.NewEncoder(w).Encode(result)
			})
			indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 0, // disable retry
				CompressionLevel:   tc.sourceCompressionLevel,
			})
			require.NoError(t, err)

			// Populate the required indexer to size
			for indexer.Size(tc.sizerType) < tc.sourceIndexerMinSize {
				require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
					Index: "testidx",
					Body: newJSONReader(map[string]any{
						"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
					}),
				}))
			}
			totalItems := indexer.Items()

			splitBulkIndexers, err := indexer.Split(tc.maxSize, tc.sizerType)
			require.NoError(t, err)

			var indexedItems int
			for _, bi := range splitBulkIndexers {
				stat, err := bi.Flush(context.Background())
				require.NoError(t, err)
				assert.LessOrEqual(t, bi.Size(tc.sizerType), tc.maxSize)
				indexedItems += int(stat.Indexed)
			}
			assert.Equal(t, totalItems, indexedItems)
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

func TestQueryParams(t *testing.T) {
	sourceFields := []string{"message", "timestamp"}

	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		queryParams := r.URL.Query()

		require.True(t, queryParams.Has("_source"))
		require.Equal(t, sourceFields, queryParams["_source"])

		_, result, _, _, _ := docappendertest.DecodeBulkRequestWithStatsAndDynamicTemplatesAndPipelines(r)
		err := json.NewEncoder(w).Encode(result)
		require.NoError(t, err)

	})

	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client: client,
		QueryParams: map[string][]string{
			"_source": sourceFields,
		},
	})
	require.NoError(t, err)

	err = indexer.Add(docappender.BulkIndexerItem{
		Index: "testidx",
		Body: newJSONReader(map[string]any{
			"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
		}),
	})
	require.NoError(t, err)

	_, err = indexer.Flush(context.Background())
	require.NoError(t, err)
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

func TestBulkIndexer_FailureStore(t *testing.T) {
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, result := docappendertest.DecodeBulkRequest(r)
		var i int
		for _, itemsMap := range result.Items {
			for k, item := range itemsMap {
				switch i % 4 {
				case 0:
					item.FailureStore = string(docappender.FailureStoreStatusUsed)
				case 1:
					item.FailureStore = string(docappender.FailureStoreStatusFailed)
				case 2:
					item.FailureStore = string(docappender.FailureStoreStatusUnknown)
				case 3:
					item.FailureStore = string(docappender.FailureStoreStatusNotEnabled)
				}
				itemsMap[k] = item
				i++
			}
		}
		err := json.NewEncoder(w).Encode(result)
		require.NoError(t, err)
	})
	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client: client,
	})
	require.NoError(t, err)

	for range 4 {
		err = indexer.Add(docappender.BulkIndexerItem{
			Index: "testidx",
			Body: newJSONReader(map[string]any{
				"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
			}),
		})
		require.NoError(t, err)
	}

	stat, err := indexer.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(4), stat.Indexed)
	require.Equal(t, int64(1), stat.FailureStoreDocs.Used)
	require.Equal(t, int64(1), stat.FailureStoreDocs.Failed)
	require.Equal(t, int64(1), stat.FailureStoreDocs.NotEnabled)
}

func TestBulkIndexerRetryDocument(t *testing.T) {
	testCases := map[string]struct {
		cfg docappender.BulkIndexerConfig
	}{
		"nocompression": {
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:    100,
				RetryOnDocumentStatus: []int{http.StatusTooManyRequests},
			},
		},
		"gzip": {
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:    100,
				RetryOnDocumentStatus: []int{http.StatusTooManyRequests},
				CompressionLevel:      gzip.BestCompression,
			},
		},
		// As populateFailedDocsInput reuses some code as document retry, ensure that they work together.
		"nocompression,populateFailedDocsInput": {
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:      100,
				RetryOnDocumentStatus:   []int{http.StatusTooManyRequests},
				PopulateFailedDocsInput: true,
			},
		},
		"gzip,populateFailedDocsInput": {
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:      100,
				RetryOnDocumentStatus:   []int{http.StatusTooManyRequests},
				CompressionLevel:        gzip.BestCompression,
				PopulateFailedDocsInput: true,
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
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
							itemResp.Error.Type = "too_many_requests"
							itemResp.Error.Reason = "for testing"
							item["create"] = itemResp
							result.HasErrors = true
						}
					}
					json.NewEncoder(w).Encode(result)
					return
				case 2:
					require.Len(t, result.Items, 7)
					assert.Equal(t, "test0", result.Items[0]["create"].Index)
					assert.Equal(t, "test4", result.Items[1]["create"].Index)
					assert.Equal(t, "test5", result.Items[2]["create"].Index)
					assert.Equal(t, "test6", result.Items[3]["create"].Index)
					assert.Equal(t, "test9", result.Items[4]["create"].Index)
					assert.Equal(t, "test10", result.Items[5]["create"].Index)
					assert.Equal(t, "test11", result.Items[6]["create"].Index)

					for i, item := range result.Items {
						switch i {
						case 0, 1, 3, 5, 6:
							itemResp := item["create"]
							itemResp.Status = http.StatusTooManyRequests
							itemResp.Error.Type = "too_many_requests"
							itemResp.Error.Reason = "for testing"
							item["create"] = itemResp
							result.HasErrors = true
						}
					}
					json.NewEncoder(w).Encode(result)
					return
				}
				require.Len(t, result.Items, 6)
				assert.Equal(t, "test0", result.Items[0]["create"].Index)
				assert.Equal(t, "test4", result.Items[1]["create"].Index)
				assert.Equal(t, "test6", result.Items[2]["create"].Index)
				assert.Equal(t, "test10", result.Items[3]["create"].Index)
				assert.Equal(t, "test11", result.Items[4]["create"].Index)
				assert.Equal(t, "test12", result.Items[5]["create"].Index)
				json.NewEncoder(w).Encode(result)
				done.Store(true)
			})

			tc.cfg.Client = client
			indexer, err := docappender.NewBulkIndexer(tc.cfg)
			require.NoError(t, err)

			const N = 10
			for i := 0; i < N; i++ {
				err := indexer.Add(docappender.BulkIndexerItem{
					Index: fmt.Sprintf("test%d", i),
					Body:  newJSONReader(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)}),
				})
				require.NoError(t, err)
			}

			// First flush - some documents fail and are retried
			stat, err := indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(5), stat.Indexed)     // 5 succeeded
			require.Equal(t, int64(5), stat.RetriedDocs) // 5 failed and retried
			require.Len(t, stat.FailedDocs, 0)           // no permanent failures
			require.Equal(t, 5, indexer.Items())         // 5 documents are queued for retry

			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "test10",
				Body:  newJSONReader(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)}),
			})
			require.NoError(t, err)

			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "test11",
				Body:  newJSONReader(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)}),
			})
			require.NoError(t, err)

			require.Equal(t, 7, indexer.Items()) // 5 retries + 2 new documents

			// Second flush - more retries
			stat, err = indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(2), stat.Indexed)     // 2 succeeded
			require.Equal(t, int64(5), stat.RetriedDocs) // 5 failed and retried again
			require.Len(t, stat.FailedDocs, 0)           // no permanent failures
			require.Equal(t, 5, indexer.Items())         // 5 documents still queued for retry

			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "test12",
				Body:  newJSONReader(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)}),
			})
			require.NoError(t, err)

			require.Equal(t, 6, indexer.Items()) // 5 retries + 1 new document

			// Final flush - all documents should succeed
			stat, err = indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(6), stat.Indexed)     // All 6 remaining documents succeed
			require.Equal(t, int64(0), stat.RetriedDocs) // No more retries needed
			require.Len(t, stat.FailedDocs, 0)           // no permanent failures
			require.Equal(t, 0, indexer.Items())         // No documents left in the indexer

			require.Eventually(t, func() bool {
				return done.Load()
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for completion")
		})
	}
}

func TestBulkIndexerRetryDocument_PermanentFailures(t *testing.T) {
	testCases := map[string]struct {
		cfg docappender.BulkIndexerConfig
	}{
		"nocompression": {
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:    2, // Low retry limit to trigger permanent failures
				RetryOnDocumentStatus: []int{http.StatusTooManyRequests},
			},
		},
		"populateFailedDocsInput": {
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:      2, // Low retry limit to trigger permanent failures
				RetryOnDocumentStatus:   []int{http.StatusTooManyRequests},
				PopulateFailedDocsInput: true,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var requestCount atomic.Int32
			var done atomic.Bool

			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result := docappendertest.DecodeBulkRequest(r)
				requestNum := requestCount.Add(1)

				// Always fail the first 3 documents with 429 errors
				for i, item := range result.Items {
					if i < 3 {
						itemResp := item["create"]
						itemResp.Status = http.StatusTooManyRequests
						itemResp.Error.Type = "too_many_requests"
						itemResp.Error.Reason = "for testing permanent failures"
						item["create"] = itemResp
						result.HasErrors = true
					}
				}
				json.NewEncoder(w).Encode(result)
				if requestNum >= 3 {
					done.Store(true)
				}
			})

			tc.cfg.Client = client
			indexer, err := docappender.NewBulkIndexer(tc.cfg)
			require.NoError(t, err)

			// Add 5 documents
			for i := 0; i < 5; i++ {
				err := indexer.Add(docappender.BulkIndexerItem{
					Index: fmt.Sprintf("test%d", i),
					Body:  newJSONReader(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)}),
				})
				require.NoError(t, err)
			}

			// First flush - 3 documents fail and are retried, 2 succeed
			stat, err := indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(2), stat.Indexed)     // 2 succeeded (test3, test4)
			require.Equal(t, int64(3), stat.RetriedDocs) // 3 failed and retried (test0, test1, test2)
			require.Len(t, stat.FailedDocs, 0)           // no permanent failures yet
			require.Equal(t, 3, indexer.Items())         // 3 documents are queued for retry

			// Second flush - same 3 documents fail again (retry count = 2)
			stat, err = indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(0), stat.Indexed)     // none succeeded
			require.Equal(t, int64(3), stat.RetriedDocs) // 3 failed and retried again
			require.Len(t, stat.FailedDocs, 0)           // no permanent failures yet (retry count = 2)
			require.Equal(t, 3, indexer.Items())         // 3 documents still queued for retry

			// Third flush - max retries (2) exceeded, documents become permanently failed
			stat, err = indexer.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(0), stat.Indexed)     // none succeeded
			require.Equal(t, int64(0), stat.RetriedDocs) // no more retries (max exceeded)
			require.Len(t, stat.FailedDocs, 3)           // 3 permanent failures
			require.Equal(t, 0, indexer.Items())         // no documents left in the indexer

			// Verify the right documents are marked as failed
			require.Equal(t, "test0", stat.FailedDocs[0].Index)
			require.Equal(t, "test1", stat.FailedDocs[1].Index)
			require.Equal(t, "test2", stat.FailedDocs[2].Index)
			for _, failedDoc := range stat.FailedDocs {
				require.Equal(t, http.StatusTooManyRequests, failedDoc.Status)
				require.Equal(t, "too_many_requests", failedDoc.Error.Type)
				if tc.cfg.PopulateFailedDocsInput {
					require.NotEmpty(t, failedDoc.Input)
				} else {
					require.Empty(t, failedDoc.Input)
				}
			}

			require.Eventually(t, func() bool {
				return done.Load()
			}, 2*time.Second, 50*time.Millisecond, "timed out waiting for completion")
		})
	}
}

func TestBulkIndexerRetryDocument_RetryOnDocumentStatus(t *testing.T) {
	testCases := map[string]struct {
		status      int
		shouldRetry bool
		cfg         docappender.BulkIndexerConfig
	}{
		"should retry": {
			status:      500,
			shouldRetry: true,
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:    1,
				RetryOnDocumentStatus: []int{429, 500},
			},
		},
		"should not retry": {
			status:      500,
			shouldRetry: false,
			cfg: docappender.BulkIndexerConfig{
				MaxDocumentRetries:    1,
				RetryOnDocumentStatus: []int{429},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var requestCount atomic.Int32
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result := docappendertest.DecodeBulkRequest(r)
				requestCount.Add(1)

				for _, item := range result.Items {
					itemResp := item["create"]
					itemResp.Status = tc.status
					itemResp.Error.Type = "test_error"
					itemResp.Error.Reason = "for testing"
					item["create"] = itemResp
					result.HasErrors = true
				}
				json.NewEncoder(w).Encode(result)
			})

			tc.cfg.Client = client
			indexer, err := docappender.NewBulkIndexer(tc.cfg)
			require.NoError(t, err)

			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "test1",
				Body:  strings.NewReader(`{}`),
			})
			require.NoError(t, err)

			// First flush
			stat, err := indexer.Flush(context.Background())
			require.NoError(t, err)

			if tc.shouldRetry {
				require.Equal(t, int64(0), stat.Indexed)     // Should not succeed
				require.Equal(t, int64(1), stat.RetriedDocs) // Should retry
				require.Len(t, stat.FailedDocs, 0)           // No permanent failures yet
			} else {
				require.Equal(t, int64(0), stat.Indexed)     // Should not succeed
				require.Equal(t, int64(0), stat.RetriedDocs) // Should not retry
				require.Len(t, stat.FailedDocs, 1)           // Should fail permanently
			}

			// If there's a retry, flush again to process it
			if tc.shouldRetry {
				stat, err = indexer.Flush(context.Background())
				require.NoError(t, err)
				// After max retries, should fail permanently
				require.Len(t, stat.FailedDocs, 1) // Should fail permanently after max retries
			}

			// Check that we got the expected number of requests
			expectedRequests := 1
			if tc.shouldRetry {
				expectedRequests = 2 // Initial request + retry
			}

			require.Eventually(t, func() bool {
				return requestCount.Load() >= int32(expectedRequests)
			}, 2*time.Second, 50*time.Millisecond, "Expected %d requests, got %d", expectedRequests, requestCount.Load())
		})
	}
}

func TestPopulateFailedDocsInput(t *testing.T) {
	test := func(enabled bool, compressionLevel int) {
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

		indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
			Client:                  client,
			PopulateFailedDocsInput: enabled,
			CompressionLevel:        compressionLevel,
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
		want := []docappender.BulkIndexerResponseItem{
			{
				Index:    "foo",
				Status:   http.StatusBadRequest,
				Position: 0,
				Error: struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				}{Type: "validation_error", Reason: ""},
				Input: `{"create":{"_index":"foo"}}
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
				}{Type: "validation_error", Reason: ""},
				Input: `{"create":{"_index":"bar"}}
{"5":"6"}
`,
			},
		}
		if !enabled {
			for i := 0; i < len(want); i++ {
				want[i].Input = ""
			}
		}
		assert.Equal(t, want, stat.FailedDocs)
	}
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			for _, compressionLevel := range []int{0, 1} {
				t.Run(fmt.Sprintf("compression_level=%d", compressionLevel), func(t *testing.T) {
					test(enabled, compressionLevel)
				})
			}
		})
	}
}

func TestBulkIndexerBatchSplitOn413(t *testing.T) {
	t.Run("basic_split_with_compression_levels", func(t *testing.T) {
		compressionTests := []struct {
			name             string
			compressionLevel int
		}{
			{"uncompressed", gzip.NoCompression},
			{"compressed", gzip.DefaultCompression},
		}

		for _, tt := range compressionTests {
			t.Run(tt.name, func(t *testing.T) {
				var (
					requestCount int64
					firstRequest = true
				)

				client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
					atomic.AddInt64(&requestCount, 1)
					currentReqCount := atomic.LoadInt64(&requestCount)

					docs, result, stats := docappendertest.DecodeBulkRequestWithStats(r)

					if firstRequest {
						// First request returns 413 (payload too large)
						firstRequest = false
						assert.Equal(t, 10, len(docs), "Initial request should have all 10 documents")
						w.WriteHeader(http.StatusRequestEntityTooLarge)
						fmt.Fprintln(w, `{"error":{"type":"request_entity_too_large","reason":"Request entity too large"}}`)
						return
					}

					// Subsequent requests (after splitting) should succeed
					// We should get exactly 2 more requests (first half and second half)
					assert.LessOrEqual(t, currentReqCount, int64(3), "Should not exceed 3 requests total")

					// Each split request should have exactly 5 documents (10/2)
					assert.Equal(t, 5, len(docs), "Each split batch should have exactly 5 docs")

					// Return successful response
					for i, itemsMap := range result.Items {
						for k, item := range itemsMap {
							item.Status = http.StatusCreated
							item.Result = "created"
							item.Version = int64(i + 1)
							itemsMap[k] = item
						}
					}
					json.NewEncoder(w).Encode(result)

					// Validate request stats
					assert.Equal(t, int64(len(docs)), stats.EventCount)
					assert.Greater(t, stats.UncompressedBytes, int64(0))
				})

				indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
					Client:                client,
					CompressionLevel:      tt.compressionLevel,
					EnableBatchSplitOn413: true,
				})
				require.NoError(t, err)

				// Add 10 documents to create a batch that will trigger 413
				for i := 0; i < 10; i++ {
					require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
						Index:  "test-index",
						Action: "create",
						Body: newJSONReader(map[string]any{
							"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
							"message":    fmt.Sprintf("Test document %d", i),
							"id":         i,
						}),
					}))
				}

				// Verify we have 10 items before flush
				assert.Equal(t, 10, indexer.Items())

				// Flush should handle 413 by splitting and succeed
				stat, err := indexer.Flush(context.Background())
				require.NoError(t, err)

				// Verify all documents were eventually indexed
				assert.Equal(t, int64(10), stat.Indexed)
				assert.Equal(t, int64(0), stat.RetriedDocs)
				assert.Empty(t, stat.FailedDocs)

				// Verify we made exactly 3 requests (1 failed + 2 successful splits)
				assert.Equal(t, int64(3), atomic.LoadInt64(&requestCount))

				// Buffer should be empty after successful flush
				assert.Equal(t, 0, indexer.Items())
				assert.Equal(t, 0, indexer.Len())
			})
		}
	})

	t.Run("with_failures_during_split", func(t *testing.T) {
		var requestCount int64

		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&requestCount, 1)
			currentReqCount := atomic.LoadInt64(&requestCount)

			docs, result, _ := docappendertest.DecodeBulkRequestWithStats(r)

			if currentReqCount == 1 {
				// First request returns 413
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				fmt.Fprintln(w, `{"error":{"type":"request_entity_too_large","reason":"Request entity too large"}}`)
				return
			}

			// Second request (first half) succeeds
			if currentReqCount == 2 {
				// Should have exactly 4 documents (first half of 8)
				assert.Equal(t, 4, len(docs), "First half should have 4 documents")
				for i, itemsMap := range result.Items {
					for k, item := range itemsMap {
						item.Status = http.StatusCreated
						item.Result = "created"
						item.Version = int64(i + 1)
						itemsMap[k] = item
					}
				}
			} else {
				// Third request (second half) has some failures
				// Should have exactly 4 documents (second half of 8)
				assert.Equal(t, 4, len(docs), "Second half should have 4 documents")
				for i, itemsMap := range result.Items {
					for k, item := range itemsMap {
						if i%2 == 0 {
							// Every other document fails
							item.Status = http.StatusBadRequest
							item.Error.Type = "test_error"
							item.Error.Reason = "simulated failure"
							result.HasErrors = true
						} else {
							item.Status = http.StatusCreated
							item.Result = "created"
							item.Version = int64(i + 1)
						}
						itemsMap[k] = item
					}
				}
			}

			json.NewEncoder(w).Encode(result)
		})

		indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
			Client:                client,
			EnableBatchSplitOn413: true,
		})
		require.NoError(t, err)

		// Add 8 documents
		for i := 0; i < 8; i++ {
			require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
				Index: "test-index",
				Body: newJSONReader(map[string]any{
					"message": fmt.Sprintf("Test document %d", i),
					"id":      i,
				}),
			}))
		}

		stat, err := indexer.Flush(context.Background())
		require.NoError(t, err)

		// Should have 6 successful documents (4 from first half + 2 from second half that succeed)
		// and 2 failed documents (2 from second half that fail due to i%2 == 0)
		assert.Equal(t, int64(6), stat.Indexed, "Should have exactly 6 indexed documents")
		assert.Equal(t, len(stat.FailedDocs), 2, "Should have exactly 2 failed documents")

		// Check that failed document positions are adjusted correctly for second half
		// Failed docs should be at positions 4 and 6 (original positions in the full 8-doc batch)
		expectedFailedPositions := []int{4, 6} // Even positions in second half: 0->4, 2->6
		actualFailedPositions := make([]int, len(stat.FailedDocs))
		for i, failedDoc := range stat.FailedDocs {
			actualFailedPositions[i] = failedDoc.Position
			assert.GreaterOrEqual(t, failedDoc.Position, 0)
			assert.Less(t, failedDoc.Position, 8) // Should be within original batch size
			assert.Equal(t, "test_error", failedDoc.Error.Type)
		}
		assert.ElementsMatch(t, expectedFailedPositions, actualFailedPositions, "Failed document positions should be correctly adjusted")

		// Should have made 3 requests total
		assert.Equal(t, int64(3), atomic.LoadInt64(&requestCount))
	})

	t.Run("single_document_cannot_split", func(t *testing.T) {
		// This test verifies that when a single document causes a 413 error,
		// we don't attempt to split (since there's nothing to split) and instead
		// return the original 413 error to the caller.
		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			docs, _, _ := docappendertest.DecodeBulkRequestWithStats(r)
			assert.Equal(t, 1, len(docs), "Should always have exactly 1 document")

			// Always return 413 - this should not trigger splitting for single document
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			fmt.Fprintln(w, `{"error":{"type":"request_entity_too_large","reason":"Request entity too large"}}`)
		})

		indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
			Client:                client,
			EnableBatchSplitOn413: true,
		})
		require.NoError(t, err)

		// Add only 1 document
		require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
			Index: "test-index",
			Body: newJSONReader(map[string]any{
				"message": "Single large document",
			}),
		}))

		// Should return error without splitting since there's only 1 document
		_, err = indexer.Flush(context.Background())
		require.Error(t, err)

		// Should be ErrorFlushFailed with payloadTooLarge = true
		var flushErr docappender.ErrorFlushFailed
		require.ErrorAs(t, err, &flushErr)
		assert.Equal(t, http.StatusRequestEntityTooLarge, flushErr.StatusCode())
	})

	t.Run("recursive_splitting", func(t *testing.T) {
		// This test verifies that recursive splitting works when even split batches are too large
		var requestCount int64

		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&requestCount, 1)
			docs, result, _ := docappendertest.DecodeBulkRequestWithStats(r)

			// Return 413 for batches with more than 2 documents
			if len(docs) > 2 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				fmt.Fprintln(w, `{"error":{"type":"request_entity_too_large","reason":"Request entity too large"}}`)
				return
			}

			// Success for batches with 1-2 documents
			for i, itemsMap := range result.Items {
				for k, item := range itemsMap {
					item.Status = http.StatusCreated
					item.Result = "created"
					item.Version = int64(i + 1)
					itemsMap[k] = item
				}
			}
			json.NewEncoder(w).Encode(result)
		})

		indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
			Client:                client,
			EnableBatchSplitOn413: true,
		})
		require.NoError(t, err)

		// Add 8 documents
		for i := 0; i < 8; i++ {
			require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
				Index: "test-index",
				Body: newJSONReader(map[string]any{
					"message": fmt.Sprintf("Test document %d", i),
					"id":      i,
				}),
			}))
		}

		// Flush should recursively split until all batches are ≤ 2 documents
		stat, err := indexer.Flush(context.Background())
		require.NoError(t, err)

		// All 8 documents should eventually be indexed
		assert.Equal(t, int64(8), stat.Indexed)
		assert.Empty(t, stat.FailedDocs)

		// Should make multiple requests due to recursive splitting:
		// 1st: 8 docs -> 413
		// 2nd: 4 docs (first half) -> 413
		// 3rd: 4 docs (second half) -> 413
		// 4th: 2 docs (first quarter) -> success
		// 5th: 2 docs (second quarter) -> success
		// 6th: 2 docs (third quarter) -> success
		// 7th: 2 docs (fourth quarter) -> success
		// Total: 7 requests
		assert.Equal(t, int64(7), atomic.LoadInt64(&requestCount))
	})

	t.Run("splitting_disabled_returns_error", func(t *testing.T) {
		// This test verifies that when EnableBatchSplitOn413 is false,
		// a 413 error is returned without attempting to split
		var requestCount int64

		client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&requestCount, 1)

			// Always return 413
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			fmt.Fprintln(w, `{"error":{"type":"request_entity_too_large","reason":"Request entity too large"}}`)
		})

		indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
			Client:                client,
			EnableBatchSplitOn413: false,
		})
		require.NoError(t, err)

		// Add 10 documents
		for i := 0; i < 10; i++ {
			require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
				Index: "test-index",
				Body: newJSONReader(map[string]any{
					"message": fmt.Sprintf("Test document %d", i),
					"id":      i,
				}),
			}))
		}

		// Flush should return error without splitting
		_, err = indexer.Flush(context.Background())
		require.Error(t, err)

		// Should be ErrorFlushFailed with 413 status
		var flushErr docappender.ErrorFlushFailed
		require.ErrorAs(t, err, &flushErr)
		assert.Equal(t, http.StatusRequestEntityTooLarge, flushErr.StatusCode())

		// Should have made exactly 1 request (no retry/split attempts)
		assert.Equal(t, int64(1), atomic.LoadInt64(&requestCount))

		// Documents should be discarded after failed flush
		assert.Equal(t, 0, indexer.Items())
	})
}

func BenchmarkAppendBinary(b *testing.B) {
	itemsCount := 100000
	for _, bc := range []struct {
		name        string
		compression int
	}{
		{
			name:        "uncompressed",
			compression: gzip.NoCompression,
		},
		{
			name:        "compressed",
			compression: gzip.BestCompression,
		},
	} {
		b.Run(bc.name, func(b *testing.B) {
			client := docappendertest.NewMockElasticsearchClient(
				b, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				},
			)
			indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 0,
				CompressionLevel:   bc.compression,
			})
			require.NoError(b, err)

			for i := 0; i < itemsCount; i++ {
				require.NoError(b, indexer.Add(docappender.BulkIndexerItem{
					Index: "testidx",
					Body: newJSONReader(map[string]any{
						"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
					}),
				}))
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err = indexer.AppendBinary(nil)
				if err != nil {
					// Error check to avoid benchmarking `require` overhead
					require.NoError(b, err)
				}
			}
		})
	}
}

func BenchmarkMerge(b *testing.B) {
	client := docappendertest.NewMockElasticsearchClient(
		b, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	)
	newIndexerCfg := func(compression int) docappender.BulkIndexerConfig {
		return docappender.BulkIndexerConfig{
			Client:             client,
			MaxDocumentRetries: 0,
			CompressionLevel:   compression,
		}
	}
	for _, bc := range []struct {
		name        string
		compression int
	}{
		{
			name:        "uncompressed",
			compression: gzip.NoCompression,
		},
		{
			name:        "compressed",
			compression: gzip.BestCompression,
		},
	} {
		b.Run(bc.name, func(b *testing.B) {
			// prepare bulk indexers for benchmarking merge
			bulkIndexers := make([]*docappender.BulkIndexer, 0, b.N)
			for i := 0; i < b.N; i++ {
				indexer, err := docappender.NewBulkIndexer(newIndexerCfg(bc.compression))
				require.NoError(b, err)

				for j := 0; j < 100; j++ {
					require.NoError(b, indexer.Add(docappender.BulkIndexerItem{
						Index: "testidx",
						Body: newJSONReader(map[string]any{
							"@timestamp": time.Now().Format(docappendertest.TimestampFormat),
						}),
					}))
				}
				bulkIndexers = append(bulkIndexers, indexer)
			}
			// create final indexer that will merge data from all other indexers
			final, err := docappender.NewBulkIndexer(newIndexerCfg(bc.compression))
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				assert.NoError(b, final.Merge(bulkIndexers[i]))
			}
		})
	}
}
