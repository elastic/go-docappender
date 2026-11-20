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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	jsoniter "github.com/json-iterator/go"
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

func TestSettingFilterPath(t *testing.T) {
	for _, tc := range []struct {
		name               string
		filterPath         string
		expectedFilterPath string
	}{
		{
			name:               "default",
			expectedFilterPath: docappender.DefaultFilterPath,
		},
		{
			name:               "set in BulkIndexerConfig",
			filterPath:         "items.*.error.reason",
			expectedFilterPath: "items.*.error.reason",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				filterPath := r.URL.Query().Get("filter_path")
				assert.Equal(t, tc.expectedFilterPath, filterPath)

				_, result := docappendertest.DecodeBulkRequest(r)
				json.NewEncoder(w).Encode(result)
			})

			indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:     client,
				FilterPath: tc.filterPath,
			})
			require.NoError(t, err)

			err = indexer.Add(docappender.BulkIndexerItem{
				Index: "testidx",
				Body:  strings.NewReader(`{"message":"test"}`),
			})
			require.NoError(t, err)

			_, err = indexer.Flush(context.Background())
			require.NoError(t, err)
		})
	}
}

func TestPopulateIndexStats(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, result := docappendertest.DecodeBulkRequest(r)
				for _, itemsMap := range result.Items {
					for k, item := range itemsMap {
						switch item.Index {
						case "bad", "retried":
							result.HasErrors = true
							item.Status = http.StatusBadRequest
							if item.Index == "retried" {
								item.Status = http.StatusTooManyRequests
							}
							item.Error.Type = "validation_error"
							itemsMap[k] = item
						}
					}
				}
				json.NewEncoder(w).Encode(result)
			})
			indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:             client,
				MaxDocumentRetries: 1,
				PopulateIndexStats: enabled,
			})
			require.NoError(t, err)
			for _, index := range []string{"a", "b", "a", "bad", "a", "retried", "bad"} {
				require.NoError(t, indexer.Add(docappender.BulkIndexerItem{
					Index: index,
					Body:  strings.NewReader(`{"1":"2"}`),
				}))
			}

			stat, err := indexer.Flush(context.Background())
			require.NoError(t, err)
			assert.Equal(t, int64(4), stat.Indexed)
			assert.Len(t, stat.FailedDocs, 2)
			assert.Equal(t, int64(1), stat.RetriedDocs)
			if enabled {
				// The document pending retry is not counted as failed.
				assert.Equal(t, map[string]docappender.BulkIndexerIndexStat{
					"a":   {Indexed: 3},
					"b":   {Indexed: 1},
					"bad": {Failed: 2},
				}, stat.IndexStats)
			} else {
				assert.Nil(t, stat.IndexStats)
			}
		})
	}
}

func TestPopulateIndexStatsFailureStore(t *testing.T) {
	// failure_store is deliberately sent before _index, to check that the
	// counts do not depend on the order of the fields in the response.
	const body = `{"items":[
		{"create":{"failure_store":"used","_index":".fs-a","status":201}},
		{"create":{"_index":"a","status":201}},
		{"create":{"failure_store":"used","_index":".fs-a","status":201}},
		{"create":{"failure_store":"not_enabled","_index":"b","status":400,"error":{"type":"mapper_parsing_exception"}}},
		{"create":{"failure_store":"failed","_index":"b","status":400,"error":{"type":"mapper_parsing_exception"}}},
		{"create":{"failure_store":"not_applicable_or_unknown","_index":"c","status":201}}
	]}`
	client := docappendertest.NewMockElasticsearchClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, err := io.WriteString(w, body)
		require.NoError(t, err)
	})
	indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
		Client:             client,
		PopulateIndexStats: true,
	})
	require.NoError(t, err)
	for range 6 {
		require.NoError(t, indexer.Add(docappender.BulkIndexerItem{Index: "x", Body: strings.NewReader(`{}`)}))
	}

	stat, err := indexer.Flush(context.Background())
	require.NoError(t, err)

	assert.Equal(t, docappender.FailureStoreDocsStat{Used: 2, Failed: 1, NotEnabled: 1}, stat.FailureStoreDocs)
	assert.Equal(t, map[string]docappender.BulkIndexerIndexStat{
		".fs-a": {Indexed: 2, FailureStoreDocs: docappender.FailureStoreDocsStat{Used: 2}},
		"a":     {Indexed: 1},
		"b":     {Failed: 2, FailureStoreDocs: docappender.FailureStoreDocsStat{Failed: 1, NotEnabled: 1}},
		"c":     {Indexed: 1},
	}, stat.IndexStats)
}

type performFunc func(*http.Request) (*http.Response, error)

func (f performFunc) Perform(r *http.Request) (*http.Response, error) { return f(r) }

func respondWith(status int, body io.Reader) performFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: io.NopCloser(body)}, nil
	}
}

func addTestDocs(t *testing.T, bi *docappender.BulkIndexer, n int) {
	t.Helper()
	for range n {
		require.NoError(t, bi.Add(docappender.BulkIndexerItem{
			Index: "idx",
			Body:  strings.NewReader(`{"message":"test"}`),
		}))
	}
}

func TestBulkIndexerResponseStatJSONIterator(t *testing.T) {
	var result struct {
		Stat  docappender.BulkIndexerResponseStat
		After int
	}
	err := jsoniter.UnmarshalFromString(
		`{"Stat":{"items":[{"create":{"status":201}},{"create":{"error":{"type":"parse"}}}],"took":1},"After":2}`,
		&result,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 1, result.Stat.Indexed)
	require.Len(t, result.Stat.FailedDocs, 1)
	assert.Equal(t, 1, result.Stat.FailedDocs[0].Position)
	assert.Equal(t, "parse", result.Stat.FailedDocs[0].Error.Type)
	assert.Equal(t, 2, result.After)
}

func TestBulkIndexerInvalidResponse(t *testing.T) {
	const decodeErr = "error decoding bulk response"

	check := func(t *testing.T, body io.Reader, filterPath, wantErr string, docs int, readErr error) {
		t.Helper()
		bi, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
			Client: respondWith(http.StatusOK, body), FilterPath: filterPath,
			MaxDocumentRetries: 1, PopulateIndexStats: true,
		})
		require.NoError(t, err)
		addTestDocs(t, bi, docs)
		stat, err := bi.Flush(t.Context())
		require.ErrorContains(t, err, wantErr)
		if readErr != nil {
			assert.ErrorIs(t, err, readErr)
		}
		assert.NotContains(t, err.Error(), "source text")
		assert.Equal(t, docappender.BulkIndexerResponseStat{}, stat)
		assert.Zero(t, bi.Items())
	}

	for _, tc := range []struct{ name, body string }{
		{"before items", `{"took":[1,],"items":[{"create":{"status":201}}]}`},
		{"truncated items", `{"items":[{"create":{"status":201}}`},
		{"truncated object", `{"items":[{"create":{"status":201}}]`},
		{"malformed field after items", `{"items":[{"create":{"status":201}}],"took":}`},
		{"second JSON value", `{"items":[{"create":{"status":201}}]}{}`},
		{"duplicate items", `{"items":[{"create":{"status":201}}],"items":[]}`},
		{"null items", `{"items":null}`},
		{"null action", `{"items":[{"create":null}]}`},
		{"negative status", `{"items":[{"create":{"status":-201}}]}`},
		{"zero status", `{"items":[{"create":{"status":0}}]}`},
		{"1xx status", `{"items":[{"create":{"status":100}}]}`},
		{"status out of range", `{"items":[{"create":{"status":600}}]}`},
		{"string status", `{"items":[{"create":{"status":"201"}}]}`},
		{"fractional status", `{"items":[{"create":{"status":201.5}}]}`},
		{"null status", `{"items":[{"create":{"status":null}}]}`},
		{"duplicate status", `{"items":[{"create":{"status":201,"status":200}}]}`},
	} {
		for _, filterPath := range []string{"", "items.*.status"} {
			t.Run(fmt.Sprintf("%s/filter=%q", tc.name, filterPath), func(t *testing.T) {
				check(t, strings.NewReader(tc.body), filterPath, decodeErr, 1, nil)
			})
		}
	}

	const extraItem = "item count 2 does not match document count 1"
	for _, tc := range []struct{ name, body, filterPath, wantErr string }{
		{"extra failed item", `{"items":[{"create":{"status":201}},{"create":{"status":429}}]}`, "", extraItem},
		{"extra created item", `{"items":[{"create":{"status":201}},{"create":{"status":201}}]}`, "", extraItem},
		{"extra item with custom filter path", `{"items":[{"create":{}},{"create":{"error":{"type":"x"}}}]}`, "items.*.error", extraItem},
		{"no items", `{"errors":false}`, "", "item count 0 does not match document count 1"},
		{"null body", `null`, "", "response body is not a JSON object"},
		{"array body", `[]`, "", "response body is not a JSON object"},
		{"item without action", `{"items":[{}]}`, "", "item 0 has no action"},
		{"item with two actions", `{"items":[{"create":{"status":201},"index":{"status":201}}]}`, "", "item 0 has more than one action"},
		{"item without status", `{"items":[{"create":{"_index":"idx"}}]}`, "", "item 0 has no status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check(t, strings.NewReader(tc.body), tc.filterPath, tc.wantErr, 1, nil)
		})
	}

	t.Run("redact and discard partial stats", func(t *testing.T) {
		const body = `{"items":[
			{"create":{"_index":"idx","status":201,"failure_store":"used"}},
			{"create":{"_index":"idx","status":400,"error":{"type":"parse","reason":"source text"}}},
			{"create":{"error":{"reason":"source text"},"status":!}}
		]}`
		check(t, strings.NewReader(body), "", decodeErr, 3, nil)
	})

	readErr := errors.New("response read failed")
	for _, tc := range []struct{ name, body string }{
		{"before object", ""},
		{"before items", `{"took":`},
		{"during items", `{"items":[{"create":{"status":201`},
		{"after items", `{"items":[{"create":{"status":201}}]`},
		{"after object", `{"items":[{"create":{"status":201}}]}`},
	} {
		t.Run("read error "+tc.name, func(t *testing.T) {
			body := iotest.DataErrReader(io.MultiReader(strings.NewReader(tc.body), iotest.ErrReader(readErr)))
			check(t, body, "", decodeErr, 1, readErr)
		})
	}
	t.Run("read error on next read", func(t *testing.T) {
		body := io.MultiReader(strings.NewReader(`{"items":[{"create":{"status":201}}]}`), iotest.ErrReader(readErr))
		check(t, body, "", decodeErr, 1, readErr)
	})
}

func TestBulkIndexerCustomFilterPathAllowsPartialItems(t *testing.T) {
	var failed docappender.BulkIndexerResponseItem
	failed.Error.Type = "mapper_parsing_exception"

	for _, tc := range []struct {
		name       string
		filterPath string
		body       string
		want       docappender.BulkIndexerResponseStat
	}{
		{
			name:       "only failed items",
			filterPath: "items.*.error",
			body:       `{"items":[{"create":{"error":{"type":"mapper_parsing_exception"}}}]}`,
			want:       docappender.BulkIndexerResponseStat{FailedDocs: []docappender.BulkIndexerResponseItem{failed}},
		},
		{
			name:       "no items",
			filterPath: "items.*.error",
			body:       `{}`,
			want:       docappender.BulkIndexerResponseStat{},
		},
		{
			name:       "items without status",
			filterPath: "items.*._index",
			body:       `{"items":[{"create":{"_index":"idx"}}]}`,
			want:       docappender.BulkIndexerResponseStat{Indexed: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bi, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client:     respondWith(http.StatusOK, strings.NewReader(tc.body)),
				FilterPath: tc.filterPath,
			})
			require.NoError(t, err)
			addTestDocs(t, bi, 2)

			stat, err := bi.Flush(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tc.want, stat)
		})
	}
}

func TestBulkIndexerHTTPErrorBody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		readErr error
	}{
		{"unreadable", http.StatusRequestEntityTooLarge, `{"error":`, io.ErrUnexpectedEOF},
		{"informational", http.StatusEarlyHints, "", io.ErrUnexpectedEOF},
		{"non-JSON", http.StatusBadGateway, "<html>Bad gateway</html>", nil},
	} {
		for name, includeSource := range map[string]docappender.Value{
			"unset": docappender.Unset,
			"false": docappender.False,
			"true":  docappender.True,
		} {
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				var body io.Reader = strings.NewReader(tc.body)
				if tc.readErr != nil {
					body = io.MultiReader(body, iotest.ErrReader(tc.readErr))
				}
				bi, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
					Client: respondWith(tc.status, body), IncludeSourceOnError: includeSource,
					PopulateIndexStats: true,
				})
				require.NoError(t, err)
				addTestDocs(t, bi, 1)

				stat, err := bi.Flush(t.Context())
				var flushErr docappender.ErrorFlushFailed
				require.ErrorAs(t, err, &flushErr)
				if tc.readErr != nil {
					assert.ErrorIs(t, err, tc.readErr)
				} else {
					assert.IsType(t, flushErr, err)
				}
				assert.Equal(t, tc.status, flushErr.StatusCode())
				wantBody := tc.body
				if tc.readErr != nil || includeSource == docappender.Unset {
					wantBody = ""
				}
				assert.Equal(t, wantBody, flushErr.ResponseBody())
				assert.Equal(t, docappender.BulkIndexerResponseStat{}, stat)
				assert.Zero(t, bi.Items())
			})
		}
	}
}

type ndjsonBuffer struct {
	*strings.Reader
	items int
}

func (b ndjsonBuffer) ItemCount() int { return b.items }

func TestBulkIndexerExtractionError(t *testing.T) {
	const create = `{"create":{"_index":"idx"}}` + "\n" + `{"id":1}` + "\n"
	const deleteLine = `{"delete":{"_index":"idx","_id":"1"}}` + "\n"
	partial := create + "{}\n{}\n{}\n" + strings.Repeat("x", 9000)
	const retryThird = `{"items":[{"create":{"status":429}},{"create":{"status":201}},{"create":{"status":429}}]}`
	for _, tc := range []struct {
		name, request, response string
		items, failedPos        int
	}{
		{"one-line delete before retry", deleteLine + create, `{"items":[{"delete":{"status":200}},{"create":{"status":429}}]}`, 2, 1},
		{"missing start newline", create, retryThird, 3, 2},
		{"missing end newline during retry", partial, retryThird, 3, 2},
		{"missing end newline during input lookup", partial, `{"items":[{"create":{"status":429}},{"create":{"status":201}},{"create":{"status":400}}]}`, 3, 2},
	} {
		for _, compression := range []int{gzip.NoCompression, gzip.BestSpeed} {
			t.Run(fmt.Sprintf("%s/compression=%d", tc.name, compression), func(t *testing.T) {
				bi, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
					Client: respondWith(http.StatusOK, strings.NewReader(tc.response)), CompressionLevel: compression,
					MaxDocumentRetries: 1, PopulateFailedDocsInput: true, PopulateIndexStats: true,
				})
				require.NoError(t, err)
				require.NoError(t, bi.AddBuffer(ndjsonBuffer{strings.NewReader(tc.request), tc.items}))
				stat, err := bi.Flush(t.Context())
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				assert.ErrorContains(t, err, fmt.Sprintf("position %d", tc.failedPos))
				assert.Equal(t, docappender.BulkIndexerResponseStat{}, stat)
				assert.Zero(t, bi.Items())
				assert.Zero(t, bi.Len())
				assert.Zero(t, bi.UncompressedLen())
			})
		}
	}
}

func TestBulkIndexerExtractsLargeFailedDocuments(t *testing.T) {
	const action = `{"create":{"_index":"idx"}}` + "\n"
	var docs []string
	for i, size := range []int{5000, 9000, 9000, 200, 9000} {
		docs = append(docs, action+fmt.Sprintf("{\"id\":%d,\"message\":%q}\n", i, strings.Repeat("x", size)))
	}
	const body = `{"items":[{"create":{"status":429}},{"create":{"status":201}},{"create":{"status":400}},{"create":{"status":201}},{"create":{"status":429}}]}`
	for _, compression := range []int{gzip.NoCompression, gzip.BestSpeed} {
		t.Run(fmt.Sprintf("compression=%d", compression), func(t *testing.T) {
			bi, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
				Client: respondWith(http.StatusOK, strings.NewReader(body)), CompressionLevel: compression,
				MaxDocumentRetries: 1, PopulateFailedDocsInput: true,
			})
			require.NoError(t, err)
			require.NoError(t, bi.AddBuffer(ndjsonBuffer{strings.NewReader(strings.Join(docs, "")), len(docs)}))
			stat, err := bi.Flush(t.Context())
			require.NoError(t, err)
			assert.EqualValues(t, 2, stat.Indexed)
			assert.EqualValues(t, 2, stat.RetriedDocs)
			require.Len(t, stat.FailedDocs, 1)
			assert.Equal(t, 2, stat.FailedDocs[0].Position)
			assert.Equal(t, docs[2], stat.FailedDocs[0].Input)
			require.Equal(t, 2, bi.Items())

			bi.SetClient(performFunc(func(req *http.Request) (*http.Response, error) {
				var body io.Reader = req.Body
				if compression != gzip.NoCompression {
					r, err := gzip.NewReader(body)
					require.NoError(t, err)
					defer r.Close()
					body = r
				}
				payload, err := io.ReadAll(body)
				require.NoError(t, err)
				assert.Equal(t, docs[0]+docs[4], string(payload))
				const accepted = `{"items":[{"create":{"status":201}},{"create":{"status":201}}]}`
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(accepted))}, nil
			}))
			stat, err = bi.Flush(t.Context())
			require.NoError(t, err)
			assert.EqualValues(t, 2, stat.Indexed)
		})
	}
}
