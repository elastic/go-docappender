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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-docappender/v2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// BenchmarkFlushIndexStats measures a flush of 1000 documents spread across
// a number of destination indices, with and without PopulateIndexStats.
func BenchmarkFlushIndexStats(b *testing.B) {
	const docs = 1000
	for _, tc := range []struct {
		indices int
		grouped bool // consecutive documents share an index
	}{{1, false}, {10, false}, {100, false}, {100, true}} {
		indices := tc.indices
		var resp strings.Builder
		resp.WriteString(`{"items":[`)
		for i := 0; i < docs; i++ {
			if i > 0 {
				resp.WriteByte(',')
			}
			n := i % indices
			if tc.grouped {
				n = i * indices / docs
			}
			fmt.Fprintf(&resp, `{"create":{"_index":".ds-logs-app%d-default-2026.10.13-000001","status":201}}`, n)
		}
		resp.WriteString(`]}`)
		body := resp.String()

		client, err := elastictransport.New(elastictransport.Config{
			URLs: []*url.URL{{Scheme: "http", Host: "localhost:9200"}},
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
					Body:       io.NopCloser(strings.NewReader(body)),
				}, nil
			}),
		})
		require.NoError(b, err)

		for _, populate := range []bool{false, true} {
			b.Run(fmt.Sprintf("indices=%d/grouped=%v/populate=%v", indices, tc.grouped, populate), func(b *testing.B) {
				indexer, err := docappender.NewBulkIndexer(docappender.BulkIndexerConfig{
					Client:             client,
					PopulateIndexStats: populate,
				})
				require.NoError(b, err)
				b.ReportAllocs()
				for b.Loop() {
					for i := 0; i < docs; i++ {
						if err := indexer.Add(docappender.BulkIndexerItem{
							Index: "idx",
							Body:  strings.NewReader(`{"a":1}`),
						}); err != nil {
							b.Fatal(err)
						}
					}
					stat, err := indexer.Flush(context.Background())
					if err != nil || stat.Indexed != docs {
						b.Fatalf("unexpected flush result: %v, %d", err, stat.Indexed)
					}
				}
			})
		}
	}
}
