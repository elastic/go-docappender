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

package docappendertest

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/apm/module/apmelasticsearch/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type BulkRequestItemMeta struct {
	Action            string            `json:"-"`
	Index             string            `json:"_index"`
	DocumentID        string            `json:"_id"`
	Pipeline          string            `json:"pipeline"`
	DynamicTemplates  map[string]string `json:"dynamic_templates"`
	RequireDataStream bool              `json:"require_data_stream"`
}

// TimestampFormat holds the time format for formatting timestamps according to
// Elasticsearch's strict_date_optional_time date format, which includes a fractional
// seconds component.
const TimestampFormat = "2006-01-02T15:04:05.000Z07:00"

// DecodeBulkRequest decodes a /_bulk request's body, returning the decoded documents and a response body.
func DecodeBulkRequest(r *http.Request) ([][]byte, esutil.BulkIndexerResponse) {
	indexed, result, _ := DecodeBulkRequestWithStats(r)
	return indexed, result
}

// DecodeBulkRequestWithStats decodes a /_bulk request's body, returning the decoded documents
// and a response body and stats about request.
func DecodeBulkRequestWithStats(r *http.Request) (
	docs [][]byte,
	res esutil.BulkIndexerResponse,
	stats RequestStats) {
	indexed, result, stats, _ := DecodeBulkRequestWithStatsAndDynamicTemplates(r)
	return indexed, result, stats
}

// DecodeBulkRequestWithStatsAndMeta decodes a /_bulk request's body,
// returning the decoded bulk request action/meta and documents,
// and a response body and stats about the request.
func DecodeBulkRequestWithStatsAndMeta(r *http.Request) (
	docs [][]byte,
	meta []BulkRequestItemMeta,
	res esutil.BulkIndexerResponse,
	stats RequestStats,
) {
	return decodeBulkRequest(r)
}

// DecodeBulkRequestWithStatsAndDynamicTemplates decodes a /_bulk request's body,
// returning the decoded documents and a response body and stats about request, and per-request dynamic templates.
func DecodeBulkRequestWithStatsAndDynamicTemplates(r *http.Request) (
	docs [][]byte,
	res esutil.BulkIndexerResponse,
	stats RequestStats,
	dynamicTemplates []map[string]string) {

	indexed, result, stats, dynamicTemplates, _ := DecodeBulkRequestWithStatsAndDynamicTemplatesAndPipelines(r)
	return indexed, result, stats, dynamicTemplates
}

// DecodeBulkRequestWithStatsAndDynamicTemplatesAndPipelines decodes a /_bulk request's body,
// returning the decoded documents and a response body and stats about request, per-request dynamic templates and pipelines specified in the event.
func DecodeBulkRequestWithStatsAndDynamicTemplatesAndPipelines(r *http.Request) (
	docs [][]byte,
	res esutil.BulkIndexerResponse,
	stats RequestStats,
	dynamicTemplates []map[string]string,
	pipelines []string,
) {
	docs, meta, res, stats := decodeBulkRequest(r)
	for _, meta := range meta {
		dynamicTemplates = append(dynamicTemplates, meta.DynamicTemplates)
		pipelines = append(pipelines, meta.Pipeline)
	}
	return docs, res, stats, dynamicTemplates, pipelines
}

func decodeBulkRequest(r *http.Request) (
	docs [][]byte,
	meta []BulkRequestItemMeta,
	result esutil.BulkIndexerResponse,
	stats RequestStats,
) {
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
	cr := &countReader{
		ReadCloser: body,
	}
	body = cr
	defer cr.Close()

	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		action := make(map[string]BulkRequestItemMeta)
		if err := json.NewDecoder(strings.NewReader(scanner.Text())).Decode(&action); err != nil {
			panic(err)
		}
		var actionType string
		for actionType = range action {
		}
		if !scanner.Scan() {
			panic("expected source")
		}

		doc := append([]byte{}, scanner.Bytes()...)
		if !json.Valid(doc) {
			panic(fmt.Errorf("invalid JSON: %s", doc))
		}
		docs = append(docs, doc)

		item := esutil.BulkIndexerResponseItem{Status: http.StatusCreated, Index: action[actionType].Index}
		result.Items = append(result.Items, map[string]esutil.BulkIndexerResponseItem{actionType: item})

		itemMeta := action[actionType]
		itemMeta.Action = actionType
		meta = append(meta, itemMeta)
	}
	return docs, meta, result, RequestStats{int64(cr.bytesRead)}
}

// NewMockElasticsearchClient returns an elasticsearch.Client which sends /_bulk requests to bulkHandler.
func NewMockElasticsearchClient(t testing.TB, bulkHandler http.HandlerFunc) *elasticsearch.Client {
	config := NewMockElasticsearchClientConfig(t, bulkHandler)
	client, err := elasticsearch.NewClient(config)
	require.NoError(t, err)
	return client
}

// NewMockElasticsearchClientConfig starts an httptest.Server, and returns an elasticsearch.Config which
// sends /_bulk requests to bulkHandler. The httptest.Server will be closed via t.Cleanup.
func NewMockElasticsearchClientConfig(t testing.TB, bulkHandler http.HandlerFunc) elasticsearch.Config {
	mux := http.NewServeMux()
	HandleBulk(mux, bulkHandler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	config := elasticsearch.Config{}
	config.Addresses = []string{srv.URL}
	config.DisableRetry = true
	config.Transport = apmelasticsearch.WrapRoundTripper(http.DefaultTransport)

	return config
}

// HandleBulk registers bulkHandler with mux for handling /_bulk requests,
// wrapping bulkHandler to conform with go-elasticsearch version checking.
func HandleBulk(mux *http.ServeMux, bulkHandler http.HandlerFunc) {
	mux.HandleFunc("/_bulk", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		bulkHandler.ServeHTTP(w, r)
	})
}

// NewAssertCounter returns a function that canbe used to assert counter metrics
func NewAssertCounter(t testing.TB, asserted *atomic.Int64) func(metric metricdata.Metrics, count int64, attrs attribute.Set) {
	t.Helper()

	return func(metric metricdata.Metrics, count int64, attrs attribute.Set) {
		asserted.Add(1)
		counter := metric.Data.(metricdata.Sum[int64])
		for _, dp := range counter.DataPoints {
			assert.Equal(t, count, dp.Value)
			assert.Equal(t, attrs, dp.Attributes)
		}
	}
}

// AssertOTelMetrics asserts OTel metrics using a closure.
func AssertOTelMetrics(t testing.TB, ms []metricdata.Metrics, assert func(m metricdata.Metrics)) {
	t.Helper()

	for _, m := range ms {
		assert(m)
	}
}

// helper reader to keep track of the bytes read by ReadCloser
type countReader struct {
	bytesRead int
	io.ReadCloser
}

// Read implements the [io.Reader] interface.
func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.bytesRead += n
	return n, err
}

type RequestStats struct {
	UncompressedBytes int64
}
