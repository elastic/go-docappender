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
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	jsoniter "github.com/json-iterator/go"
)

// At the time of writing, the go-elasticsearch BulkIndexer implementation
// sends all items to a channel, and multiple persistent worker goroutines will
// receive those items and independently fill up their own buffers. Each one
// will independently flush when their buffer is filled up, or when the flush
// interval elapses. If there are many workers, then this may lead to sparse
// bulk requests.
//
// We take a different approach, where we fill up one bulk request at a time.
// When the buffer is filled up, or the flush interval elapses, we start a new
// goroutine to send the request in the background, with a limit on the number
// of concurrent bulk requests. This way we can ensure bulk requests have the
// maximum possible size, based on configuration and throughput.

type bulkIndexerV2 struct {
	client           *elasticsearch.Client
	maxDocumentRetry int
	bytesFlushed     int
}

func newBulkIndexerV2(client *elasticsearch.Client, maxDocRetry int) *bulkIndexerV2 {
	b := &bulkIndexerV2{
		client:           client,
		maxDocumentRetry: maxDocRetry,
	}
	return b
}

// BulkIndexer resets b, ready for a new request.
func (b *bulkIndexerV2) Reset() {
	b.bytesFlushed = 0
}

// BytesFlushed returns the number of bytes flushed by the bulk indexer.
func (b *bulkIndexerV2) BytesFlushed() int {
	return b.bytesFlushed
}

// Flush executes a bulk request if there are any items buffered, and clears out the buffer.
func (b *bulkIndexerV2) Flush(ctx context.Context, isGzip bool, buffers []*AppenderBuffer) (BulkIndexerResponseStat, error) {
	docsAdded := 0
	requestSize := 0
	var readers []io.Reader

	var bufNames []string
	for i := range buffers {
		readers = append(readers, bytes.NewReader(buffers[i].buf.Bytes()))
		docsAdded += buffers[i].docsAdded
		requestSize += buffers[i].buf.Len()
		bufNames = append(bufNames, buffers[i].index)
	}
	if docsAdded == 0 {
		return BulkIndexerResponseStat{}, nil
	}

	req := esapi.BulkRequest{
		Body:       io.MultiReader(readers...),
		Header:     make(http.Header),
		FilterPath: []string{"items.*._index", "items.*.status", "items.*.error.type", "items.*.error.reason"},
	}
	if isGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	bytesFlushed := requestSize
	res, err := req.Do(ctx, b.client)
	if err != nil {
		return BulkIndexerResponseStat{}, fmt.Errorf("failed to execute the request: %w", err)
	}
	defer res.Body.Close()

	// Record the number of flushed bytes only when err == nil. The body may
	// not have been sent otherwise.
	b.bytesFlushed = bytesFlushed
	var resp BulkIndexerResponseStat
	if res.IsError() {
		if res.StatusCode == http.StatusTooManyRequests {
			fmt.Printf("too many: %s\n", strings.Join(bufNames, ","))
			return resp, errorTooManyRequests{res: res}
		}
		return resp, fmt.Errorf("flush failed: %v | %s", res.StatusCode, res.String())
	}

	if err := jsoniter.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("error decoding bulk response: %w", err)
	}

	return resp, nil
}
