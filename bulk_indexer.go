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
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"go.elastic.co/fastjson"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
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

type bulkIndexer struct {
	client       *elasticsearch.Client
	itemsAdded   int
	bytesFlushed int
	jsonw        fastjson.Writer
	gzipw        *gzip.Writer
	copybuf      [32 * 1024]byte
	writer       io.Writer
	buf          bytes.Buffer
	resp         esutil.BulkIndexerResponse
}

func newBulkIndexer(client *elasticsearch.Client, compressionLevel int) *bulkIndexer {
	b := &bulkIndexer{client: client}
	if compressionLevel != gzip.NoCompression {
		b.gzipw, _ = gzip.NewWriterLevel(&b.buf, compressionLevel)
		b.writer = b.gzipw
	} else {
		b.writer = &b.buf
	}
	b.Reset()
	return b
}

// BulkIndexer resets b, ready for a new request.
func (b *bulkIndexer) Reset() {
	b.itemsAdded, b.bytesFlushed = 0, 0
	b.buf.Reset()
	if b.gzipw != nil {
		b.gzipw.Reset(&b.buf)
	}
	b.resp = esutil.BulkIndexerResponse{Items: b.resp.Items[:0]}
}

// Added returns the number of buffered items.
func (b *bulkIndexer) Items() int {
	return b.itemsAdded
}

// Len returns the number of buffered bytes.
func (b *bulkIndexer) Len() int {
	return b.buf.Len()
}

// BytesFlushed returns the number of bytes flushed by the bulk indexer.
func (b *bulkIndexer) BytesFlushed() int {
	return b.bytesFlushed
}

type bulkIndexerItem struct {
	Index      string
	Action     string
	DocumentID string
	Body       io.Reader
}

// add encodes an item in the buffer.
func (b *bulkIndexer) add(item bulkIndexerItem) error {
	b.writeMeta(item.Index, item.Action, item.DocumentID)
	if _, err := io.CopyBuffer(b.writer, item.Body, b.copybuf[:]); err != nil {
		return err
	}
	if _, err := b.writer.Write([]byte("\n")); err != nil {
		return err
	}
	b.itemsAdded++
	return nil
}

func (b *bulkIndexer) writeMeta(index, action, documentID string) {
	b.jsonw.RawByte('{')
	b.jsonw.String(action)
	b.jsonw.RawString(":{")
	if documentID != "" {
		b.jsonw.RawString(`"_id":`)
		b.jsonw.String(documentID)
	}
	if index != "" {
		if documentID != "" {
			b.jsonw.RawByte(',')
		}
		b.jsonw.RawString(`"_index":`)
		b.jsonw.String(index)
	}
	b.jsonw.RawString("}}\n")
	b.writer.Write(b.jsonw.Bytes())
	b.jsonw.Reset()
}

// Flush executes a bulk request if there are any items buffered, and clears out the buffer.
func (b *bulkIndexer) Flush(ctx context.Context) (esutil.BulkIndexerResponse, error) {
	if b.itemsAdded == 0 {
		return esutil.BulkIndexerResponse{}, nil
	}
	if b.gzipw != nil {
		if err := b.gzipw.Close(); err != nil {
			return esutil.BulkIndexerResponse{}, fmt.Errorf(
				"failed closing the gzip writer: %w", err,
			)
		}
	}

	req := esapi.BulkRequest{Body: &b.buf, Header: make(http.Header)}
	if b.gzipw != nil {
		req.Header.Set("Content-Encoding", "gzip")
	}

	bytesFlushed := b.buf.Len()
	res, err := req.Do(ctx, b.client)
	if err != nil {
		return esutil.BulkIndexerResponse{}, err
	}
	defer res.Body.Close()
	// Record the number of flushed bytes only when err == nil. The body may
	// not have been sent otherwise.
	b.bytesFlushed = bytesFlushed
	if res.IsError() {
		if res.StatusCode == http.StatusTooManyRequests {
			return esutil.BulkIndexerResponse{}, errorTooManyRequests{res: res}
		}
		return esutil.BulkIndexerResponse{}, fmt.Errorf("flush failed: %s", res.String())
	}
	if err := json.NewDecoder(res.Body).Decode(&b.resp); err != nil {
		return esutil.BulkIndexerResponse{}, fmt.Errorf("error decoding bulk response: %w", err)
	}
	return b.resp, nil
}

type errorTooManyRequests struct {
	res *esapi.Response
}

func (e errorTooManyRequests) Error() string {
	return fmt.Sprintf("flush failed: %s", e.res.String())
}
