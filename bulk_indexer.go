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
	"fmt"
	"io"
	"net/http"
	"strings"
	"unsafe"

	"go.elastic.co/fastjson"

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

type bulkIndexer struct {
	client        *elasticsearch.Client
	itemsAdded    int
	bytesFlushed  int
	jsonw         fastjson.Writer
	gzipw         *gzip.Writer
	buf           bytes.Buffer
	compressedBuf bytes.Buffer
}

type BulkIndexerResponseStat struct {
	Indexed    int64
	FailedDocs []BulkIndexerResponseItem
}

// BulkIndexerResponseItem represents the Elasticsearch response item.
type BulkIndexerResponseItem struct {
	Index  string `json:"_index"`
	Status int    `json:"status"`

	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error,omitempty"`
}

func init() {
	jsoniter.RegisterTypeDecoderFunc("docappender.BulkIndexerResponseStat", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		iter.ReadObjectCB(func(i *jsoniter.Iterator, s string) bool {
			switch s {
			case "items":
				iter.ReadArrayCB(func(i *jsoniter.Iterator) bool {
					return i.ReadMapCB(func(i *jsoniter.Iterator, s string) bool {
						var item BulkIndexerResponseItem
						i.ReadObjectCB(func(i *jsoniter.Iterator, s string) bool {
							switch s {
							case "_index":
								item.Index = i.ReadString()
							case "status":
								item.Status = i.ReadInt()
							case "error":
								i.ReadObjectCB(func(i *jsoniter.Iterator, s string) bool {
									switch s {
									case "type":
										item.Error.Type = i.ReadString()
									case "reason":
										// Match Elasticsearch field mapper field value:
										// failed to parse field [%s] of type [%s] in %s. Preview of field's value: '%s'
										// https://github.com/elastic/elasticsearch/blob/588eabe185ad319c0268a13480465966cef058cd/server/src/main/java/org/elasticsearch/index/mapper/FieldMapper.java#L234
										item.Error.Reason, _, _ = strings.Cut(
											i.ReadString(), ". Preview",
										)
									default:
										i.Skip()
									}
									return true
								})
							default:
								i.Skip()
							}
							return true
						})
						if item.Error.Type != "" || item.Status > 201 {
							(*((*BulkIndexerResponseStat)(ptr))).FailedDocs = append((*((*BulkIndexerResponseStat)(ptr))).FailedDocs, item)
						} else {
							(*((*BulkIndexerResponseStat)(ptr))).Indexed = (*((*BulkIndexerResponseStat)(ptr))).Indexed + 1
						}
						return true
					})
				})
				// no need to proceed further, return early
				return false
			default:
				i.Skip()
				return true
			}
		})
	})
}

func newBulkIndexer(client *elasticsearch.Client, compressionLevel int) *bulkIndexer {
	b := &bulkIndexer{client: client}
	if compressionLevel != gzip.NoCompression {
		b.gzipw, _ = gzip.NewWriterLevel(&b.compressedBuf, compressionLevel)
	}
	return b
}

// BulkIndexer resets b, ready for a new request.
func (b *bulkIndexer) Reset() {
	b.itemsAdded, b.bytesFlushed = 0, 0
	b.compressedBuf.Reset()
	b.buf.Reset()
}

// Added returns the number of buffered items.
func (b *bulkIndexer) Items() int {
	return b.itemsAdded
}

// Len returns the number of buffered bytes.
func (b *bulkIndexer) Len() int {
	if b.gzipw == nil {
		return b.buf.Len()
	}
	return b.compressedBuf.Len()
}

// BytesFlushed returns the number of bytes flushed by the bulk indexer.
func (b *bulkIndexer) BytesFlushed() int {
	return b.bytesFlushed
}

type bulkIndexerItem struct {
	Index      string
	DocumentID string
	Body       io.WriterTo
}

// add encodes an item in the buffer.
func (b *bulkIndexer) add(item bulkIndexerItem) error {
	b.writeMeta(item.Index, item.DocumentID)
	if _, err := item.Body.WriteTo(&b.buf); err != nil {
		return fmt.Errorf("failed to write bulk indexer item: %w", err)
	}
	if _, err := b.buf.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	if b.gzipw != nil && b.buf.Len() > 512*1024 {
		if err := compress(b.gzipw, &b.buf, &b.compressedBuf); err != nil {
			return fmt.Errorf("failed to compress %d bytes: %w", b.buf.Len(), err)
		}
		b.buf.Reset()
	}
	b.itemsAdded++
	return nil
}

func (b *bulkIndexer) writeMeta(index, documentID string) {
	b.jsonw.RawString(`{"create":{`)
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
	b.buf.Write(b.jsonw.Bytes())
	b.jsonw.Reset()
}

// Flush executes a bulk request if there are any items buffered, and clears out the buffer.
func (b *bulkIndexer) Flush(ctx context.Context) (BulkIndexerResponseStat, error) {
	if b.itemsAdded == 0 {
		return BulkIndexerResponseStat{}, nil
	}

	var body bytes.Buffer
	compressed := b.gzipw != nil && b.compressedBuf.Len() != 0

	if compressed {
		if b.buf.Len() != 0 {
			if err := compress(b.gzipw, &b.buf, &b.compressedBuf); err != nil {
				return BulkIndexerResponseStat{}, fmt.Errorf("failed to compress buffered events before flushing: %w", err)
			}
			b.buf.Reset()
		}
		body = b.compressedBuf
	} else {
		body = b.buf
	}

	req := esapi.BulkRequest{
		Body:       &body,
		Header:     make(http.Header),
		FilterPath: []string{"items.*._index", "items.*.status", "items.*.error.type", "items.*.error.reason"},
	}
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	bytesFlushed := body.Len()
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
			return resp, errorTooManyRequests{res: res}
		}
		return resp, fmt.Errorf("flush failed: %s", res.String())
	}

	if err := jsoniter.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("error decoding bulk response: %w", err)
	}
	return resp, nil
}

func compress(gzipw *gzip.Writer, in *bytes.Buffer, out *bytes.Buffer) error {
	gzipw.Reset(out)
	if _, err := gzipw.Write(in.Bytes()); err != nil {
		return fmt.Errorf("failed to write to gzip writer: %w", err)
	}
	if err := gzipw.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return nil
}

type errorTooManyRequests struct {
	res *esapi.Response
}

func (e errorTooManyRequests) Error() string {
	return fmt.Sprintf("flush failed: %s", e.res.String())
}
