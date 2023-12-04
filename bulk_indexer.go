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
	client       *elasticsearch.Client
	itemsAdded   int
	bytesFlushed int
	jsonw        fastjson.Writer
	writer       io.Writer
	gzipw        *gzip.Writer
	buf          bytes.Buffer
}

type BulkIndexerResponseStat struct {
	Indexed    int64
	FailedDocs []BulkIndexerResponseItem
}

// BulkIndexerResponseItem represents the Elasticsearch response item.
type BulkIndexerResponseItem struct {
	Index  string `json:"_index"`
	Status int    `json:"status"`

	Position int

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
				var idx int
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
						item.Position = idx
						idx++
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

func newBulkIndexer(client *elasticsearch.Client, compressionLevel int, compressThreshold int) *bulkIndexer {
	b := &bulkIndexer{client: client}
	if compressionLevel != gzip.NoCompression {
		b.gzipw, _ = gzip.NewWriterLevel(&b.buf, compressionLevel)
		b.writer = b.gzipw
	} else {
		b.writer = &b.buf
	}
	return b
}

// BulkIndexer resets b, ready for a new request.
func (b *bulkIndexer) Reset() {
	b.bytesFlushed = 0
}

func (b *bulkIndexer) resetBuf() {
	b.itemsAdded = 0
	b.buf.Reset()
	if b.gzipw != nil {
		b.gzipw.Reset(&b.buf)
	}
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
	DocumentID string
	Body       io.WriterTo
}

// add encodes an item in the buffer.
func (b *bulkIndexer) add(item bulkIndexerItem) error {
	b.writeMeta(item.Index, item.DocumentID)
	if _, err := item.Body.WriteTo(b.writer); err != nil {
		return fmt.Errorf("failed to write bulk indexer item: %w", err)
	}
	if _, err := b.writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
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
	b.writer.Write(b.jsonw.Bytes())
	b.jsonw.Reset()
}

// Flush executes a bulk request if there are any items buffered, and clears out the buffer.
func (b *bulkIndexer) Flush(ctx context.Context) (BulkIndexerResponseStat, error) {
	if b.itemsAdded == 0 {
		return BulkIndexerResponseStat{}, nil
	}

	if b.gzipw != nil {
		if err := b.gzipw.Close(); err != nil {
			return BulkIndexerResponseStat{}, fmt.Errorf("failed closing the gzip writer: %w", err)
		}
	}

	copyBuf := b.buf.Bytes()

	req := esapi.BulkRequest{
		Body:       &b.buf,
		Header:     make(http.Header),
		FilterPath: []string{"items.*._index", "items.*.status", "items.*.error.type", "items.*.error.reason"},
	}
	if b.gzipw != nil {
		req.Header.Set("Content-Encoding", "gzip")
	}

	bytesFlushed := b.buf.Len()
	res, err := req.Do(ctx, b.client)
	if err != nil {
		b.resetBuf()
		return BulkIndexerResponseStat{}, fmt.Errorf("failed to execute the request: %w", err)
	}
	defer res.Body.Close()

	b.resetBuf()

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

	for _, biri := range resp.FailedDocs {
		if biri.Status == http.StatusTooManyRequests {
			startlnIdx := biri.Position * 2
			endlnIdx := startlnIdx + 2

			if b.gzipw != nil {
				gr, err := gzip.NewReader(bytes.NewReader(copyBuf))
				if err != nil {
					return resp, fmt.Errorf("failed to decompress request payload: %w", err)
				}
				defer gr.Close()

				seen := 0

				buf := make([]byte, 1024)
				n, _ := gr.Read(buf[:1024])
				buf = buf[:n]
				for newlines := bytes.Count(buf, []byte{'\n'}); seen+newlines < startlnIdx; newlines = bytes.Count(buf, []byte{'\n'}) {
					seen += newlines
					n, _ := gr.Read(buf[:1024])
					buf = buf[:n]
				}

				start := Indexnth(buf, startlnIdx-seen, '\n') + 1
				end := Indexnth(buf, endlnIdx-seen, '\n') + 1

				for end == 0 {
					// Add more capacity (let append pick how much).
					buf = append(buf, 0)[:len(buf)]

					n, _ := gr.Read(buf[len(buf):cap(buf)])
					buf = buf[:len(buf)+n]

					end = Indexnth(buf, endlnIdx-seen, '\n') + 1
				}

				b.writer.Write(buf[start:end])
			} else {
				start := Indexnth(copyBuf, startlnIdx, '\n') + 1
				end := Indexnth(copyBuf, endlnIdx, '\n') + 1

				b.writer.Write(copyBuf[start:end])
			}

			b.itemsAdded++
		}
	}

	return resp, nil
}

// Indexnth returns the index of the nth instance of sep in s.
// It returns -1 if sep is not present in s or nth is 0.
func Indexnth(s []byte, nth int, sep rune) int {
	if nth == 0 {
		return -1
	}

	count := 0
	return bytes.IndexFunc(s, func(r rune) bool {
		if r == '\n' {
			count++
		}
		return nth == count
	})
}

type errorTooManyRequests struct {
	res *esapi.Response
}

func (e errorTooManyRequests) Error() string {
	return fmt.Sprintf("flush failed: %s", e.res.String())
}
