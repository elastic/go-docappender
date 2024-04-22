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
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"unsafe"

	"github.com/klauspost/compress/gzip"
	"go.elastic.co/fastjson"

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

// BulkIndexerConfig holds configuration for BulkIndexer.
type BulkIndexerConfig struct {
	// Client holds the Elasticsearch client.
	Client esapi.Transport

	// MaxDocumentRetries holds the maximum number of document retries
	MaxDocumentRetries int

	// RetryOnDocumentStatus holds the document level statuses that will trigger a document retry.
	RetryOnDocumentStatus []int

	// CompressionLevel holds the gzip compression level, from 0 (gzip.NoCompression)
	// to 9 (gzip.BestCompression). Higher values provide greater compression, at a
	// greater cost of CPU. The special value -1 (gzip.DefaultCompression) selects the
	// default compression level.
	CompressionLevel int

	// Pipeline holds the ingest pipeline ID.
	//
	// If Pipeline is empty, no ingest pipeline will be specified in the Bulk request.
	Pipeline string
}

type BulkIndexer struct {
	config       BulkIndexerConfig
	itemsAdded   int
	bytesFlushed int
	jsonw        fastjson.Writer
	writer       io.Writer
	gzipw        *gzip.Writer
	copyBuf      []byte
	buf          bytes.Buffer
	retryCounts  map[int]int
}

type BulkIndexerResponseStat struct {
	Indexed     int64
	RetriedDocs int64
	FailedDocs  []BulkIndexerResponseItem
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

// NewBulkIndexer returns a bulk indexer that issues bulk requests to Elasticsearch.
// It is only tested with v8 go-elasticsearch client. Use other clients at your own risk.
func NewBulkIndexer(cfg BulkIndexerConfig) (*BulkIndexer, error) {
	if cfg.Client == nil {
		return nil, errors.New("client is nil")
	}

	if cfg.CompressionLevel < -1 || cfg.CompressionLevel > 9 {
		return nil, fmt.Errorf(
			"expected CompressionLevel in range [-1,9], got %d",
			cfg.CompressionLevel,
		)
	}

	b := &BulkIndexer{
		config:      cfg,
		retryCounts: make(map[int]int),
	}

	// use a len check instead of a nil check because document level retries
	// should be disabled using MaxDocumentRetries instead.
	if len(b.config.RetryOnDocumentStatus) == 0 {
		b.config.RetryOnDocumentStatus = []int{http.StatusTooManyRequests}
	}

	if cfg.CompressionLevel != gzip.NoCompression {
		b.gzipw, _ = gzip.NewWriterLevel(&b.buf, cfg.CompressionLevel)
		b.writer = b.gzipw
	} else {
		b.writer = &b.buf
	}
	return b, nil
}

// Reset resets bulk indexer, ready for a new request.
func (b *BulkIndexer) Reset() {
	b.bytesFlushed = 0
}

func (b *BulkIndexer) resetBuf() {
	b.itemsAdded = 0
	b.buf.Reset()
	if b.gzipw != nil {
		b.gzipw.Reset(&b.buf)
	}
}

// Items returns the number of buffered items.
func (b *BulkIndexer) Items() int {
	return b.itemsAdded
}

// Len returns the number of buffered bytes.
func (b *BulkIndexer) Len() int {
	return b.buf.Len()
}

// BytesFlushed returns the number of bytes flushed by the bulk indexer.
func (b *BulkIndexer) BytesFlushed() int {
	return b.bytesFlushed
}

type BulkIndexerItem struct {
	Index      string
	DocumentID string
	Body       io.WriterTo
}

// Add encodes an item in the buffer.
func (b *BulkIndexer) Add(item BulkIndexerItem) error {
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

func (b *BulkIndexer) writeMeta(index, documentID string) {
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
func (b *BulkIndexer) Flush(ctx context.Context) (BulkIndexerResponseStat, error) {
	if b.itemsAdded == 0 {
		return BulkIndexerResponseStat{}, nil
	}

	if b.gzipw != nil {
		if err := b.gzipw.Close(); err != nil {
			return BulkIndexerResponseStat{}, fmt.Errorf("failed closing the gzip writer: %w", err)
		}
	}

	if b.config.MaxDocumentRetries > 0 {
		if cap(b.copyBuf) < b.buf.Len() {
			b.copyBuf = slices.Grow(b.copyBuf, b.buf.Len()-cap(b.copyBuf))
			b.copyBuf = b.copyBuf[:cap(b.copyBuf)]
		}
		n := copy(b.copyBuf, b.buf.Bytes())
		b.copyBuf = b.copyBuf[:n]
	}

	req := esapi.BulkRequest{
		Body:       &b.buf,
		Header:     make(http.Header),
		FilterPath: []string{"items.*._index", "items.*.status", "items.*.error.type", "items.*.error.reason"},
		Pipeline:   b.config.Pipeline,
	}
	if b.gzipw != nil {
		req.Header.Set("Content-Encoding", "gzip")
	}

	bytesFlushed := b.buf.Len()
	res, err := req.Do(ctx, b.config.Client)
	if err != nil {
		b.resetBuf()
		return BulkIndexerResponseStat{}, fmt.Errorf("failed to execute the request: %w", err)
	}
	defer res.Body.Close()

	// Reset the buffer and gzip writer so they can be reused in case
	// document level retries are needed.
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

	// Only run the retry logic if document retries are enabled
	if b.config.MaxDocumentRetries > 0 {
		buf := make([]byte, 0, 4096)

		// Eliminate previous retry counts that aren't present in the bulk
		// request response.
		for k := range b.retryCounts {
			found := false
			for _, res := range resp.FailedDocs {
				if res.Position == k {
					found = true
					break
				}
			}
			if !found {
				// Retried request succeeded, remove from retry counts
				delete(b.retryCounts, k)
			}
		}

		tmp := resp.FailedDocs[:0]
		lastln := 0
		lastIdx := 0
		var gr *gzip.Reader

		if b.gzipw != nil {
			gr, err = gzip.NewReader(bytes.NewReader(b.copyBuf))
			if err != nil {
				return resp, fmt.Errorf("failed to decompress request payload: %w", err)
			}
			defer gr.Close()
		}

		// keep track of the previous newlines
		// the buffer is being read lazily
		seen := 0

		for _, res := range resp.FailedDocs {
			if b.shouldRetryOnStatus(res.Status) {
				// there are two lines for each document:
				// - action
				// - document
				//
				// Find the document by looking up the newline separators.
				// First the newline (if exists) before the 'action' then the
				// newline at the end of the 'document' line.
				startln := res.Position * 2
				endln := startln + 2

				// Increment retry count for the positions found.
				count := b.retryCounts[res.Position] + 1
				// check if we are above the maxDocumentRetry setting
				if count > b.config.MaxDocumentRetries {
					// do not retry, return the document as failed
					tmp = append(tmp, res)
					continue
				}

				// Since some items may have succeeded, counter positions need
				// to be updated to match the next current buffer position.
				b.retryCounts[b.itemsAdded] = count

				if b.gzipw != nil {
					// First loop, read from the gzip reader
					if len(buf) == 0 {
						n, err := gr.Read(buf[:cap(buf)])
						if err != nil && err != io.EOF {
							return resp, fmt.Errorf("failed to read from compressed buffer: %w", err)
						}
						buf = buf[:n]
					}

					// newlines in the current buf
					newlines := bytes.Count(buf, []byte{'\n'})

					// loop until we've seen the start newline
					for seen+newlines < startln {
						seen += newlines
						n, err := gr.Read(buf[:cap(buf)])
						if err != nil && err != io.EOF {
							return resp, fmt.Errorf("failed to read from compressed buffer: %w", err)
						}
						buf = buf[:n]
						newlines = bytes.Count(buf, []byte{'\n'})
					}

					startIdx := indexnth(buf, startln-seen, '\n') + 1
					endIdx := indexnth(buf, endln-seen, '\n') + 1

					// If the end newline is not in the buffer read more data
					if endIdx == 0 {
						// Write what we have
						b.writer.Write(buf[startIdx:])

						// loop until we've seen the end newline
						for seen+newlines < endln {
							seen += newlines
							n, err := gr.Read(buf[:cap(buf)])
							if err != nil && err != io.EOF {
								return resp, fmt.Errorf("failed to read from compressed buffer: %w", err)
							}
							buf = buf[:n]
							newlines = bytes.Count(buf, []byte{'\n'})
							if seen+newlines < endln {
								// endln is not here, write what we have and keep going
								b.writer.Write(buf)
							}
						}

						// try again to find the end newline in the extra data
						// we just read.
						endIdx = indexnth(buf, endln-seen, '\n') + 1
						b.writer.Write(buf[:endIdx])
					} else {
						// If the end newline is in the buffer write the event
						b.writer.Write(buf[startIdx:endIdx])
					}
				} else {
					startIdx := indexnth(b.copyBuf[lastIdx:], startln-lastln, '\n') + 1
					endIdx := indexnth(b.copyBuf[lastIdx:], endln-lastln, '\n') + 1

					b.writer.Write(b.copyBuf[lastIdx:][startIdx:endIdx])

					lastln = endln
					lastIdx += endIdx
				}

				resp.RetriedDocs++
				b.itemsAdded++
			} else {
				// If it's not a retriable error, treat the document as failed
				tmp = append(tmp, res)
			}
		}

		if len(tmp) > 0 {
			resp.FailedDocs = tmp
		}
	}

	return resp, nil
}

func (b *BulkIndexer) shouldRetryOnStatus(docStatus int) bool {
	for _, status := range b.config.RetryOnDocumentStatus {
		if docStatus == status {
			return true
		}
	}
	return false
}

// indexnth returns the index of the nth instance of sep in s.
// It returns -1 if sep is not present in s or nth is 0.
func indexnth(s []byte, nth int, sep rune) int {
	if nth == 0 {
		return -1
	}

	count := 0
	return bytes.IndexFunc(s, func(r rune) bool {
		if r == sep {
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
