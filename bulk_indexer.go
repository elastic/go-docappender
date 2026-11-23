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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/gzip"
	"go.elastic.co/fastjson"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
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

const (
	// Actions are all the actions that can be used when indexing data.
	// `create` will be used by default.
	ActionCreate = "create"
	ActionDelete = "delete"
	ActionIndex  = "index"
	ActionUpdate = "update"

	// DefaultFilterPath is the default `filter_path` used when
	// sending the Bulk request.
	DefaultFilterPath        = "items.*._index,items.*.status,items.*.failure_store,items.*.error.type,items.*.error.reason"
	HeaderEventCount         = "X-Elastic-Event-Count"
	HeaderUncompressedLength = "X-Elastic-Uncompressed-Request-Length"
)

// BulkIndexer issues bulk requests to Elasticsearch. It is NOT safe for concurrent use
// by multiple goroutines.
type BulkIndexer struct {
	config             BulkIndexerConfig
	itemsAdded         int
	bytesFlushed       int
	bytesUncompFlushed int
	jsonw              fastjson.Writer
	writer             *countWriter
	gzipw              *gzip.Writer
	buf                bytes.Buffer
	// retryCounts holds the retry count of each retried document in the
	// buffer, by position. Flush writes the retried documents first, so they
	// are the first len(retryCounts) documents; documents added later have no
	// entry. resetBuf drops the counts with the buffer.
	retryCounts       []int
	requireDataStream bool
	// deletes holds, in ascending order, the positions of the "delete" items
	// in the buffer. Delete items are encoded as a single action line, while
	// all other items take two lines (action and document).
	deletes []int
}

type BulkIndexerResponseStat struct {
	// Indexed contains the total number of successfully indexed documents.
	Indexed int64
	// IndexStats contains document counts per destination index, as reported
	// by the `_index` field of the response. It is only populated when
	// BulkIndexerConfig.PopulateIndexStats is set.
	IndexStats map[string]BulkIndexerIndexStat
	// RetriedDocs contains the total number of retried documents.
	RetriedDocs int64
	// FailureStoreDocs contains failure store specific document stats.
	FailureStoreDocs FailureStoreDocsStat
	// GreatestRetry contains the greatest observed retry count in the entire
	// bulk request.
	GreatestRetry int
	// FailedDocs contains the failed documents.
	FailedDocs []BulkIndexerResponseItem
}

// FailureStoreDocsStat contains the number of documents per failure store status.
type FailureStoreDocsStat struct {
	// Used contains the total number of documents indexed to failure store.
	Used int64
	// Failed contains the total number of documents which failed when indexed to failure store.
	Failed int64
	// NotEnabled contains the total number of documents which could have been indexed to failure store
	// if it was enabled.
	NotEnabled int64
}

// add counts a document with the given failure store status. For the stats track
// only actionable explicit failure store statuses "used", "failed" and "not_enabled".
func (s *FailureStoreDocsStat) add(status FailureStoreStatus) {
	switch status {
	case FailureStoreStatusUsed:
		s.Used++
	case FailureStoreStatusFailed:
		s.Failed++
	case FailureStoreStatusNotEnabled:
		s.NotEnabled++
	}
}

// BulkIndexerIndexStat holds the document counts of a single destination index.
type BulkIndexerIndexStat struct {
	// Indexed contains the number of successfully indexed documents.
	// It includes the documents indexed to the failure store.
	Indexed int64
	// Failed contains the number of documents that failed permanently,
	// i.e. those in BulkIndexerResponseStat.FailedDocs. Documents that
	// are pending a retry are not included.
	Failed int64
	// FailureStoreDocs contains failure store specific document stats.
	FailureStoreDocs FailureStoreDocsStat
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

	Input string `json:"-"`
}

// FailureStoreStatus defines enumeration type for all known failure store statuses.
type FailureStoreStatus string

const (
	// FailureStoreStatusUnknown implicit status which represents that there is no information about
	// this response or that the failure store is not applicable.
	FailureStoreStatusUnknown FailureStoreStatus = "not_applicable_or_unknown"
	// FailureStoreStatusUsed status which represents that this document was stored in the failure store successfully.
	FailureStoreStatusUsed FailureStoreStatus = "used"
	// FailureStoreStatusFailed status which represents that this document was rejected from the failure store.
	FailureStoreStatusFailed FailureStoreStatus = "failed"
	// FailureStoreStatusNotEnabled status which represents that this document was rejected, but
	// it could have ended up in the failure store if it was enabled.
	FailureStoreStatusNotEnabled FailureStoreStatus = "not_enabled"
)

func init() {
	jsoniter.RegisterTypeDecoderFunc("docappender.BulkIndexerResponseStat", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		_, iter.Error = decodeBulkResponse(iter, (*BulkIndexerResponseStat)(ptr), false)
	})
}

// decodeBulkResponse reads through the closing brace. On error, resp may hold partial counts.
func decodeBulkResponse(iter *jsoniter.Iterator, resp *BulkIndexerResponseStat, requireStatus bool) (int, error) {
	var items int
	var seenItems bool
	var err error
	iter.ReadMapCB(func(i *jsoniter.Iterator, field string) bool {
		if field != "items" {
			i.Skip()
			return i.Error == nil
		}
		if seenItems {
			err = errors.New("response has more than one items field")
			return false
		}
		seenItems = true
		if i.WhatIsNext() != jsoniter.ArrayValue {
			err = errors.New("items is not a JSON array")
			return false
		}
		return i.ReadArrayCB(func(i *jsoniter.Iterator) bool {
			item := BulkIndexerResponseItem{Position: items}
			failureStore, itemErr := decodeBulkItem(i, &item)
			err = itemErr
			if err != nil || i.Error != nil {
				return false
			}
			if requireStatus && item.Status == 0 {
				err = fmt.Errorf("item %d has no status", items)
				return false
			}
			items++

			// Count failure store results after decoding the index name.
			resp.FailureStoreDocs.add(failureStore)
			if m := resp.IndexStats; m != nil && failureStore != "" {
				s := m[item.Index]
				s.FailureStoreDocs.add(failureStore)
				m[item.Index] = s
			}
			if item.Error.Type != "" || item.Status > 201 {
				resp.FailedDocs = append(resp.FailedDocs, item)
			} else {
				resp.Indexed++
				if m := resp.IndexStats; m != nil {
					s := m[item.Index]
					s.Indexed++
					m[item.Index] = s
				}
			}
			return true
		})
	})
	if iter.Error != nil {
		return items, iter.Error
	}
	return items, err
}

func decodeBulkItem(iter *jsoniter.Iterator, item *BulkIndexerResponseItem) (FailureStoreStatus, error) {
	var failureStore FailureStoreStatus
	var seenAction bool
	var err error
	ok := iter.ReadMapCB(func(i *jsoniter.Iterator, _ string) bool {
		if seenAction {
			err = fmt.Errorf("item %d has more than one action", item.Position)
			return false
		}
		seenAction = true
		if i.WhatIsNext() != jsoniter.ObjectValue {
			err = fmt.Errorf("item %d action result is not a JSON object", item.Position)
			return false
		}
		return i.ReadMapCB(func(i *jsoniter.Iterator, field string) bool {
			switch field {
			case "_index":
				item.Index = i.ReadString()
			case "status":
				if item.Status != 0 {
					err = fmt.Errorf("item %d has more than one status field", item.Position)
					return false
				}
				item.Status = i.ReadInt()
				if item.Status < 200 || item.Status > 599 {
					err = fmt.Errorf("item %d has invalid status %d", item.Position, item.Status)
					return false
				}
			case "failure_store":
				failureStore = FailureStoreStatus(i.ReadString())
			case "error":
				return i.ReadMapCB(func(i *jsoniter.Iterator, field string) bool {
					switch field {
					case "type":
						item.Error.Type = i.ReadString()
					case "reason":
						item.Error.Reason = i.ReadString()
					default:
						i.Skip()
					}
					return i.Error == nil
				})
			default:
				i.Skip()
			}
			return i.Error == nil
		})
	})
	if ok && !seenAction {
		err = fmt.Errorf("item %d has no action", item.Position)
	}
	return failureStore, err
}

// bulkResponseReader saves the first read error, including one returned with data.
// jsoniter ignores read errors returned with data and can include body text in parse errors.
type bulkResponseReader struct {
	io.Reader
	err error
}

func (r *bulkResponseReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.Reader.Read(p)
	r.err = err
	return n, err
}

func readBulkResponse(body io.Reader, resp *BulkIndexerResponseStat, requireStatus, redact bool) (int, error) {
	r := &bulkResponseReader{Reader: body}
	iter := jsoniter.Parse(jsoniter.ConfigDefault, r, 512)
	var items int
	var err error
	if iter.WhatIsNext() == jsoniter.ObjectValue {
		items, err = decodeBulkResponse(iter, resp, requireStatus)
	} else {
		err = errors.New("response body is not a JSON object")
	}
	if err == nil {
		// Read to EOF to reject extra data and catch errors after the closing brace.
		iter.WhatIsNext()
		if iter.Error != io.EOF {
			err = errors.New("unexpected data after response object")
		}
	}
	if r.err != nil && r.err != io.EOF {
		return items, r.err
	}
	if redact && err != nil && iter.Error != nil {
		return items, errors.New("invalid JSON in response body")
	}
	return items, err
}

type countWriter struct {
	bytesWritten int
	io.Writer
}

func (cw *countWriter) Write(p []byte) (int, error) {
	cw.bytesWritten += len(p)
	return cw.Writer.Write(p)
}

// NewBulkIndexer returns a bulk indexer that issues bulk requests to Elasticsearch.
// It is only tested with v8 go-elasticsearch client. Use other clients at your own risk.
// The returned BulkIndexer is NOT safe for concurrent use by multiple goroutines.
func NewBulkIndexer(cfg BulkIndexerConfig) (*BulkIndexer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newBulkIndexer(cfg), nil
}

func newBulkIndexer(cfg BulkIndexerConfig) *BulkIndexer {
	b := &BulkIndexer{
		config:            cfg,
		requireDataStream: cfg.RequireDataStream,
	}

	// use a len check instead of a nil check because document level retries
	// should be disabled using MaxDocumentRetries instead.
	if len(b.config.RetryOnDocumentStatus) == 0 {
		b.config.RetryOnDocumentStatus = []int{http.StatusTooManyRequests}
	}
	var writer io.Writer
	if cfg.CompressionLevel != gzip.NoCompression {
		b.gzipw, _ = gzip.NewWriterLevel(&b.buf, cfg.CompressionLevel)
		writer = b.gzipw
	} else {
		writer = &b.buf
	}
	b.writer = &countWriter{0, writer}
	return b
}

// Reset resets bulk indexer, ready for a new request.
func (b *BulkIndexer) Reset() {
	b.bytesFlushed = 0
	b.bytesUncompFlushed = 0
}

// SetClient resets the client used by the bulk indexer.
func (b *BulkIndexer) SetClient(client elastictransport.Interface) {
	b.config.Client = client
}

// ResetBuf resets the internal ndjson buffer and item counter so that the
// BulkIndexer can be reused without discarding the configuration.  Unlike
// Flush, this does NOT send any buffered items to Elasticsearch.  It is
// intended for use in benchmarks and tests.
func (b *BulkIndexer) ResetBuf() {
	b.resetBuf()
}

// resetBuf discards the buffered documents and their retry counts.
func (b *BulkIndexer) resetBuf() {
	b.itemsAdded = 0
	b.deletes = nil
	b.retryCounts = nil
	b.writer.bytesWritten = 0
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

// UncompressedLen returns the number of uncompressed buffered bytes.
func (b *BulkIndexer) UncompressedLen() int {
	return b.writer.bytesWritten
}

// BytesFlushed returns the number of bytes flushed by the bulk indexer.
func (b *BulkIndexer) BytesFlushed() int {
	return b.bytesFlushed
}

// BytesUncompressedFlushed returns the number of uncompressed bytes flushed by the bulk indexer.
func (b *BulkIndexer) BytesUncompressedFlushed() int {
	return b.bytesUncompFlushed
}

type BulkIndexerItem struct {
	Index             string
	DocumentID        string
	Pipeline          string
	Action            string
	Body              io.WriterTo
	DynamicTemplates  map[string]string
	RequireDataStream bool
}

// Add encodes an item in the buffer.
func (b *BulkIndexer) Add(item BulkIndexerItem) error {
	action := item.Action
	if action == "" {
		action = ActionCreate
	}

	switch action {
	case ActionCreate, ActionDelete, ActionIndex, ActionUpdate:
	default:
		return fmt.Errorf("%s is not a valid action", action)
	}

	b.writeMeta(
		item.Index,
		item.DocumentID,
		item.Pipeline,
		action,
		item.DynamicTemplates,
		item.RequireDataStream,
	)
	if action == ActionDelete {
		// The bulk API expects no document line for delete actions, so
		// item.Body (which may be nil) is not written.
		b.deletes = append(b.deletes, b.itemsAdded)
	} else {
		if _, err := item.Body.WriteTo(b.writer); err != nil {
			return fmt.Errorf("failed to write bulk indexer item: %w", err)
		}
		if _, err := b.writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}
	b.itemsAdded++
	return nil
}

func (b *BulkIndexer) writeMeta(
	index, documentID, pipeline, action string,
	dynamicTemplates map[string]string,
	requireDataStream bool,
) {
	writeItemMetaTo(
		&b.jsonw,
		b.writer,
		index,
		documentID,
		pipeline,
		action,
		dynamicTemplates,
		requireDataStream,
	)
}

func (b *BulkIndexer) newBulkIndexRequest(ctx context.Context) (*http.Request, error) {
	// We should not pass the original b.buf bytes.Buffer down to the client/http layer because
	// the indexer will reuse the buffer. The underlying http client/transport implementation may keep
	// reading from the buffer after the request is done and the call to `req.Do` has returned.
	// This may happen in HTTP error cases when the server isn't required to read the full
	// request body before sending a response.
	// This can cause undefined behavior (and panics) due to concurrent reads/writes to bytes.Buffer
	// internal member variables (b.buf.off, b.buf.lastRead).
	// See: https://github.com/golang/go/issues/51907
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_bulk", bytes.NewReader(b.buf.Bytes()))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add(HeaderEventCount, strconv.Itoa(b.itemsAdded))
	req.Header.Add(HeaderUncompressedLength, strconv.Itoa(b.UncompressedLen()))
	v := req.URL.Query()

	for key, valSlice := range b.config.QueryParams {
		for _, value := range valSlice {
			v.Add(key, value)
		}
	}

	if b.config.Pipeline != "" {
		v.Set("pipeline", b.config.Pipeline)
	}
	if b.config.RequireDataStream {
		v.Set("require_data_stream", strconv.FormatBool(b.config.RequireDataStream))
	}
	if b.config.FilterPath == "" {
		v.Set("filter_path", DefaultFilterPath)
	} else {
		v.Set("filter_path", b.config.FilterPath)
	}
	if b.config.IncludeSourceOnError != Unset {
		switch b.config.IncludeSourceOnError {
		case False:
			v.Set("include_source_on_error", "false")
		case True:
			v.Set("include_source_on_error", "true")
		}
	}

	req.URL.RawQuery = v.Encode()

	return req, nil
}

// AddBuffer appends all items from buf into the BulkIndexer's internal ndjson
// stream.  Items are accumulated in the buffer (zero intermediate allocation),
// and when they are ready to be sent to Elasticsearch, AddBuffer loads them
// into the indexer in a single write before calling Flush.
func (b *BulkIndexer) AddBuffer(buf BulkIndexerBuffer) error {
	if buf.ItemCount() == 0 {
		return nil
	}
	if _, err := buf.WriteTo(b.writer); err != nil {
		return fmt.Errorf("failed to write bulk indexer buffer: %w", err)
	}
	b.itemsAdded += buf.ItemCount()
	return nil
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

	req, err := b.newBulkIndexRequest(ctx)
	if err != nil {
		return BulkIndexerResponseStat{}, fmt.Errorf("failed to create bulk index request: %w", err)
	}

	if b.gzipw != nil {
		req.Header.Set("Content-Encoding", "gzip")
	}

	sentItems := b.itemsAdded
	bytesFlushed := b.buf.Len()
	bytesUncompFlushed := b.writer.bytesWritten
	res, err := b.config.Client.Perform(req)
	if err != nil {
		b.resetBuf()
		return BulkIndexerResponseStat{}, fmt.Errorf("failed to execute the request: %w", err)
	}
	defer res.Body.Close()

	// Failed documents may need to be read from the sent request body.
	bodyBuf := b.buf.Bytes()
	// Keep the retry counts of the sent documents, by position in the bulk
	// request, before resetBuf drops them.
	sentRetryCounts := b.retryCounts
	// positions of the delete items in the original bulk request body
	sentDeletes := b.deletes

	// Reset the buffer and gzip writer so they can be reused in case
	// document level retries are needed.
	b.resetBuf()

	b.bytesFlushed = bytesFlushed
	b.bytesUncompFlushed = bytesUncompFlushed
	redact := b.config.IncludeSourceOnError == Unset
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return BulkIndexerResponseStat{}, flushResponseError(res, redact)
	}

	var resp BulkIndexerResponseStat
	if b.config.PopulateIndexStats {
		resp.IndexStats = make(map[string]BulkIndexerIndexStat)
	}
	// A custom filter path can omit items or their status.
	fullResponse := b.config.FilterPath == ""
	decodedItems, err := readBulkResponse(res.Body, &resp, fullResponse, redact)
	if err != nil {
		return BulkIndexerResponseStat{}, fmt.Errorf("error decoding bulk response: %w", err)
	}
	if decodedItems > sentItems || (fullResponse && decodedItems != sentItems) {
		return BulkIndexerResponseStat{}, fmt.Errorf(
			"invalid bulk response: item count %d does not match document count %d", decodedItems, sentItems,
		)
	}

	if redact {
		for i := range resp.FailedDocs {
			resp.FailedDocs[i].Error.Reason = ""
		}
	}

	if b.config.MaxDocumentRetries > 0 || b.config.PopulateFailedDocsInput {
		if len(resp.FailedDocs) > 0 {
			// Retries write to b.buf, so the scan needs its own copy of the request.
			buf := bytes.Clone(bodyBuf)
			var gr *gzip.Reader
			var scratch []byte
			if b.gzipw != nil {
				gr, err = gzip.NewReader(bytes.NewReader(buf))
				if err != nil {
					return BulkIndexerResponseStat{}, fmt.Errorf("failed to decompress request body: %w", err)
				}
				defer gr.Close()
				buf = nil
				scratch = make([]byte, 4096)
			}

			// Failed documents arrive in request order, so scan the body once.
			// Write a contiguous slice for the part of each chunk to copy.
			line := 0
			advance := func(to io.Writer, targetLine int) error {
				for line < targetLine {
					if len(buf) == 0 {
						if gr == nil {
							return io.ErrUnexpectedEOF
						}
						n, err := gr.Read(scratch)
						if err == io.EOF && n == 0 {
							return io.ErrUnexpectedEOF
						}
						if err != nil && err != io.EOF {
							return err
						}
						buf = scratch[:n]
					}

					end := 0
					for line < targetLine {
						n := bytes.IndexByte(buf[end:], '\n')
						if n == -1 {
							end = len(buf)
							break
						}
						end += n + 1
						line++
					}
					if to != nil {
						if _, err := to.Write(buf[:end]); err != nil {
							return err
						}
					}
					buf = buf[end:]
				}
				return nil
			}

			// Each document uses two lines: the action and the source.
			writeItemAtPos := func(to io.Writer, pos int) error {
				deletesBefore, isDelete := deletesBeforePos(sentDeletes, pos)
				startln := pos*2 - deletesBefore
				endln := startln + 2
				if isDelete {
					endln = startln + 1
				}
				err := advance(nil, startln)
				if err == nil {
					err = advance(to, endln)
				}
				if err != nil {
					return fmt.Errorf("failed to extract document at position %d: %w", pos, err)
				}
				return nil
			}

			var inputBuf bytes.Buffer
			nonRetriable := resp.FailedDocs[:0]
			appendNonRetriable := func(item BulkIndexerResponseItem) error {
				if b.config.PopulateFailedDocsInput {
					inputBuf.Reset()
					if err := writeItemAtPos(&inputBuf, item.Position); err != nil {
						return err
					}
					item.Input = inputBuf.String()
				}
				nonRetriable = append(nonRetriable, item)
				return nil
			}

			for _, item := range resp.FailedDocs {
				if b.config.MaxDocumentRetries > 0 && b.shouldRetryOnStatus(item.Status) {
					count := 1
					if item.Position < len(sentRetryCounts) {
						count += sentRetryCounts[item.Position]
					}
					// check if we are above the maxDocumentRetry setting
					if count > b.config.MaxDocumentRetries {
						// do not retry, return the document as failed
						if err := appendNonRetriable(item); err != nil {
							b.resetBuf()
							return BulkIndexerResponseStat{}, err
						}
						continue
					}
					if err := writeItemAtPos(b.writer, item.Position); err != nil {
						b.resetBuf()
						return BulkIndexerResponseStat{}, err
					}
					if _, isDelete := deletesBeforePos(sentDeletes, item.Position); isDelete {
						b.deletes = append(b.deletes, b.itemsAdded)
					}
					// Record the count only for a document that is in the buffer.
					b.retryCounts = append(b.retryCounts, count)
					b.itemsAdded++
					resp.RetriedDocs++
					if resp.GreatestRetry < count {
						resp.GreatestRetry = count
					}
				} else {
					// If it's not a retriable error, treat the document as failed
					if err := appendNonRetriable(item); err != nil {
						b.resetBuf()
						return BulkIndexerResponseStat{}, err
					}
				}
			}

			resp.FailedDocs = nonRetriable
		}
	}

	// Failures are counted here, rather than in the response decoder, so that
	// documents pending a retry are not included.
	if resp.IndexStats != nil {
		for _, doc := range resp.FailedDocs {
			s := resp.IndexStats[doc.Index]
			s.Failed++
			resp.IndexStats[doc.Index] = s
		}
	}

	return resp, nil
}

func flushResponseError(res *http.Response, redact bool) error {
	e := ErrorFlushFailed{statusCode: res.StatusCode}
	switch {
	case res.StatusCode == 429:
		e.tooMany = true
	case res.StatusCode >= 500:
		e.serverError = true
	case res.StatusCode >= 400:
		e.clientError = true
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return errors.Join(e, fmt.Errorf("failed to read response body: %w", err))
	}
	if !redact {
		e.resp = string(body)
		return e
	}

	var redacted struct {
		Error struct {
			Type     string `json:"type,omitempty"`
			CausedBy struct {
				Type string `json:"type,omitempty"`
			} `json:"caused_by,omitempty"`
		} `json:"error,omitempty"`
	}
	if json.Unmarshal(body, &redacted) == nil {
		body, _ = json.Marshal(&redacted)
		e.resp = string(body)
	}
	return e
}

func (b *BulkIndexer) shouldRetryOnStatus(docStatus int) bool {
	for _, status := range b.config.RetryOnDocumentStatus {
		if docStatus == status {
			return true
		}
	}
	return false
}

// deletesBeforePos returns the number of delete items located before pos in
// the sorted deletes slice, and whether the item at pos is itself a delete.
func deletesBeforePos(deletes []int, pos int) (before int, isDelete bool) {
	before = sort.SearchInts(deletes, pos)
	return before, before < len(deletes) && deletes[before] == pos
}

type ErrorFlushFailed struct {
	resp        string
	statusCode  int
	tooMany     bool
	clientError bool
	serverError bool
}

func (e ErrorFlushFailed) StatusCode() int {
	return e.statusCode
}

func (e ErrorFlushFailed) ResponseBody() string {
	return e.resp
}

func (e ErrorFlushFailed) Error() string {
	return fmt.Sprintf("flush failed (%d): %s", e.statusCode, e.resp)
}

// WriteItemMeta encodes the Elasticsearch bulk metadata line for item into w,
// including the trailing newline. It returns the number of bytes written.
//
// This is the authoritative encoding of the Elasticsearch bulk API meta format.
// External callers (e.g. an exporter that buffers items independently) should
// use this function rather than duplicating the encoding logic.
//
// An empty item.Action defaults to ActionCreate. An unrecognised action returns
// an error and nothing is written to w.
func WriteItemMeta(w io.Writer, item BulkIndexerItem) (int, error) {
	action := item.Action
	if action == "" {
		action = ActionCreate
	}
	switch action {
	case ActionCreate, ActionDelete, ActionIndex, ActionUpdate:
	default:
		return 0, fmt.Errorf("%s is not a valid action", action)
	}
	var jsonw fastjson.Writer
	return writeItemMetaTo(&jsonw, w, item.Index, item.DocumentID, item.Pipeline, action, item.DynamicTemplates, item.RequireDataStream)
}

// writeItemMetaTo is the shared encoding kernel used by both WriteItemMeta and
// BulkIndexer.writeMeta. Callers supply their own *fastjson.Writer so that
// hot-path callers (BulkIndexer) can reuse the writer across items without
// allocating on every call.
func writeItemMetaTo(
	jsonw *fastjson.Writer,
	w io.Writer,
	index, documentID, pipeline, action string,
	dynamicTemplates map[string]string,
	requireDataStream bool,
) (int, error) {
	jsonw.RawString(`{"`)
	jsonw.RawString(action)
	jsonw.RawString(`":{`)

	first := true
	if documentID != "" {
		jsonw.RawString(`"_id":`)
		jsonw.String(documentID)
		first = false
	}
	if index != "" {
		if !first {
			jsonw.RawByte(',')
		}
		jsonw.RawString(`"_index":`)
		jsonw.String(index)
		first = false
	}
	if pipeline != "" {
		if !first {
			jsonw.RawByte(',')
		}
		jsonw.RawString(`"pipeline":`)
		jsonw.String(pipeline)
		first = false
	}
	if len(dynamicTemplates) > 0 {
		if !first {
			jsonw.RawByte(',')
		}
		jsonw.RawString(`"dynamic_templates":{`)
		firstDynamicTemplate := true
		for k, v := range dynamicTemplates {
			if !firstDynamicTemplate {
				jsonw.RawByte(',')
			}
			jsonw.String(k)
			jsonw.RawByte(':')
			jsonw.String(v)
			firstDynamicTemplate = false
		}
		jsonw.RawByte('}')
		first = false
	}
	if requireDataStream {
		if !first {
			jsonw.RawByte(',')
		}
		jsonw.RawString(`"require_data_stream":true`)
	}
	jsonw.RawString("}}\n")
	n, err := w.Write(jsonw.Bytes())
	jsonw.Reset()
	return n, err
}
