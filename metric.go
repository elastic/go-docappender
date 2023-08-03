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
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	bufferDuration metric.Float64Histogram
	flushDuration  metric.Float64Histogram

	bulkRequests          metric.Int64Counter
	docsAdded             metric.Int64Counter
	docsActive            metric.Int64Counter
	docsFailed            metric.Int64Counter
	docsFailedClient      metric.Int64Counter
	docsFailedServer      metric.Int64Counter
	docsIndexed           metric.Int64Counter
	tooManyRequests       metric.Int64Counter
	bytesTotal            metric.Int64Counter
	availableBulkRequests metric.Int64Counter
	activeCreated         metric.Int64Counter
	activeDestroyed       metric.Int64Counter
}

const (
	metricBufferDuration   = "elasticsearch.buffer.latency"
	metricFlushDuration    = "elasticsearch.flushed.latency"
	metricBulkRequests     = "elasticsearch.bulk_requests.count"
	metricDocsAdded        = "elasticsearch.events.count"
	metricDocsActive       = "elasticsearch.events.queued"
	metricDocsFailed       = "elasticsearch.failed.count"
	metricDocsFailedClient = "elasticsearch.failed.client.count"
	metricDocsFailedServer = "elasticsearch.failed.server.count"
	metricDocsIndexed      = "elasticsearch.events.processed"
	metricTooManyReq       = "elasticsearch.failed.too_many_reqs"
	metricBytesTotal       = "elasticsearch.flushed.bytes"
	metricAvailableBulkReq = "elasticsearch.bulk_requests.available"
	metricActiveCreated    = "elasticsearch.indexer.created"
	metricActiveDestroyed  = "elasticsearch.indexer.destroyed"
)

func newMetrics(cfg Config) (metrics, error) {
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	meter := cfg.MeterProvider.Meter("github.com/elastic/go-docappender")

	var errs []error
	ms := metrics{
		bufferDuration: newFloat64Histogram(
			meter,
			metricBufferDuration,
			"The amount of time a document was buffered for, in seconds.",
			"s",
			&errs,
		),
		flushDuration: newFloat64Histogram(
			meter,
			metricFlushDuration,
			"The amount of time a _bulk request took, in seconds.",
			"s",
			&errs,
		),
		bulkRequests: newInt64Counter(
			meter,
			metricBulkRequests,
			"The number of bulk requests completed.",
			"1",
			&errs),
		docsAdded: newInt64Counter(
			meter,
			metricDocsAdded,
			"the total number of items added to the indexer.",
			"1",
			&errs),
		docsActive: newInt64Counter(
			meter,
			metricDocsActive,
			"the number of active items waiting in the indexer's queue.",
			"1",
			&errs),
		docsFailed: newInt64Counter(
			meter,
			metricDocsFailed,
			"The number of indexing operations that failed",
			"1",
			&errs),
		docsFailedClient: newInt64Counter(
			meter,
			metricDocsFailedClient,
			"The number of docs failed to get indexed with client error(status_code >= 400 < 500, but not 429).",
			"1",
			&errs),
		docsFailedServer: newInt64Counter(
			meter,
			metricDocsFailedServer,
			"The number of docs failed to get indexed with server error(status_code >= 500).",
			"1",
			&errs),
		docsIndexed: newInt64Counter(
			meter,
			metricDocsIndexed,
			"The number of docs indexed successfully.",
			"1",
			&errs),
		tooManyRequests: newInt64Counter(
			meter,
			metricTooManyReq,
			"The number of 429 errors returned from the bulk indexer due to too many requests.",
			"1",
			&errs),
		bytesTotal: newInt64Counter(
			meter,
			metricBytesTotal,
			"The total number of bytes written to the request body",
			"by",
			&errs),
		availableBulkRequests: newInt64Counter(
			meter,
			metricAvailableBulkReq,
			"The number of bulk indexers available for making bulk index requests.",
			"1",
			&errs),
		activeCreated: newInt64Counter(
			meter,
			metricActiveCreated,
			"The number of active indexer creations.",
			"1",
			&errs),
		activeDestroyed: newInt64Counter(
			meter,
			metricActiveDestroyed,
			"The number of times an active indexer was destroyed.",
			"1",
			&errs),
	}

	if len(errs) > 0 {
		return ms, fmt.Errorf(
			"failed creating metrics: %w", errors.Join(errs...),
		)
	}

	return ms, nil
}

func newInt64Counter(meter metric.Meter, name string, description string, unit string, errs *[]error) metric.Int64Counter {
	m, err := meter.Int64Counter(
		name,
		metric.WithUnit(unit),
		metric.WithDescription(description),
	)

	if err != nil {
		*errs = append(*errs, fmt.Errorf(
			"failed creating %s metric: %w", name, err,
		))
	}
	return m
}

func newFloat64Histogram(meter metric.Meter, name string, description string, unit string, errors *[]error) metric.Float64Histogram {
	m, err := meter.Float64Histogram(
		name,
		metric.WithUnit(unit),
		metric.WithDescription(description),
	)

	if err != nil {
		*errors = append(*errors, fmt.Errorf(
			"failed creating %s metric: %w", name, err,
		))
	}
	return m
}
