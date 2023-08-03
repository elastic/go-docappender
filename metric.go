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

type histogramMetric struct {
	name        string
	description string
	unit        string
	p           *metric.Float64Histogram
}

type counterMetric struct {
	name        string
	description string
	unit        string
	p           *metric.Int64Counter
}

func newMetrics(cfg Config) (metrics, error) {
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	meter := cfg.MeterProvider.Meter("github.com/elastic/go-docappender")
	ms := metrics{}
	histograms := []histogramMetric{
		{
			name:        "elasticsearch.buffer.latency",
			description: "The amount of time a document was buffered for, in seconds.",
			unit:        "s",
			p:           &ms.bufferDuration,
		},
		{
			name:        "elasticsearch.flushed.latency",
			description: "The amount of time a _bulk request took, in seconds.",
			unit:        "s",
			p:           &ms.flushDuration,
		},
	}

	for _, m := range histograms {
		err := newFloat64Histogram(meter, m)
		if err != nil {
			return ms, err
		}
	}

	counters := []counterMetric{
		{
			name:        "elasticsearch.bulk_requests.count",
			description: "The number of bulk requests completed.",
			p:           &ms.bulkRequests,
		},
		{
			name:        "elasticsearch.events.count",
			description: "the total number of items added to the indexer.",
			p:           &ms.docsAdded,
		},
		{
			name:        "elasticsearch.events.queued",
			description: "the number of active items waiting in the indexer's queue.",
			p:           &ms.docsActive,
		},
		{
			name:        "elasticsearch.failed.count",
			description: "The amount of time a document was buffered for, in seconds.",
			p:           &ms.docsFailed,
		},
		{
			name:        "elasticsearch.failed.client.count",
			description: "The number of docs failed to get indexed with client error(status_code >= 400 < 500, but not 429).",
			p:           &ms.docsFailedClient,
		},
		{
			name:        "elasticsearch.failed.server.count",
			description: "The number of docs failed to get indexed with server error(status_code >= 500).",
			p:           &ms.docsFailedServer,
		},
		{
			name:        "elasticsearch.events.processed",
			description: "The number of docs indexed successfully.",
			p:           &ms.docsIndexed,
		},
		{
			name:        "elasticsearch.failed.too_many_reqs",
			description: "The number of 429 errors returned from the bulk indexer due to too many requests.",
			p:           &ms.tooManyRequests,
		},
		{
			name:        "elasticsearch.flushed.bytes",
			description: "The total number of bytes written to the request body",
			unit:        "by",
			p:           &ms.bytesTotal,
		},
		{
			name:        "elasticsearch.bulk_requests.available",
			description: "The number of bulk indexers available for making bulk index requests.",
			p:           &ms.availableBulkRequests,
		},
		{
			name:        "elasticsearch.indexer.created",
			description: "The number of active indexer creations.",
			p:           &ms.activeCreated,
		},
		{
			name:        "elasticsearch.indexer.destroyed",
			description: "The number of times an active indexer was destroyed.",
			p:           &ms.activeDestroyed,
		},
	}
	for _, m := range counters {
		err := newInt64Counter(meter, m)
		if err != nil {
			return ms, err
		}
	}

	return ms, nil
}

func newInt64Counter(meter metric.Meter, c counterMetric) error {
	unit := c.unit
	if unit == "" {
		unit = "1"
	}
	m, err := meter.Int64Counter(
		c.name,
		metric.WithUnit(unit),
		metric.WithDescription(c.description),
	)

	if err != nil {
		return fmt.Errorf(
			"failed creating %s metric: %w", c.name, err,
		)
	}
	*c.p = m
	return nil
}

func newFloat64Histogram(meter metric.Meter, h histogramMetric) error {
	m, err := meter.Float64Histogram(
		h.name,
		metric.WithUnit(h.unit),
		metric.WithDescription(h.description),
	)

	if err != nil {
		return fmt.Errorf(
			"failed creating %s metric: %w", h.name, err,
		)
	}
	*h.p = m
	return nil
}
