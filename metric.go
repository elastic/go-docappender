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
	"context"
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	bufferDuration        metric.Float64Histogram
	flushDuration         metric.Float64Histogram
	bulkRequests          metric.Int64Counter
	docsAdded             metric.Int64Counter
	docsActive            metric.Int64Counter
	bytesTotal            metric.Int64Counter
	availableBulkRequests metric.Int64Counter
	activeCreated         metric.Int64Counter
	activeDestroyed       metric.Int64Counter

	// attributes for docsProcessed metric
	docsIndexed      int64
	docsFailedClient int64
	docsFailedServer int64
	tooManyRequests  int64
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

func newMetrics(cfg Config) (*metrics, metric.Registration, error) {
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
			return &ms, nil, err
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
			description: "Number of APM Events received for indexing",
			p:           &ms.docsAdded,
		},
		{
			name:        "elasticsearch.events.queued",
			description: "the number of active items waiting in the indexer's queue.",
			p:           &ms.docsActive,
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
			return &ms, nil, err
		}
	}

	elasticsearchEventsProcessed, err := meter.Int64ObservableCounter(
		"elasticsearch.events.processed",
		metric.WithUnit("1"),
		metric.WithDescription("Number of APM Events flushed to Elasticsearch. Dimensions are used to report the project ID, success or failures"),
	)
	if err != nil {
		return &ms, nil, fmt.Errorf("elasticsearch: failed to create metric for elasticsearch processed events: %w", err)
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		pattrs := metric.WithAttributeSet(cfg.MetricAttributes)
		obs.ObserveInt64(elasticsearchEventsProcessed, atomic.LoadInt64(&ms.docsIndexed),
			pattrs,
			metric.WithAttributes(
				attribute.String("status", "Success"), // Create action.
			))
		obs.ObserveInt64(elasticsearchEventsProcessed, atomic.LoadInt64(&ms.docsFailedClient),
			pattrs, metric.WithAttributes(
				attribute.String("status", "FailedClient"), // Failed
			))
		obs.ObserveInt64(elasticsearchEventsProcessed, atomic.LoadInt64(&ms.docsFailedServer),
			pattrs,
			metric.WithAttributes(
				attribute.String("status", "FailedServer"), // Failed
			))
		obs.ObserveInt64(elasticsearchEventsProcessed, atomic.LoadInt64(&ms.tooManyRequests),
			pattrs,
			metric.WithAttributes(
				attribute.String("status", "TooMany"), // TooMany
			))
		return nil
	},
		elasticsearchEventsProcessed,
	)

	if err != nil {
		return &ms, nil, fmt.Errorf("elasticsearch: failed to register metric callback: %w", err)
	}
	fmt.Println("successfully registered callback")

	return &ms, reg, nil
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
