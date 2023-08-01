package docappender

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
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
	METRIC_NAME_BUFFER_DURATION    = "elasticsearch.buffer.latency"
	METRIC_NAME_FLUSH_DURATION     = "elasticsearch.flushed.latency"
	METRIC_NAME_BULK_REQUESTS      = "elasticsearch.bulk_requests.count"
	METRIC_NAME_DOCS_ADDED         = "elasticsearch.events.count"
	METRIC_NAME_DOCS_ACTIVE        = "elasticsearch.events.queued"
	METRIC_NAME_DOCS_FAILED        = "elasticsearch.failed.count"
	METRIC_NAME_DOCS_FAILED_CLIENT = "elasticsearch.failed.client"
	METRIC_NAME_DOCS_FAILED_SERVER = "elasticsearch.failed.server"
	METRIC_NAME_DOCS_INDEXED       = "elasticsearch.events.processed"
	METRIC_NAME_TOO_MANY_REQ       = "elasticsearch.failed.too_many_reqs"
	METRIC_NAME_BYTES_TOTAL        = "elasticsearch.flushed.bytes"
	METRIC_NAME_AVAILABLE_BULK_REQ = "elasticsearch.bulk_requests.available"
	METRIC_NAME_ACTIVE_CREATED     = "elasticsearch.indexer.created"
	METRIC_NAME_ACTIVE_DESTROYED   = "elasticsearch.indexer.destroyed"
)

func newMetrics(cfg Config) (Metrics, error) {
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	meter := cfg.MeterProvider.Meter("github.com/elastic/go-docappender")

	var errs []error
	ms := Metrics{
		bufferDuration: newFloat64Histogram(
			meter,
			METRIC_NAME_BUFFER_DURATION,
			"The amount of time a document was buffered for, in seconds.",
			"s",
			&errs,
		),
		flushDuration: newFloat64Histogram(
			meter,
			METRIC_NAME_FLUSH_DURATION,
			"The amount of time a _bulk request took, in seconds.",
			"s",
			&errs,
		),
		bulkRequests: newInt64Counter(
			meter,
			METRIC_NAME_BULK_REQUESTS,
			"The number of bulk requests completed.",
			"1",
			&errs),
		docsAdded: newInt64Counter(
			meter,
			METRIC_NAME_DOCS_ADDED,
			"the total number of items added to the indexer.",
			"1",
			&errs),
		docsActive: newInt64Counter(
			meter,
			METRIC_NAME_DOCS_ACTIVE,
			"the number of active items waiting in the indexer's queue.",
			"1",
			&errs),
		docsFailed: newInt64Counter(
			meter,
			METRIC_NAME_DOCS_FAILED,
			"The number of indexing operations that failed",
			"1",
			&errs),
		docsFailedClient: newInt64Counter(
			meter,
			METRIC_NAME_DOCS_FAILED_CLIENT,
			"The number of docs failed to get indexed with client error(status_code >= 400 < 500, but not 429).",
			"1",
			&errs),
		docsFailedServer: newInt64Counter(
			meter,
			METRIC_NAME_DOCS_FAILED_SERVER,
			"The number of docs failed to get indexed with server error(status_code >= 500).",
			"1",
			&errs),
		docsIndexed: newInt64Counter(
			meter,
			METRIC_NAME_DOCS_INDEXED,
			"The number of docs indexed successfully.",
			"1",
			&errs),
		tooManyRequests: newInt64Counter(
			meter,
			METRIC_NAME_TOO_MANY_REQ,
			"The number of 429 errors returned from the bulk indexer due to too many requests.",
			"1",
			&errs),
		bytesTotal: newInt64Counter(
			meter,
			METRIC_NAME_BYTES_TOTAL,
			"The total number of bytes written to the request body",
			"by",
			&errs),
		availableBulkRequests: newInt64Counter(
			meter,
			METRIC_NAME_AVAILABLE_BULK_REQ,
			"The number of bulk indexers available for making bulk index requests.",
			"1",
			&errs),
		activeCreated: newInt64Counter(
			meter,
			METRIC_NAME_ACTIVE_CREATED,
			"The number of active bulk indexers that are concurrently processing batches.",
			"1",
			&errs),
		activeDestroyed: newInt64Counter(
			meter,
			METRIC_NAME_ACTIVE_DESTROYED,
			"Tthe number of times an active indexer was destroyed.",
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
