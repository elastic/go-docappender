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
	"time"

	"go.elastic.co/apm/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Config holds configuration for Appender.
type Config struct {
	// Logger holds an optional Logger to use for logging indexing requests.
	//
	// All Elasticsearch errors will be logged at error level, so in cases
	// where the indexer is used for high throughput indexing, is recommended
	// that a rate-limited logger is used.
	//
	// If Logger is nil, logging will be disabled.
	Logger *zap.Logger

	// Tracer holds an optional apm.Tracer to use for tracing bulk requests
	// to Elasticsearch. Each bulk request is traced as a transaction.
	//
	// If Tracer is nil, requests will not be traced.
	Tracer *apm.Tracer

	// CompressionLevel holds the gzip compression level, from 0 (gzip.NoCompression)
	// to 9 (gzip.BestCompression). Higher values provide greater compression, at a
	// greater cost of CPU. The special value -1 (gzip.DefaultCompression) selects the
	// default compression level.
	CompressionLevel int

	// MaxRequests holds the maximum number of bulk index requests to execute concurrently.
	// The maximum memory usage of Appender is thus approximately MaxRequests*FlushBytes.
	//
	// If MaxRequests is less than or equal to zero, the default of 10 will be used.
	MaxRequests int

	// MaxDocumentRetries holds the maximum number of document retries
	MaxDocumentRetries int

	// FlushBytes holds the flush threshold in bytes. If Compression is enabled,
	// The number of documents that can be buffered will be greater.
	//
	// If FlushBytes is zero, the default of 1MB will be used.
	FlushBytes int

	// FlushInterval holds the flush threshold as a duration.
	//
	// If FlushInterval is zero, the default of 30 seconds will be used.
	FlushInterval time.Duration

	// FlushTimeout holds the flush timeout as a duration.
	//
	// If FlushTimeout is zero, no timeout will be used.
	FlushTimeout time.Duration

	// DocumentBufferSize sets the number of documents that can be buffered before
	// they are stored in the active indexer buffer.
	//
	// If DocumentBufferSize is zero, the default 1024 will be used.
	DocumentBufferSize int

	// Pipeline holds the ingest pipeline ID.
	//
	// If Pipeline is empty, no ingest pipeline will be specified in the Bulk request.
	Pipeline string

	// Scaling configuration for the docappender.
	//
	// If unspecified, scaling is enabled by default.
	Scaling ScalingConfig

	// MeterProvider holds the OTel MeterProvider to be used to create and
	// record appender metrics.
	//
	// If unset, the global OTel MeterProvider will be used, if that is unset,
	// no metrics will be recorded.
	MeterProvider metric.MeterProvider

	// MetricAttributes holds any extra attributes to set in the recorded
	// metrics.
	MetricAttributes attribute.Set
}

// ScalingConfig holds the docappender autoscaling configuration.
type ScalingConfig struct {
	// Disabled toggles active indexer scaling on.
	//
	// It is enabled by default.
	Disabled bool

	// ActiveRatio defines the threshold for (potential) active indexers to
	// GOMAXPROCS. The higher the number, the more potential active indexers
	// there will be actively pulling from the BulkIndexerItem channel.
	// For example, when ActiveRatio:1 and GOMAXPROCS:2, there can be a max
	// of 2 active indexers, or 1 per GOMAXPROCS.
	// If set to 0.5, the maximum number of active indexers is 1, since.
	// The value must be between 0 and 1.
	//
	// It defaults to 0.25 by default.
	ActiveRatio float64

	// ScaleDown configures the Threshold and CoolDown for the scale down
	// action. In order to scale down an active indexer, the Threshold has
	// to be met after the CoolDown has elapsed. Scale down will only take
	// place if there are more than 1 active indexer.
	// Active indexers will be destroyed when they aren't needed anymore,
	// when enough timed flushes (FlushInterval) are performed by an active
	// indexer (controlled by Threshold), or when an active indexer is idle
	// for (IdleInterval * Threshold) as long as CoolDown allows it.
	//
	// When unset, the default of 30 is used for Threshold, and 30 seconds for
	// CoolDown.
	ScaleDown ScaleActionConfig

	// ScaleUp configures the Threshold and CoolDown for the scale up action.
	//
	// In order for a scale up to occur, the Threshold has to be met after
	// the CoolDown has elapsed. By default, a single active indexer is created
	// which actively pulls items from the internal buffered queue. When enough
	// full flushes (FlushBytes) are performed by an active indexer (controlled
	// by Threshold), a new active indexer will be created until GOMAXPROCS / 4
	// is reached (25% of CPU capacity) if the CoolDown allows it.
	//
	// When unspecified, the default of 60 is used for Threshold, and 60 seconds
	// for CoolDown.
	ScaleUp ScaleActionConfig

	// IdleInterval defines how long an active indexer performs an inactivity
	// check. The ScaleDown.Threshold and ScaleDown.CoolDown needs to be met
	// for an active indexer to be destroyed.
	//
	// When unspecified, the default of 30 seconds will be used.
	IdleInterval time.Duration
}

// ScaleActionConfig holds the configuration for a scaling action
type ScaleActionConfig struct {
	// Threshold is the number of consecutive times a scale up/down condition
	// has to happen for the scaling action will be triggered.
	Threshold uint

	// CoolDown is the amount of time needed to elapse between scaling actions
	// to trigger it.
	CoolDown time.Duration
}
