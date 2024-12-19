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
	"go.elastic.co/apm/v2"
	"go.opentelemetry.io/otel/trace"
)

type linkedTraceContext struct {
	TraceID [16]byte
	SpanID  [8]byte
}

func (c linkedTraceContext) APMLink() apm.SpanLink {
	return apm.SpanLink{Trace: c.TraceID, Span: c.SpanID}
}

func (c linkedTraceContext) OTELLink() trace.Link {
	return trace.Link{SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: c.TraceID,
		SpanID:  c.SpanID,
	})}
}

func newLinkedTraceContextFromAPM(ctx apm.TraceContext) *linkedTraceContext {
	if err := ctx.Trace.Validate(); err != nil {
		return nil
	}
	return &linkedTraceContext{
		TraceID: ctx.Trace,
		SpanID:  ctx.Span,
	}
}

func newLinkedTraceIDFromOTEL(ctx trace.SpanContext) *linkedTraceContext {
	if !ctx.HasTraceID() || !ctx.HasSpanID() {
		return nil
	}
	return &linkedTraceContext{
		TraceID: ctx.TraceID(),
		SpanID:  ctx.SpanID(),
	}
}
