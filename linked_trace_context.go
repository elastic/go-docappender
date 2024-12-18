package docappender

import (
	"go.elastic.co/apm/v2"
	"go.opentelemetry.io/otel/trace"
)

type LinkedTraceContext struct {
	TraceID [16]byte
	SpanID  [8]byte
}

func (c LinkedTraceContext) APMLink() apm.SpanLink {
	return apm.SpanLink{Trace: c.TraceID, Span: c.SpanID}
}

func (c LinkedTraceContext) OTELLink() trace.Link {
	return trace.Link{SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: c.TraceID,
		SpanID:  c.SpanID,
	})}
}

func NewLinkedTraceContextFromAPM(ctx apm.TraceContext) LinkedTraceContext {
	return LinkedTraceContext{
		TraceID: ctx.Trace,
		SpanID:  ctx.Span,
	}
}

func NewLinkedTraceIDFromOTEL(ctx trace.SpanContext) LinkedTraceContext {
	return LinkedTraceContext{
		TraceID: ctx.TraceID(),
		SpanID:  ctx.SpanID(),
	}
}
