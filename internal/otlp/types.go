package otlp

import (
	"fmt"
	"sort"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ScopeName is the instrumentation scope every span and record is emitted under.
const ScopeName = "antwatcher"

// ServiceName is the resource service.name of everything antwatcher exports.
const ServiceName = "antwatcher"

// Resource attribute keys set by DefaultResource.
const (
	AttrServiceName    = "service.name"
	AttrServiceVersion = "service.version"
)

// SpanKind mirrors the OTLP span kinds.
type SpanKind int

// Span kinds.
const (
	SpanKindUnspecified SpanKind = iota
	SpanKindInternal
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

// StatusCode mirrors the OTLP span status codes.
type StatusCode int

// Span status codes.
const (
	StatusUnset StatusCode = iota
	StatusOk
	StatusError
)

// Status is the outcome of a span.
type Status struct {
	Code    StatusCode
	Message string
}

// Span is one transport-neutral span. A zero ParentSpanID means a root span.
type Span struct {
	TraceID      [16]byte
	SpanID       [8]byte
	ParentSpanID [8]byte
	Name         string
	Kind         SpanKind
	Start        time.Time
	End          time.Time
	Attributes   map[string]any
	Status       Status
}

// Severity mirrors the OTLP severity numbers.
type Severity int

// Severities (the canonical value of each range).
const (
	SeverityUnspecified Severity = 0
	SeverityTrace       Severity = 1
	SeverityDebug       Severity = 5
	SeverityInfo        Severity = 9
	SeverityWarn        Severity = 13
	SeverityError       Severity = 17
	SeverityFatal       Severity = 21
)

// LogRecord is one transport-neutral log record. Zero TraceID and SpanID are
// omitted from the export.
type LogRecord struct {
	Time         time.Time
	ObservedTime time.Time
	Severity     Severity
	SeverityText string
	Body         string
	Attributes   map[string]any
	TraceID      [16]byte
	SpanID       [8]byte
}

// Resource describes the entity producing the telemetry.
type Resource struct {
	Attributes map[string]any
}

// DefaultResource is the resource antwatcher exports under: service.name and
// service.version.
func DefaultResource(version string) Resource {
	return Resource{Attributes: map[string]any{
		AttrServiceName:    ServiceName,
		AttrServiceVersion: version,
	}}
}

// scopeVersion is the instrumentation scope version: the service version.
func (r Resource) scopeVersion() string {
	v, _ := r.Attributes[AttrServiceVersion].(string)
	return v
}

// ResourceSpans converts spans under res into one OTLP ResourceSpans with a
// single scope named ScopeName.
func ResourceSpans(res Resource, spans []Span) *tracepb.ResourceSpans {
	out := make([]*tracepb.Span, 0, len(spans))
	for i := range spans {
		out = append(out, spans[i].proto())
	}
	return &tracepb.ResourceSpans{
		Resource: res.proto(),
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: &commonpb.InstrumentationScope{Name: ScopeName, Version: res.scopeVersion()},
			Spans: out,
		}},
	}
}

// ResourceLogs converts records under res into one OTLP ResourceLogs with a
// single scope named ScopeName.
func ResourceLogs(res Resource, records []LogRecord) *logspb.ResourceLogs {
	out := make([]*logspb.LogRecord, 0, len(records))
	for i := range records {
		out = append(out, records[i].proto())
	}
	return &logspb.ResourceLogs{
		Resource: res.proto(),
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope:      &commonpb.InstrumentationScope{Name: ScopeName, Version: res.scopeVersion()},
			LogRecords: out,
		}},
	}
}

func (r Resource) proto() *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: Attributes(r.Attributes)}
}

func (s *Span) proto() *tracepb.Span {
	p := &tracepb.Span{
		TraceId:           s.TraceID[:],
		SpanId:            s.SpanID[:],
		Name:              s.Name,
		Kind:              tracepb.Span_SpanKind(s.Kind), //nolint:gosec // same enumeration, values 0..5
		StartTimeUnixNano: unixNano(s.Start),
		EndTimeUnixNano:   unixNano(s.End),
		Attributes:        Attributes(s.Attributes),
		Status: &tracepb.Status{
			Code:    tracepb.Status_StatusCode(s.Status.Code), //nolint:gosec // same enumeration, values 0..2
			Message: s.Status.Message,
		},
	}
	if s.ParentSpanID != [8]byte{} {
		p.ParentSpanId = s.ParentSpanID[:]
	}
	return p
}

func (r *LogRecord) proto() *logspb.LogRecord {
	p := &logspb.LogRecord{
		TimeUnixNano:         unixNano(r.Time),
		ObservedTimeUnixNano: unixNano(r.ObservedTime),
		SeverityNumber:       logspb.SeverityNumber(r.Severity), //nolint:gosec // same enumeration, values 0..24
		SeverityText:         r.SeverityText,
		Body:                 AnyValue(r.Body),
		Attributes:           Attributes(r.Attributes),
	}
	if r.TraceID != [16]byte{} {
		p.TraceId = r.TraceID[:]
	}
	if r.SpanID != [8]byte{} {
		p.SpanId = r.SpanID[:]
	}
	return p
}

// unixNano converts t to OTLP nanoseconds; the zero time is 0 (unset).
func unixNano(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano()) //nolint:gosec // times before 1970 do not occur in webhook payloads
}

// Attributes converts a map to OTLP key/values sorted by key, so the same
// input always produces the same bytes.
func Attributes(m map[string]any) []*commonpb.KeyValue {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*commonpb.KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, &commonpb.KeyValue{Key: k, Value: AnyValue(m[k])})
	}
	return out
}

// AnyValue converts a Go value to an OTLP AnyValue. Strings, booleans, signed
// and unsigned integers, floats, byte slices, string slices, []any, and
// map[string]any map to their OTLP counterparts; time.Time renders as RFC 3339
// with nanoseconds; anything else falls back to its fmt %v text.
func AnyValue(v any) *commonpb.AnyValue {
	switch t := v.(type) {
	case nil:
		return &commonpb.AnyValue{}
	case string:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: t}}
	case bool:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: t}}
	case int:
		return intValue(int64(t))
	case int8:
		return intValue(int64(t))
	case int16:
		return intValue(int64(t))
	case int32:
		return intValue(int64(t))
	case int64:
		return intValue(t)
	case uint:
		return uintValue(uint64(t))
	case uint8:
		return intValue(int64(t))
	case uint16:
		return intValue(int64(t))
	case uint32:
		return intValue(int64(t))
	case uint64:
		return uintValue(t)
	case float32:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: float64(t)}}
	case float64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: t}}
	case []byte:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: t}}
	case time.Time:
		return AnyValue(t.UTC().Format(time.RFC3339Nano))
	case []string:
		vals := make([]*commonpb.AnyValue, 0, len(t))
		for _, s := range t {
			vals = append(vals, AnyValue(s))
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}}
	case []any:
		vals := make([]*commonpb.AnyValue, 0, len(t))
		for _, e := range t {
			vals = append(vals, AnyValue(e))
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}}
	case map[string]any:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: Attributes(t)}}}
	case fmt.Stringer:
		return AnyValue(t.String())
	default:
		return AnyValue(fmt.Sprintf("%v", v))
	}
}

func intValue(i int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: i}}
}

// uintValue keeps values that fit int64 numeric and renders the rest as text
// rather than wrapping them negative.
func uintValue(u uint64) *commonpb.AnyValue {
	if u > 1<<63-1 {
		return AnyValue(fmt.Sprintf("%d", u))
	}
	return intValue(int64(u))
}
