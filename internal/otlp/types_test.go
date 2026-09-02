package otlp

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var (
	testTraceID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	testSpanID  = [8]byte{0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x11}
	testParent  = [8]byte{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27}
	testStart   = time.Date(2026, 9, 2, 10, 0, 0, 123456789, time.UTC)
	testEnd     = testStart.Add(90 * time.Second)
)

func str(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func kv(k string, v *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: v}
}

func TestResourceSpans(t *testing.T) {
	res := DefaultResource("1.2.3")
	spans := []Span{
		{
			TraceID: testTraceID, SpanID: testParent, Name: "workflow:ci", Kind: SpanKindServer,
			Start: testStart, End: testEnd,
			Attributes: map[string]any{"github.run_id": int64(42), "github.repository": "o/r", "github.conclusion": "success"},
			Status:     Status{Code: StatusOk},
		},
		{
			TraceID: testTraceID, SpanID: testSpanID, ParentSpanID: testParent, Name: "job:build", Kind: SpanKindInternal,
			Start: testStart.Add(time.Second), End: testEnd.Add(-time.Second),
			Status: Status{Code: StatusError, Message: "failure"},
		},
	}
	got := ResourceSpans(res, spans)

	want := &tracepb.ResourceSpans{
		Resource: res.proto(),
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: &commonpb.InstrumentationScope{Name: "antwatcher", Version: "1.2.3"},
			Spans: []*tracepb.Span{
				{
					TraceId: testTraceID[:], SpanId: testParent[:], ParentSpanId: nil,
					Name: "workflow:ci", Kind: tracepb.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: 1788343200123456789, EndTimeUnixNano: 1788343290123456789,
					Attributes: []*commonpb.KeyValue{
						kv("github.conclusion", str("success")),
						kv("github.repository", str("o/r")),
						kv("github.run_id", intValue(42)),
					},
					Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
				},
				{
					TraceId: testTraceID[:], SpanId: testSpanID[:], ParentSpanId: testParent[:],
					Name: "job:build", Kind: tracepb.Span_SPAN_KIND_INTERNAL,
					StartTimeUnixNano: 1788343201123456789, EndTimeUnixNano: 1788343289123456789,
					Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "failure"},
				},
			},
		}},
	}
	assert.True(t, proto.Equal(want, got), "got %v", got)

	assert.Equal(t, uint64(testStart.UnixNano()), got.ScopeSpans[0].Spans[0].StartTimeUnixNano)
	assert.Nil(t, got.ScopeSpans[0].Spans[0].ParentSpanId, "root span carries no parent")
	wantRes := []*commonpb.KeyValue{
		kv("service.name", str("antwatcher")),
		kv("service.version", str("1.2.3")),
	}
	require.Len(t, got.Resource.Attributes, len(wantRes))
	for i := range wantRes {
		assert.True(t, proto.Equal(wantRes[i], got.Resource.Attributes[i]), "resource attribute %d: %v", i, got.Resource.Attributes[i])
	}
}

func TestResourceLogs(t *testing.T) {
	res := DefaultResource("v9")
	recs := []LogRecord{
		{
			Time: testStart, ObservedTime: testEnd, Severity: SeverityError, SeverityText: "ERROR",
			Body:       "workflow_job completed: build",
			Attributes: map[string]any{"github.event": "workflow_job", "github.job_id": 7},
			TraceID:    testTraceID, SpanID: testSpanID,
		},
		{Time: testStart, Severity: SeverityInfo, SeverityText: "INFO", Body: "ping"},
	}
	got := ResourceLogs(res, recs)

	want := &logspb.ResourceLogs{
		Resource: res.proto(),
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: "antwatcher", Version: "v9"},
			LogRecords: []*logspb.LogRecord{
				{
					TimeUnixNano: 1788343200123456789, ObservedTimeUnixNano: 1788343290123456789,
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, SeverityText: "ERROR",
					Body: str("workflow_job completed: build"),
					Attributes: []*commonpb.KeyValue{
						kv("github.event", str("workflow_job")),
						kv("github.job_id", intValue(7)),
					},
					TraceId: testTraceID[:], SpanId: testSpanID[:],
				},
				{
					TimeUnixNano:   1788343200123456789,
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO, SeverityText: "INFO",
					Body: str("ping"),
				},
			},
		}},
	}
	assert.True(t, proto.Equal(want, got), "got %v", got)
	second := got.ScopeLogs[0].LogRecords[1]
	assert.Nil(t, second.TraceId, "zero trace id omitted")
	assert.Nil(t, second.SpanId, "zero span id omitted")
	assert.Zero(t, second.ObservedTimeUnixNano, "zero time is unset")
}

func TestSeverityValues(t *testing.T) {
	assert.EqualValues(t, logspb.SeverityNumber_SEVERITY_NUMBER_TRACE, SeverityTrace)
	assert.EqualValues(t, logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG, SeverityDebug)
	assert.EqualValues(t, logspb.SeverityNumber_SEVERITY_NUMBER_INFO, SeverityInfo)
	assert.EqualValues(t, logspb.SeverityNumber_SEVERITY_NUMBER_WARN, SeverityWarn)
	assert.EqualValues(t, logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, SeverityError)
	assert.EqualValues(t, logspb.SeverityNumber_SEVERITY_NUMBER_FATAL, SeverityFatal)
	assert.EqualValues(t, tracepb.Span_SPAN_KIND_CONSUMER, SpanKindConsumer)
	assert.EqualValues(t, tracepb.Status_STATUS_CODE_ERROR, StatusError)
}

type stringer struct{}

func (stringer) String() string { return "stringed" }

func TestAnyValue(t *testing.T) {
	arr := func(vals ...*commonpb.AnyValue) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}}
	}
	dbl := func(f float64) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: f}}
	}
	cases := []struct {
		in   any
		want *commonpb.AnyValue
	}{
		{nil, &commonpb.AnyValue{}},
		{"s", str("s")},
		{true, &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		{int(1), intValue(1)},
		{int8(2), intValue(2)},
		{int16(3), intValue(3)},
		{int32(4), intValue(4)},
		{int64(5), intValue(5)},
		{uint(6), intValue(6)},
		{uint8(7), intValue(7)},
		{uint16(8), intValue(8)},
		{uint32(9), intValue(9)},
		{uint64(10), intValue(10)},
		{uint64(1 << 63), str("9223372036854775808")},
		{float32(1.5), dbl(1.5)},
		{2.5, dbl(2.5)},
		{[]byte{1, 2}, &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{1, 2}}}},
		{testStart, str("2026-09-02T10:00:00.123456789Z")},
		{[]string{"a", "b"}, arr(str("a"), str("b"))},
		{[]any{"a", 1}, arr(str("a"), intValue(1))},
		{map[string]any{"b": 1, "a": "x"}, &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
			Values: []*commonpb.KeyValue{kv("a", str("x")), kv("b", intValue(1))},
		}}}},
		{stringer{}, str("stringed")},
		{struct{ A int }{3}, str("{3}")},
	}
	for _, tc := range cases {
		got := AnyValue(tc.in)
		assert.True(t, proto.Equal(tc.want, got), "%T %v: got %v", tc.in, tc.in, got)
	}
}

func TestAttributes_DeterministicOrder(t *testing.T) {
	assert.Nil(t, Attributes(nil))
	assert.Nil(t, Attributes(map[string]any{}))

	m := map[string]any{}
	for i := 0; i < 50; i++ {
		m[fmt.Sprintf("k%02d", 49-i)] = i
	}
	first, err := proto.Marshal(&commonpb.KeyValueList{Values: Attributes(m)})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		again, err := proto.Marshal(&commonpb.KeyValueList{Values: Attributes(m)})
		require.NoError(t, err)
		assert.Equal(t, first, again)
	}
	keys := Attributes(m)
	assert.Equal(t, "k00", keys[0].Key)
	assert.Equal(t, "k49", keys[49].Key)
}

func TestDefaultResource(t *testing.T) {
	r := DefaultResource("abc")
	assert.Equal(t, "antwatcher", r.Attributes[AttrServiceName])
	assert.Equal(t, "abc", r.Attributes[AttrServiceVersion])
	assert.Equal(t, "abc", r.scopeVersion())
	assert.Empty(t, Resource{}.scopeVersion())
}
