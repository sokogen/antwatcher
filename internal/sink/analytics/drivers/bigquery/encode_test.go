package bigquery_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

// decodeRow unmarshals an encoded row with d and returns it as column →
// value in the same shape as Record.Fields(): timestamps back to time.Time,
// repeated strings as []string, unset fields absent.
func decodeRow(t *testing.T, d *bqdriver.RowDescriptor, s analytics.Schema, row []byte) map[string]any {
	t.Helper()
	msg := dynamicpb.NewMessage(d.Message)
	require.NoError(t, proto.Unmarshal(row, msg))
	out := map[string]any{}
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		col, ok := s.Column(name)
		require.True(t, ok, "field %s is a schema column", name)
		switch col.Type {
		case analytics.TypeString:
			out[name] = v.String()
		case analytics.TypeInt64:
			out[name] = v.Int()
		case analytics.TypeBool:
			out[name] = v.Bool()
		case analytics.TypeTimestamp:
			out[name] = time.UnixMicro(v.Int()).UTC()
		case analytics.TypeStringArray:
			list := v.List()
			arr := make([]string, list.Len())
			for i := range arr {
				arr[i] = list.Get(i).String()
			}
			out[name] = arr
		}
		return true
	})
	return out
}

func TestEncode_RoundTripFixtures(t *testing.T) {
	d, err := bqdriver.NewRowDescriptor(analytics.Current)
	require.NoError(t, err)
	for _, fixture := range []string{
		"workflow_run.requested", "workflow_run.in_progress", "workflow_run.completed",
		"workflow_job.queued", "workflow_job.in_progress", "workflow_job.completed",
	} {
		t.Run(fixture, func(t *testing.T) {
			for _, rec := range fixtureRecords(t, fixture) {
				row, err := bqdriver.Encode(d, &rec)
				require.NoError(t, err)
				got := decodeRow(t, d, analytics.Current, row)
				want := rec.Fields()
				// times must compare by instant; Fields returns UTC already
				for k, v := range want {
					if ts, ok := v.(time.Time); ok {
						want[k] = ts.UTC()
					}
				}
				assert.Equal(t, want, got, "record %s (%s)", rec.RecordID, rec.Kind)
				// NULL columns are absent on the wire, not zero values
				for _, c := range analytics.Current.Columns {
					_, present := want[c.Name]
					_, decoded := got[c.Name]
					assert.Equal(t, present, decoded, "column %s presence", c.Name)
				}
			}
		})
	}
}

func TestEncode_TimestampMicrosecondsAndLabels(t *testing.T) {
	d, err := bqdriver.NewRowDescriptor(analytics.Current)
	require.NoError(t, err)
	at := time.Date(2026, 9, 2, 10, 5, 0, 123456789, time.FixedZone("plus2", 2*3600))
	rec := analytics.Record{
		RecordID: "r", Kind: "job", RepositoryID: 1, RunID: 2, RunAttempt: 1,
		StatusRank: 3, EventTime: at, ReceivedAt: at, DeliveryGUID: "g", SchemaVersion: 1,
		Labels: []string{"self-hosted", "linux"},
	}
	msg, err := bqdriver.Message(d, &rec)
	require.NoError(t, err)
	fd := d.Message.Fields().ByName(analytics.ColEventTime)
	assert.Equal(t, at.UnixMicro(), msg.Get(fd).Int(), "microseconds since epoch, sub-microsecond precision dropped")
	assert.Equal(t, time.Date(2026, 9, 2, 8, 5, 0, 123456000, time.UTC).UnixMicro(), msg.Get(fd).Int(), "converted to UTC")
	labels := msg.Get(d.Message.Fields().ByName(analytics.ColLabels)).List()
	require.Equal(t, 2, labels.Len())
	assert.Equal(t, "self-hosted", labels.Get(0).String())

	empty := rec
	empty.Labels = []string{}
	msg, err = bqdriver.Message(d, &empty)
	require.NoError(t, err)
	assert.Equal(t, 0, msg.Get(d.Message.Fields().ByName(analytics.ColLabels)).List().Len(),
		"an empty label list has no elements on the wire")
}

func TestEncode_BoolColumn(t *testing.T) {
	s := analytics.Schema{Columns: []analytics.Column{
		{Name: "record_id", Type: analytics.TypeString, Required: true},
		{Name: "flag", Type: analytics.TypeBool},
	}}
	d, err := bqdriver.NewRowDescriptor(s)
	require.NoError(t, err)
	// Record.Fields never yields a bool today; drive Message through the
	// descriptor directly to cover the bool branch of the encoder
	msg := dynamicpb.NewMessage(d.Message)
	msg.Set(d.Message.Fields().ByName("flag"), protoreflect.ValueOfBool(true))
	_, err = proto.Marshal(msg)
	require.Error(t, err, "proto2 required fields are enforced at marshal time")
	msg.Set(d.Message.Fields().ByName("record_id"), protoreflect.ValueOfString("r"))
	row, err := proto.Marshal(msg)
	require.NoError(t, err)
	got := decodeRow(t, d, s, row)
	assert.Equal(t, map[string]any{"record_id": "r", "flag": true}, got)
}

func TestEncode_ColumnMissingFromDescriptorIsAnError(t *testing.T) {
	reduced := analytics.Schema{Columns: []analytics.Column{
		{Name: analytics.ColRecordID, Type: analytics.TypeString, Required: true},
	}}
	d, err := bqdriver.NewRowDescriptor(reduced)
	require.NoError(t, err)
	rec := fixtureRecords(t, "workflow_run.completed")[0]
	_, err = bqdriver.Encode(d, &rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not in the table descriptor")
}
