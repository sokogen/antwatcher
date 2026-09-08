package bigquery_test

import (
	"fmt"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

// renderSchema is the golden form of a BigQuery schema: one "name TYPE MODE"
// line per field.
func renderSchema(s bq.Schema) string {
	var b strings.Builder
	for _, f := range s {
		mode := "NULLABLE"
		switch {
		case f.Repeated:
			mode = "REPEATED"
		case f.Required:
			mode = "REQUIRED"
		}
		fmt.Fprintf(&b, "%s %s %s\n", f.Name, f.Type, mode)
	}
	return b.String()
}

const goldenTableSchema = `record_id STRING REQUIRED
kind STRING REQUIRED
repository_id INTEGER REQUIRED
repository STRING NULLABLE
workflow_id INTEGER NULLABLE
workflow_name STRING NULLABLE
workflow_path STRING NULLABLE
run_id INTEGER REQUIRED
run_number INTEGER NULLABLE
run_attempt INTEGER REQUIRED
job_id INTEGER NULLABLE
job_name STRING NULLABLE
step_number INTEGER NULLABLE
step_name STRING NULLABLE
status STRING NULLABLE
conclusion STRING NULLABLE
status_rank INTEGER REQUIRED
created_at TIMESTAMP NULLABLE
started_at TIMESTAMP NULLABLE
completed_at TIMESTAMP NULLABLE
queued_ms INTEGER NULLABLE
duration_ms INTEGER NULLABLE
trigger_event STRING NULLABLE
head_branch STRING NULLABLE
head_sha STRING NULLABLE
actor STRING NULLABLE
runner_name STRING NULLABLE
runner_group STRING NULLABLE
labels STRING REPEATED
html_url STRING NULLABLE
event_time TIMESTAMP REQUIRED
received_at TIMESTAMP REQUIRED
delivery_guid STRING REQUIRED
schema_version INTEGER REQUIRED
`

func TestTableSchema_Golden(t *testing.T) {
	schema, err := bqdriver.TableSchema(analytics.Current)
	require.NoError(t, err)
	assert.Equal(t, goldenTableSchema, renderSchema(schema))
	for _, f := range schema {
		assert.NotEmpty(t, f.Description, "column %s keeps its description", f.Name)
	}
}

func TestTableSchema_AllTypesAndErrors(t *testing.T) {
	s := analytics.Schema{Columns: []analytics.Column{
		{Name: "s", Type: analytics.TypeString},
		{Name: "i", Type: analytics.TypeInt64, Required: true},
		{Name: "t", Type: analytics.TypeTimestamp},
		{Name: "b", Type: analytics.TypeBool},
		{Name: "a", Type: analytics.TypeStringArray, Required: true},
	}}
	schema, err := bqdriver.TableSchema(s)
	require.NoError(t, err)
	assert.Equal(t, "s STRING NULLABLE\ni INTEGER REQUIRED\nt TIMESTAMP NULLABLE\nb BOOLEAN NULLABLE\na STRING REPEATED\n", renderSchema(schema),
		"a required array is REPEATED, never REQUIRED")

	_, err = bqdriver.TableSchema(analytics.Schema{Columns: []analytics.Column{{Name: "x", Type: "float"}}})
	require.Error(t, err, "invalid schemas are rejected")
	_, err = bqdriver.TableMetadata(analytics.Schema{Columns: []analytics.Column{{Name: "", Type: analytics.TypeString}}})
	require.Error(t, err)
	_, err = bqdriver.NewRowDescriptor(analytics.Schema{Columns: []analytics.Column{{Name: "x", Type: "float"}}})
	require.Error(t, err)
}

func TestTableMetadata_PartitioningAndClustering(t *testing.T) {
	md, err := bqdriver.TableMetadata(analytics.Current)
	require.NoError(t, err)
	require.NotNil(t, md.TimePartitioning)
	assert.Equal(t, bq.DayPartitioningType, md.TimePartitioning.Type)
	assert.Equal(t, analytics.ColEventTime, md.TimePartitioning.Field)
	require.NotNil(t, md.Clustering)
	assert.Equal(t, []string{analytics.ColRepository, analytics.ColKind}, md.Clustering.Fields)
	assert.Empty(t, md.ViewQuery)
	assert.Contains(t, md.Description, "_current")
	assert.Len(t, md.Schema, len(analytics.Current.Columns))

	plain, err := bqdriver.TableMetadata(analytics.Schema{Columns: []analytics.Column{{Name: "x", Type: analytics.TypeString}}})
	require.NoError(t, err)
	assert.Nil(t, plain.TimePartitioning, "no partition column, no partitioning")
	assert.Nil(t, plain.Clustering)
}

func TestViewMetadata(t *testing.T) {
	md := bqdriver.ViewMetadata("my-proj.github.actions")
	assert.Equal(t, analytics.CurrentViewSQL("my-proj.github.actions"), md.ViewQuery)
	assert.Contains(t, md.ViewQuery, "FROM `my-proj.github.actions`")
	assert.Nil(t, md.Schema)
}

// renderDescriptor is the golden form of the row descriptor: one
// "name LABEL TYPE" line per field, in order.
func renderDescriptor(dp *descriptorpb.DescriptorProto) string {
	var b strings.Builder
	for _, f := range dp.GetField() {
		label := strings.TrimPrefix(f.GetLabel().String(), "LABEL_")
		typ := strings.TrimPrefix(f.GetType().String(), "TYPE_")
		fmt.Fprintf(&b, "%s %s %s\n", f.GetName(), label, typ)
	}
	return b.String()
}

const goldenDescriptor = `record_id REQUIRED STRING
kind REQUIRED STRING
repository_id REQUIRED INT64
repository OPTIONAL STRING
workflow_id OPTIONAL INT64
workflow_name OPTIONAL STRING
workflow_path OPTIONAL STRING
run_id REQUIRED INT64
run_number OPTIONAL INT64
run_attempt REQUIRED INT64
job_id OPTIONAL INT64
job_name OPTIONAL STRING
step_number OPTIONAL INT64
step_name OPTIONAL STRING
status OPTIONAL STRING
conclusion OPTIONAL STRING
status_rank REQUIRED INT64
created_at OPTIONAL INT64
started_at OPTIONAL INT64
completed_at OPTIONAL INT64
queued_ms OPTIONAL INT64
duration_ms OPTIONAL INT64
trigger_event OPTIONAL STRING
head_branch OPTIONAL STRING
head_sha OPTIONAL STRING
actor OPTIONAL STRING
runner_name OPTIONAL STRING
runner_group OPTIONAL STRING
labels REPEATED STRING
html_url OPTIONAL STRING
event_time REQUIRED INT64
received_at REQUIRED INT64
delivery_guid REQUIRED STRING
schema_version REQUIRED INT64
`

func TestNewRowDescriptor_Golden(t *testing.T) {
	d, err := bqdriver.NewRowDescriptor(analytics.Current)
	require.NoError(t, err)
	assert.Equal(t, goldenDescriptor, renderDescriptor(d.Proto), "timestamps travel as int64 microseconds")
	assert.Equal(t, protoreflect.Proto2, d.Message.Syntax())
	assert.Equal(t, len(analytics.Current.Columns), d.Message.Fields().Len())
	for _, c := range analytics.Current.Columns {
		assert.NotNil(t, d.Message.Fields().ByName(protoreflect.Name(c.Name)), "field %s", c.Name)
	}
	// the normalized proto is self-contained: no nested types to resolve
	assert.Empty(t, d.Proto.GetNestedType())
}

func TestVerifySchema(t *testing.T) {
	want, err := bqdriver.TableSchema(analytics.Current)
	require.NoError(t, err)

	t.Run("identical", func(t *testing.T) {
		require.NoError(t, bqdriver.VerifySchema(analytics.Current, want))
	})
	t.Run("extra columns and relaxed modes are fine", func(t *testing.T) {
		have := append(bq.Schema{&bq.FieldSchema{Name: "extra", Type: bq.StringFieldType}}, want...)
		relaxed := make(bq.Schema, len(have))
		for i, f := range have {
			c := *f
			c.Required = false
			relaxed[i] = &c
		}
		require.NoError(t, bqdriver.VerifySchema(analytics.Current, relaxed))
	})
	t.Run("missing, wrong type, wrong repetition, stricter mode", func(t *testing.T) {
		have := make(bq.Schema, 0, len(want))
		for _, f := range want {
			c := *f
			switch c.Name {
			case analytics.ColRecordID:
				continue
			case analytics.ColRunID:
				c.Type = bq.StringFieldType
			case analytics.ColLabels:
				c.Repeated = false
			case analytics.ColActor:
				c.Required = true
			}
			have = append(have, &c)
		}
		err := bqdriver.VerifySchema(analytics.Current, have)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `column "record_id" is missing`)
		assert.Contains(t, err.Error(), `column "run_id" is STRING, want INTEGER`)
		assert.Contains(t, err.Error(), `column "labels" repeated=false, want true`)
		assert.Contains(t, err.Error(), `column "actor" is REQUIRED`)
	})
	t.Run("invalid schema", func(t *testing.T) {
		require.Error(t, bqdriver.VerifySchema(analytics.Schema{Columns: []analytics.Column{{Name: "x", Type: "float"}}}, want))
	})
}
