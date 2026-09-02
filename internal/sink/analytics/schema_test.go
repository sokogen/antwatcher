package analytics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

func TestCurrent_Golden(t *testing.T) {
	require.NoError(t, analytics.Current.Validate())

	wantColumns := []string{
		"record_id", "kind",
		"repository_id", "repository", "workflow_id", "workflow_name", "workflow_path",
		"run_id", "run_number", "run_attempt", "job_id", "job_name", "step_number", "step_name",
		"status", "conclusion", "status_rank",
		"created_at", "started_at", "completed_at", "queued_ms", "duration_ms",
		"trigger_event", "head_branch", "head_sha", "actor",
		"runner_name", "runner_group", "labels",
		"html_url", "event_time", "received_at", "delivery_guid", "schema_version",
	}
	assert.Equal(t, wantColumns, analytics.Current.ColumnNames())

	wantTypes := map[string]analytics.Type{
		"record_id": analytics.TypeString, "kind": analytics.TypeString,
		"repository_id": analytics.TypeInt64, "repository": analytics.TypeString,
		"workflow_id": analytics.TypeInt64, "workflow_name": analytics.TypeString, "workflow_path": analytics.TypeString,
		"run_id": analytics.TypeInt64, "run_number": analytics.TypeInt64, "run_attempt": analytics.TypeInt64,
		"job_id": analytics.TypeInt64, "job_name": analytics.TypeString,
		"step_number": analytics.TypeInt64, "step_name": analytics.TypeString,
		"status": analytics.TypeString, "conclusion": analytics.TypeString, "status_rank": analytics.TypeInt64,
		"created_at": analytics.TypeTimestamp, "started_at": analytics.TypeTimestamp, "completed_at": analytics.TypeTimestamp,
		"queued_ms": analytics.TypeInt64, "duration_ms": analytics.TypeInt64,
		"trigger_event": analytics.TypeString, "head_branch": analytics.TypeString, "head_sha": analytics.TypeString,
		"actor": analytics.TypeString, "runner_name": analytics.TypeString, "runner_group": analytics.TypeString,
		"labels": analytics.TypeStringArray, "html_url": analytics.TypeString,
		"event_time": analytics.TypeTimestamp, "received_at": analytics.TypeTimestamp,
		"delivery_guid": analytics.TypeString, "schema_version": analytics.TypeInt64,
	}
	wantRequired := map[string]bool{
		"record_id": true, "kind": true, "repository_id": true, "run_id": true, "run_attempt": true,
		"status_rank": true, "event_time": true, "received_at": true, "delivery_guid": true, "schema_version": true,
	}
	for _, c := range analytics.Current.Columns {
		assert.Equal(t, wantTypes[c.Name], c.Type, c.Name)
		assert.Equal(t, wantRequired[c.Name], c.Required, "%s required", c.Name)
		assert.NotEmpty(t, c.Description, "%s has a description", c.Name)
	}

	assert.Equal(t, analytics.Partition{Column: "event_time", Granularity: "day"}, analytics.Current.PartitionBy)
	assert.Equal(t, []string{"repository", "kind"}, analytics.Current.ClusterBy)
}

func TestSchema_Column(t *testing.T) {
	c, ok := analytics.Current.Column("labels")
	require.True(t, ok)
	assert.Equal(t, analytics.TypeStringArray, c.Type)
	_, ok = analytics.Current.Column("nope")
	assert.False(t, ok)
}

func TestSchema_Validate(t *testing.T) {
	col := func(name string, typ analytics.Type) analytics.Column { return analytics.Column{Name: name, Type: typ} }
	tests := []struct {
		name    string
		schema  analytics.Schema
		wantErr string
	}{
		{"empty schema", analytics.Schema{}, ""},
		{"minimal", analytics.Schema{Columns: []analytics.Column{col("a", analytics.TypeBool)}}, ""},
		{"empty name", analytics.Schema{Columns: []analytics.Column{col("", analytics.TypeString)}}, "empty name"},
		{"duplicate", analytics.Schema{Columns: []analytics.Column{col("a", analytics.TypeString), col("a", analytics.TypeInt64)}}, `duplicate column "a"`},
		{"unknown type", analytics.Schema{Columns: []analytics.Column{col("a", "float")}}, `unknown type "float"`},
		{"partition missing", analytics.Schema{PartitionBy: analytics.Partition{Column: "t", Granularity: "day"}}, `partition column "t" does not exist`},
		{"partition not timestamp", analytics.Schema{Columns: []analytics.Column{col("t", analytics.TypeString)}, PartitionBy: analytics.Partition{Column: "t", Granularity: "day"}}, `partition column "t" is string, want timestamp`},
		{"partition granularity", analytics.Schema{Columns: []analytics.Column{col("t", analytics.TypeTimestamp)}, PartitionBy: analytics.Partition{Column: "t", Granularity: "hour"}}, `granularity "hour" is not supported`},
		{"cluster missing", analytics.Schema{ClusterBy: []string{"c"}}, `cluster column "c" does not exist`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.schema.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestViewName(t *testing.T) {
	assert.Equal(t, "actions_current", analytics.ViewName("actions"))
	assert.Equal(t, "p.d.actions_current", analytics.ViewName("p.d.actions"))
}

func TestCurrentViewSQL_Golden(t *testing.T) {
	const want = "SELECT * FROM `my-proj.github.actions` " +
		"QUALIFY ROW_NUMBER() OVER (PARTITION BY record_id " +
		"ORDER BY status_rank DESC, event_time DESC, received_at DESC) = 1"
	assert.Equal(t, want, analytics.CurrentViewSQL("my-proj.github.actions"))
}

// TestCurrentViewSQL_OrderMatchesRecordSemantics pins the dedup order to the
// documented contract: the most advanced state wins, then the newest event,
// then the latest received; the view keys on the entity identity column.
func TestCurrentViewSQL_OrderMatchesRecordSemantics(t *testing.T) {
	sql := analytics.CurrentViewSQL("t")
	assert.Contains(t, sql, "PARTITION BY "+analytics.ColRecordID+" ")
	assert.Contains(t, sql, analytics.ColStatusRank+" DESC, "+analytics.ColEventTime+" DESC, "+analytics.ColReceivedAt+" DESC")
	for _, name := range []string{analytics.ColRecordID, analytics.ColStatusRank, analytics.ColEventTime, analytics.ColReceivedAt} {
		c, ok := analytics.Current.Column(name)
		require.True(t, ok, name)
		assert.True(t, c.Required, "%s must be NOT NULL for the view order to be total", name)
	}
}
