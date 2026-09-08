package analytics

import "fmt"

// Type is a portable column type. Drivers map it to their warehouse's type
// system (BigQuery: STRING, INT64, TIMESTAMP, BOOL, ARRAY<STRING>).
type Type string

// Column types.
const (
	TypeString      Type = "string"
	TypeInt64       Type = "int64"
	TypeTimestamp   Type = "timestamp"
	TypeBool        Type = "bool"
	TypeStringArray Type = "string_array"
)

// Column is one column of the base table.
type Column struct {
	Name string
	Type Type
	// Required columns are NOT NULL; every other column is nullable because
	// the model is sparse (see Record).
	Required bool
	// Description is the column comment for warehouses that keep one.
	Description string
}

// Partition says how the base table is partitioned.
type Partition struct {
	// Column is a timestamp column.
	Column string
	// Granularity is "day".
	Granularity string
}

// Schema is the portable description of the base table: ordered columns,
// partitioning, and clustering. Drivers create or verify the table from it.
type Schema struct {
	Columns     []Column
	PartitionBy Partition
	ClusterBy   []string
}

// ViewSuffix is appended to the base table name to name the current view.
const ViewSuffix = "_current"

// Current is the schema of Record: columns in table order, partitioned by
// event_time (day) and clustered by repository and kind.
var Current = Schema{
	Columns: []Column{
		{Name: ColRecordID, Type: TypeString, Required: true, Description: "entity identity, stable across events and redeliveries"},
		{Name: ColKind, Type: TypeString, Required: true, Description: "run, job, or step"},
		{Name: ColRepositoryID, Type: TypeInt64, Required: true, Description: "GitHub repository id"},
		{Name: ColRepository, Type: TypeString, Description: "owner/name"},
		{Name: ColWorkflowID, Type: TypeInt64, Description: "workflow definition id (run rows)"},
		{Name: ColWorkflowName, Type: TypeString, Description: "workflow display name"},
		{Name: ColWorkflowPath, Type: TypeString, Description: "workflow file path (run rows)"},
		{Name: ColRunID, Type: TypeInt64, Required: true, Description: "workflow run id"},
		{Name: ColRunNumber, Type: TypeInt64, Description: "human-visible run counter (run rows)"},
		{Name: ColRunAttempt, Type: TypeInt64, Required: true, Description: "run attempt, 1 for the first"},
		{Name: ColJobID, Type: TypeInt64, Description: "job id (job and step rows)"},
		{Name: ColJobName, Type: TypeString, Description: "job display name (job and step rows)"},
		{Name: ColStepNumber, Type: TypeInt64, Description: "1-based step index (step rows)"},
		{Name: ColStepName, Type: TypeString, Description: "step display name (step rows)"},
		{Name: ColStatus, Type: TypeString, Description: "queued, in_progress, completed, ..."},
		{Name: ColConclusion, Type: TypeString, Description: "success, failure, cancelled, skipped, ... once completed"},
		{Name: ColStatusRank, Type: TypeInt64, Required: true, Description: "lifecycle order of status: completed 3, in_progress 2, scheduled 1, unknown 0"},
		{Name: ColCreatedAt, Type: TypeTimestamp, Description: "entity created (run and job rows)"},
		{Name: ColStartedAt, Type: TypeTimestamp, Description: "entity started"},
		{Name: ColCompletedAt, Type: TypeTimestamp, Description: "entity completed"},
		{Name: ColQueuedMs, Type: TypeInt64, Description: "started_at - created_at once started"},
		{Name: ColDurationMs, Type: TypeInt64, Description: "completed_at - started_at once completed"},
		{Name: ColTriggerEvent, Type: TypeString, Description: "event that triggered the run (run rows)"},
		{Name: ColHeadBranch, Type: TypeString, Description: "branch of the head commit"},
		{Name: ColHeadSHA, Type: TypeString, Description: "head commit sha"},
		{Name: ColActor, Type: TypeString, Description: "login that created the run (run rows)"},
		{Name: ColRunnerName, Type: TypeString, Description: "runner that picked the job up (job and step rows)"},
		{Name: ColRunnerGroup, Type: TypeString, Description: "runner group (job and step rows)"},
		{Name: ColLabels, Type: TypeStringArray, Description: "runner labels requested by the job (job and step rows)"},
		{Name: ColHTMLURL, Type: TypeString, Description: "GitHub page of the entity"},
		{Name: ColEventTime, Type: TypeTimestamp, Required: true, Description: "when the reported transition happened; partition column"},
		{Name: ColReceivedAt, Type: TypeTimestamp, Required: true, Description: "when antwatcher received the delivery"},
		{Name: ColDeliveryGUID, Type: TypeString, Required: true, Description: "X-GitHub-Delivery of the source webhook"},
		{Name: ColSchemaVersion, Type: TypeInt64, Required: true, Description: "antwatcher envelope schema version"},
	},
	PartitionBy: Partition{Column: ColEventTime, Granularity: "day"},
	ClusterBy:   []string{ColRepository, ColKind},
}

// ColumnNames lists the column names in table order.
func (s Schema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}

// Column finds a column by name.
func (s Schema) Column(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Validate checks the schema is self-consistent: no empty or duplicate
// column names, known types, a partition column that exists and is a
// timestamp, and cluster columns that exist.
func (s Schema) Validate() error {
	seen := make(map[string]struct{}, len(s.Columns))
	for _, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("analytics schema: column with empty name")
		}
		if _, dup := seen[c.Name]; dup {
			return fmt.Errorf("analytics schema: duplicate column %q", c.Name)
		}
		seen[c.Name] = struct{}{}
		switch c.Type {
		case TypeString, TypeInt64, TypeTimestamp, TypeBool, TypeStringArray:
		default:
			return fmt.Errorf("analytics schema: column %q has unknown type %q", c.Name, c.Type)
		}
	}
	if s.PartitionBy.Column != "" {
		c, ok := s.Column(s.PartitionBy.Column)
		if !ok {
			return fmt.Errorf("analytics schema: partition column %q does not exist", s.PartitionBy.Column)
		}
		if c.Type != TypeTimestamp {
			return fmt.Errorf("analytics schema: partition column %q is %s, want %s", c.Name, c.Type, TypeTimestamp)
		}
		if s.PartitionBy.Granularity != "day" {
			return fmt.Errorf("analytics schema: partition granularity %q is not supported", s.PartitionBy.Granularity)
		}
	}
	for _, name := range s.ClusterBy {
		if _, ok := s.Column(name); !ok {
			return fmt.Errorf("analytics schema: cluster column %q does not exist", name)
		}
	}
	return nil
}

// ViewName is the name of the current view of table: table + ViewSuffix.
func ViewName(table string) string {
	return table + ViewSuffix
}

// CurrentViewSQL returns the query of the deduplicating view over table: for
// every record_id, the row with the highest status_rank, then the latest
// event_time, then the latest received_at. A later, more advanced state
// therefore wins, and a replay of an older event never overwrites it. table
// is used as given and wrapped in backquotes, so pass it fully qualified
// (for BigQuery "project.dataset.table").
func CurrentViewSQL(table string) string {
	return "SELECT * FROM `" + table + "` " +
		"QUALIFY ROW_NUMBER() OVER (PARTITION BY " + ColRecordID +
		" ORDER BY " + ColStatusRank + " DESC, " + ColEventTime + " DESC, " + ColReceivedAt + " DESC) = 1"
}
