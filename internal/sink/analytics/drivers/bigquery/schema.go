package bigquery

import (
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

// protoScope is the package scope of the generated row descriptor.
const protoScope = "antwatcher_analytics"

// fieldType maps the portable column types to BigQuery field types. A string
// array is a repeated STRING field.
func fieldType(t analytics.Type) (bq.FieldType, bool, error) {
	switch t {
	case analytics.TypeString:
		return bq.StringFieldType, false, nil
	case analytics.TypeInt64:
		return bq.IntegerFieldType, false, nil
	case analytics.TypeTimestamp:
		return bq.TimestampFieldType, false, nil
	case analytics.TypeBool:
		return bq.BooleanFieldType, false, nil
	case analytics.TypeStringArray:
		return bq.StringFieldType, true, nil
	default:
		return "", false, fmt.Errorf("unsupported column type %q", t)
	}
}

// TableSchema converts the portable schema to a BigQuery schema: every
// column in order, required columns REQUIRED, everything else NULLABLE,
// string arrays REPEATED, descriptions kept.
func TableSchema(s analytics.Schema) (bq.Schema, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	out := make(bq.Schema, 0, len(s.Columns))
	for _, c := range s.Columns {
		ft, repeated, err := fieldType(c.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}
		out = append(out, &bq.FieldSchema{
			Name:        c.Name,
			Type:        ft,
			Required:    c.Required && !repeated,
			Repeated:    repeated,
			Description: c.Description,
		})
	}
	return out, nil
}

// TableMetadata is what EnsureSchema creates for s: TableSchema, DAY
// partitioning on the partition column (when the schema has one), and
// clustering by the cluster columns.
func TableMetadata(s analytics.Schema) (*bq.TableMetadata, error) {
	schema, err := TableSchema(s)
	if err != nil {
		return nil, err
	}
	md := &bq.TableMetadata{
		Description: "antwatcher analytics base table: one row per run, job, or step per webhook event (at-least-once); query the " +
			analytics.ViewSuffix + " view for the latest state per record_id",
		Schema: schema,
	}
	if s.PartitionBy.Column != "" {
		md.TimePartitioning = &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: s.PartitionBy.Column}
	}
	if len(s.ClusterBy) > 0 {
		md.Clustering = &bq.Clustering{Fields: append([]string(nil), s.ClusterBy...)}
	}
	return md, nil
}

// ViewMetadata is what EnsureSchema creates for the current view over the
// fully qualified base table.
func ViewMetadata(tableID string) *bq.TableMetadata {
	return &bq.TableMetadata{
		Description: "antwatcher analytics current view: the latest state per record_id (highest status_rank, then latest event_time and received_at)",
		ViewQuery:   analytics.CurrentViewSQL(tableID),
	}
}

// RowDescriptor is the proto2 message descriptor rows are encoded with,
// derived from TableSchema through the Storage Write API adapters, and its
// normalized DescriptorProto for the managed stream.
type RowDescriptor struct {
	Message protoreflect.MessageDescriptor
	Proto   *descriptorpb.DescriptorProto
}

// NewRowDescriptor derives the row descriptor of s.
func NewRowDescriptor(s analytics.Schema) (*RowDescriptor, error) {
	schema, err := TableSchema(s)
	if err != nil {
		return nil, err
	}
	storageSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("storage schema: %w", err)
	}
	desc, err := adapt.StorageSchemaToProto2Descriptor(storageSchema, protoScope)
	if err != nil {
		return nil, fmt.Errorf("proto descriptor: %w", err)
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("proto descriptor: %T is not a message descriptor", desc)
	}
	dp, err := adapt.NormalizeDescriptor(md)
	if err != nil {
		return nil, fmt.Errorf("normalize descriptor: %w", err)
	}
	return &RowDescriptor{Message: md, Proto: dp}, nil
}

// VerifySchema checks that an existing table can hold rows of s: every
// column of s exists in have with the same type and repetition. Extra
// columns in the table are allowed (they stay NULL). It returns every
// mismatch in one error.
func VerifySchema(s analytics.Schema, have bq.Schema) error {
	want, err := TableSchema(s)
	if err != nil {
		return err
	}
	byName := make(map[string]*bq.FieldSchema, len(have))
	for _, f := range have {
		byName[f.Name] = f
	}
	var problems []string
	for _, f := range want {
		got, ok := byName[f.Name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("column %q is missing", f.Name))
		case got.Type != f.Type:
			problems = append(problems, fmt.Sprintf("column %q is %s, want %s", f.Name, got.Type, f.Type))
		case got.Repeated != f.Repeated:
			problems = append(problems, fmt.Sprintf("column %q repeated=%t, want %t", f.Name, got.Repeated, f.Repeated))
		case got.Required && !f.Required:
			problems = append(problems, fmt.Sprintf("column %q is REQUIRED but rows may leave it NULL", f.Name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("table schema mismatch: %s", strings.Join(problems, "; "))
	}
	return nil
}
