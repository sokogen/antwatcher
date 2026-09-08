package bigquery

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

// Encode serialises rec as a message of d. Every non-NULL field of
// rec.Fields() is set on the message: strings and int64s as they are,
// timestamps as microseconds since the Unix epoch (the Storage Write API
// wire form of TIMESTAMP), string arrays as repeated strings. NULL columns
// are left unset, so the service stores NULL. A field of rec that the
// descriptor does not know is an error: the descriptor was derived from the
// same schema the projection conforms to, so that is a programming error.
func Encode(d *RowDescriptor, rec *analytics.Record) ([]byte, error) {
	msg, err := Message(d, rec)
	if err != nil {
		return nil, err
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal row %s: %w", rec.RecordID, err)
	}
	return data, nil
}

// Message builds the dynamic message Encode serialises.
func Message(d *RowDescriptor, rec *analytics.Record) (*dynamicpb.Message, error) {
	msg := dynamicpb.NewMessage(d.Message)
	fields := d.Message.Fields()
	for name, value := range rec.Fields() {
		fd := fields.ByName(protoreflect.Name(name))
		if fd == nil {
			return nil, fmt.Errorf("row %s: column %q is not in the table descriptor", rec.RecordID, name)
		}
		switch v := value.(type) {
		case string:
			msg.Set(fd, protoreflect.ValueOfString(v))
		case int64:
			msg.Set(fd, protoreflect.ValueOfInt64(v))
		case time.Time:
			msg.Set(fd, protoreflect.ValueOfInt64(v.UnixMicro()))
		case bool:
			msg.Set(fd, protoreflect.ValueOfBool(v))
		case []string:
			list := msg.Mutable(fd).List()
			for _, s := range v {
				list.Append(protoreflect.ValueOfString(s))
			}
		default:
			return nil, fmt.Errorf("row %s: column %q has unsupported value type %T", rec.RecordID, name, value)
		}
	}
	return msg, nil
}
