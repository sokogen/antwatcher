package event

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Metadata keys written by ToMessage and read by FromMessage. They are exported
// so bus drivers can use them for broker-side features (dedup headers, filters)
// without re-deriving the envelope.
const (
	MetaDeliveryGUID  = "delivery_guid"
	MetaSchemaVersion = "schema_version"
	MetaEvent         = "event"
	MetaAction        = "action"
	MetaHookID        = "hook_id"
	MetaReceivedAt    = "received_at"
	MetaRepositoryID  = "repository_id"
	MetaRepository    = "repository"
	// MetaForwardedBy and MetaForwardHops mark a copy re-published by a forward
	// sink; see Envelope.ForwardedBy and Envelope.ForwardHops.
	MetaForwardedBy = "antwatcher.forward.by"
	MetaForwardHops = "antwatcher.forward.hops"
)

// ToMessage maps an Envelope onto a Watermill message. The message UUID is the
// delivery GUID, every envelope field is carried in metadata, and the payload is
// the raw GitHub JSON. Optional fields (action, hook_id, repository_id,
// repository, and the forward markers) are omitted from metadata when empty. The
// payload slice is shared, not copied: messages are treated as immutable once
// created.
func ToMessage(env Envelope) *message.Message {
	msg := message.NewMessage(env.DeliveryGUID, message.Payload(env.Payload))
	msg.Metadata.Set(MetaDeliveryGUID, env.DeliveryGUID)
	msg.Metadata.Set(MetaSchemaVersion, strconv.Itoa(env.SchemaVersion))
	msg.Metadata.Set(MetaEvent, env.Event)
	msg.Metadata.Set(MetaReceivedAt, env.ReceivedAt.UTC().Format(time.RFC3339Nano))
	if env.Action != "" {
		msg.Metadata.Set(MetaAction, env.Action)
	}
	if env.HookID != "" {
		msg.Metadata.Set(MetaHookID, env.HookID)
	}
	if env.RepositoryID != 0 {
		msg.Metadata.Set(MetaRepositoryID, strconv.FormatInt(env.RepositoryID, 10))
	}
	if env.Repository != "" {
		msg.Metadata.Set(MetaRepository, env.Repository)
	}
	if env.ForwardedBy != "" {
		msg.Metadata.Set(MetaForwardedBy, env.ForwardedBy)
	}
	if env.ForwardHops > 0 {
		msg.Metadata.Set(MetaForwardHops, strconv.Itoa(env.ForwardHops))
	}
	return msg
}

// FromMessage rebuilds an Envelope from a Watermill message written by ToMessage.
//
// The delivery GUID is read from metadata so it survives re-publishing under a
// different transport UUID; when the metadata key is absent the message UUID is
// used instead, for messages written before the key existed. schema_version,
// event, and received_at are required and must parse; schema_version must equal
// SchemaVersion; repository_id and the forward hop count must be non-negative
// integers when present. Any violation is returned as an error wrapping
// ErrMissingMetadata or ErrInvalidMetadata, which callers treat as permanent.
func FromMessage(msg *message.Message) (Envelope, error) {
	if msg == nil {
		return Envelope{}, fmt.Errorf("%w: nil message", ErrMissingMetadata)
	}
	md := msg.Metadata
	if md == nil {
		md = message.Metadata{}
	}

	guid, ok := md[MetaDeliveryGUID]
	if !ok {
		guid = msg.UUID
	}
	if guid == "" {
		return Envelope{}, fmt.Errorf("%w: %s (and message UUID is empty)", ErrMissingMetadata, MetaDeliveryGUID)
	}

	versionText, err := required(md, MetaSchemaVersion)
	if err != nil {
		return Envelope{}, err
	}
	version, err := strconv.Atoi(versionText)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: %s %q is not an integer", ErrInvalidMetadata, MetaSchemaVersion, versionText)
	}
	if version != SchemaVersion {
		return Envelope{}, fmt.Errorf("%w: %s %d is not supported (this build reads %d)", ErrInvalidMetadata, MetaSchemaVersion, version, SchemaVersion)
	}

	event, err := required(md, MetaEvent)
	if err != nil {
		return Envelope{}, err
	}

	receivedText, err := required(md, MetaReceivedAt)
	if err != nil {
		return Envelope{}, err
	}
	receivedAt, err := time.Parse(time.RFC3339Nano, receivedText)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: %s %q is not RFC 3339", ErrInvalidMetadata, MetaReceivedAt, receivedText)
	}

	var repositoryID int64
	if text, ok := md[MetaRepositoryID]; ok && text != "" {
		repositoryID, err = strconv.ParseInt(text, 10, 64)
		if err != nil {
			return Envelope{}, fmt.Errorf("%w: %s %q is not an integer", ErrInvalidMetadata, MetaRepositoryID, text)
		}
	}

	var hops int
	if text, ok := md[MetaForwardHops]; ok && text != "" {
		hops, err = strconv.Atoi(text)
		if err != nil || hops < 0 {
			return Envelope{}, fmt.Errorf("%w: %s %q is not a non-negative integer", ErrInvalidMetadata, MetaForwardHops, text)
		}
	}

	return Envelope{
		SchemaVersion: version,
		DeliveryGUID:  guid,
		Event:         event,
		Action:        md[MetaAction],
		HookID:        md[MetaHookID],
		ReceivedAt:    receivedAt.UTC(),
		RepositoryID:  repositoryID,
		Repository:    md[MetaRepository],
		ForwardedBy:   md[MetaForwardedBy],
		ForwardHops:   hops,
		Payload:       []byte(msg.Payload),
	}, nil
}

func required(md message.Metadata, key string) (string, error) {
	v, ok := md[key]
	if !ok || v == "" {
		return "", fmt.Errorf("%w: %s", ErrMissingMetadata, key)
	}
	return v, nil
}
