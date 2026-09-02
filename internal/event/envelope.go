// Package event defines the envelope that wraps every GitHub webhook delivery on
// its way through the bus and the mapping between the envelope and a Watermill
// message. The raw GitHub payload is never modified; the envelope only adds the
// delivery metadata that receivers, routers, and sinks need without parsing it.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SchemaVersion is the envelope schema written by this build. FromMessage rejects
// any other value: a message from a newer schema is undecodable by design and is
// surfaced as a permanent error instead of being half-read.
const SchemaVersion = 1

// GitHub webhook request headers read by FromWebhook.
const (
	HeaderDelivery = "X-GitHub-Delivery"
	HeaderEvent    = "X-GitHub-Event"
	HeaderHookID   = "X-GitHub-Hook-ID"
)

// Envelope is one webhook delivery plus the metadata needed to route it.
//
// DeliveryGUID is the stable identity of the event across re-publishing and
// redelivery: it is the message UUID on the ingress bus and the dedup key where
// the broker supports one. RepositoryID and Repository are zero when the payload
// has no repository (organization-level events such as ping).
//
// ForwardedBy and ForwardHops are set only on copies re-published by a forward
// sink: the name of the last sink that forwarded the event and how many times it
// was forwarded. A forward sink refuses an envelope whose hops reached its limit,
// which is what stops a forwarding loop. Both are zero on a delivery received
// from GitHub.
type Envelope struct {
	SchemaVersion int
	DeliveryGUID  string
	Event         string
	Action        string
	HookID        string
	ReceivedAt    time.Time
	RepositoryID  int64
	Repository    string
	ForwardedBy   string
	ForwardHops   int
	Payload       json.RawMessage
}

// Errors returned by FromWebhook and FromMessage. Callers classify all of them as
// permanent: retrying cannot make a malformed delivery well-formed.
var (
	ErrMissingDelivery = errors.New("missing " + HeaderDelivery + " header")
	ErrMissingEvent    = errors.New("missing " + HeaderEvent + " header")
	ErrInvalidPayload  = errors.New("payload is not a JSON object")
	ErrMissingMetadata = errors.New("missing message metadata")
	ErrInvalidMetadata = errors.New("invalid message metadata")
)

// payloadHead is the part of any GitHub webhook body the envelope needs.
type payloadHead struct {
	Action     string `json:"action"`
	Repository *struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
	} `json:"repository"`
}

// FromWebhook builds an Envelope from a GitHub webhook request. It reads the
// delivery GUID, event name, and hook ID from the headers and the action and
// repository from the body. The delivery GUID and event name are required; the
// body must be a JSON object. The body is copied, so the caller may reuse it.
// ReceivedAt is now in UTC.
func FromWebhook(hdr http.Header, body []byte, now time.Time) (Envelope, error) {
	env := Envelope{
		SchemaVersion: SchemaVersion,
		DeliveryGUID:  strings.TrimSpace(hdr.Get(HeaderDelivery)),
		Event:         strings.TrimSpace(hdr.Get(HeaderEvent)),
		HookID:        strings.TrimSpace(hdr.Get(HeaderHookID)),
		ReceivedAt:    now.UTC(),
	}
	if env.DeliveryGUID == "" {
		return Envelope{}, ErrMissingDelivery
	}
	if env.Event == "" {
		return Envelope{}, ErrMissingEvent
	}

	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return Envelope{}, ErrInvalidPayload
	}
	var head payloadHead
	if err := json.Unmarshal(trimmed, &head); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	env.Action = head.Action
	if head.Repository != nil {
		env.RepositoryID = head.Repository.ID
		env.Repository = head.Repository.FullName
	}
	env.Payload = bytes.Clone(body)
	return env, nil
}
