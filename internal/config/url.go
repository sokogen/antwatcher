package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"gopkg.in/yaml.v3"
)

// URL is a connection URL that may carry credentials in its userinfo, as
// nats://user:pass@host:4222 does. It decodes from YAML like a string, but
// every rendering path (String, YAML, JSON, fmt verbs) replaces the userinfo
// with Mask so `-check`, `/status` and the startup log show where the service
// connects without printing the password. Only Reveal returns the value.
//
// A comma-separated list is masked element by element, which is how NATS and
// several other clients spell a cluster.
type URL string

// Reveal returns the raw URL, credentials included.
func (u URL) Reveal() string { return string(u) }

// String returns the URL with any userinfo replaced by Mask. A value that does
// not parse as a URL is returned as Mask rather than verbatim: an unparseable
// string may still hold a credential.
func (u URL) String() string {
	if u == "" {
		return ""
	}
	parts := strings.Split(string(u), ",")
	for i, p := range parts {
		parts[i] = maskUserinfo(strings.TrimSpace(p))
	}
	return strings.Join(parts, ",")
}

// schemePlaceholder is prepended to a scheme-less URL before parsing and
// stripped from the result. url.Parse reads "admin:hunter2@host:4222" as the
// opaque URL of a scheme named "admin" and reports no userinfo, so without
// this the password of a scheme-less URL -- which nats.go accepts, prepending
// the scheme itself -- would be rendered verbatim.
const schemePlaceholder = "mask://"

func maskUserinfo(raw string) string {
	prefix := ""
	if !strings.Contains(raw, "://") {
		prefix = schemePlaceholder
	}
	parsed, err := url.Parse(prefix + raw)
	if err != nil {
		return Mask
	}
	if parsed.User == nil {
		return raw
	}
	// Mask is spliced in textually: url.User would percent-encode it.
	parsed.User = nil
	stripped := parsed.String()
	i := strings.Index(stripped, "//")
	if i < 0 {
		return Mask
	}
	return strings.TrimPrefix(stripped[:i+2]+Mask+"@"+stripped[i+2:], prefix)
}

// GoString keeps %#v from leaking the credentials.
func (u URL) GoString() string { return fmt.Sprintf("config.URL(%q)", u.String()) }

// MarshalYAML renders the masked form.
func (u URL) MarshalYAML() (any, error) { return u.String(), nil }

// MarshalJSON renders the masked form.
func (u URL) MarshalJSON() ([]byte, error) { return json.Marshal(u.String()) }

// UnmarshalYAML accepts any scalar and stores its text.
func (u *URL) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: url must be a scalar", node.Line)
	}
	var v string
	if err := node.Decode(&v); err != nil {
		return err
	}
	*u = URL(v)
	return nil
}
