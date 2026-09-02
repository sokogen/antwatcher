package config

import (
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Mask replaces secret values in every rendered form of the configuration.
const Mask = "***"

// Secret is a credential. It decodes from YAML like a string but every rendering
// path (String, YAML, JSON, fmt verbs) prints Mask; only Reveal returns the value.
// An empty Secret renders as an empty string so absent credentials stay visible.
type Secret string

// Reveal returns the raw secret value.
func (s Secret) Reveal() string { return string(s) }

// String returns Mask for a non-empty secret and "" for an empty one.
func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return Mask
}

// GoString keeps %#v from leaking the value.
func (s Secret) GoString() string { return fmt.Sprintf("config.Secret(%q)", s.String()) }

// MarshalYAML renders the masked form.
func (s Secret) MarshalYAML() (any, error) { return s.String(), nil }

// MarshalJSON renders the masked form.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(s.String()) }

// UnmarshalYAML accepts any scalar and stores its text.
func (s *Secret) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: secret must be a scalar", node.Line)
	}
	var v string
	if err := node.Decode(&v); err != nil {
		return err
	}
	*s = Secret(v)
	return nil
}
