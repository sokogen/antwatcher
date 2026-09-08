package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// envPattern matches ${VAR} and ${VAR:-default}. There is no escape syntax: a
// literal "${" cannot appear in a configuration value.
var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// Load reads the YAML file at path, expands environment references in scalar
// values, and decodes the result on top of Default. Unknown core fields are
// rejected; driver blocks are kept opaque.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Parse is Load for in-memory YAML.
func Parse(data []byte) (Config, error) {
	return parse(data, os.LookupEnv)
}

func parse(data []byte, lookup func(string) (string, bool)) (Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return Config{}, fmt.Errorf("parse yaml: %w", err)
	}
	cfg := Default()
	if root.Kind == 0 {
		return cfg, nil
	}
	if err := ExpandEnv(&root, lookup); err != nil {
		return Config{}, err
	}
	if err := DecodeStrict(root, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ExpandEnv walks the node tree and expands ${VAR} / ${VAR:-default} inside every
// scalar value (mapping keys are left alone). Plain scalars are re-resolved after
// expansion so `port: ${PORT}` still decodes as a number. A reference to an
// undefined variable without a default is an error naming every such variable.
func ExpandEnv(root *yaml.Node, lookup func(string) (string, bool)) error {
	missing := map[string]struct{}{}
	expandNode(root, lookup, missing)
	if len(missing) == 0 {
		return nil
	}
	names := make([]string, 0, len(missing))
	for name := range missing {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Errorf("undefined environment variables: %s", strings.Join(names, ", "))
}

func expandNode(n *yaml.Node, lookup func(string) (string, bool), missing map[string]struct{}) {
	switch n.Kind {
	case yaml.ScalarNode:
		expandScalar(n, lookup, missing)
	case yaml.MappingNode:
		for i := 1; i < len(n.Content); i += 2 {
			expandNode(n.Content[i], lookup, missing)
		}
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			expandNode(c, lookup, missing)
		}
	case yaml.AliasNode:
		// the anchored node is visited where it is defined
	}
}

const quotedStyles = yaml.SingleQuotedStyle | yaml.DoubleQuotedStyle | yaml.LiteralStyle | yaml.FoldedStyle

func expandScalar(n *yaml.Node, lookup func(string) (string, bool), missing map[string]struct{}) {
	if !strings.Contains(n.Value, "${") {
		return
	}
	n.Value = envPattern.ReplaceAllStringFunc(n.Value, func(ref string) string {
		m := envPattern.FindStringSubmatch(ref)
		name, def, hasDef := m[1], m[2], strings.HasPrefix(ref, "${"+m[1]+":-")
		if v, ok := lookup(name); ok {
			return v
		}
		if hasDef {
			return def
		}
		missing[name] = struct{}{}
		return ""
	})
	// A plain scalar was tagged by resolving its unexpanded text (usually !!str).
	// Drop the tag so the decoder resolves the expanded value instead.
	if n.Style&(quotedStyles|yaml.TaggedStyle) == 0 {
		n.Tag = ""
		if n.Value == "" {
			n.Tag = "!!str" // an empty expansion is an empty string, not null
		}
	}
}

// DecodeStrict decodes node into out, rejecting unknown fields. A zero node (an
// absent block) leaves out untouched so driver defaults survive. Drivers use it
// for their opaque blocks; the loader uses it for the core configuration.
func DecodeStrict(node yaml.Node, out any) error {
	if node.Kind == 0 {
		return nil
	}
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	// yaml.Node.Decode cannot enforce known fields, so the node is re-encoded
	// and decoded through a Decoder. The encoder quotes whatever needs quoting,
	// so expanded values never change the document structure.
	raw, err := yaml.Marshal(&node)
	if err != nil {
		return fmt.Errorf("encode node: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
