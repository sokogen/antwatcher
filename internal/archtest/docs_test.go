package archtest

import (
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	configReferenceBegin = "<!-- config-reference:begin"
	configReferenceEnd   = "<!-- config-reference:end"

	// configDoc holds the generated configuration reference, written by
	// scripts/readme-config.sh from antwatcher.example.yml.
	configDoc = "docs/configuration.md"
)

// TestConfigDocReferenceMatchesExample pins the configuration reference in
// docs/configuration.md to antwatcher.example.yml: the block between the markers
// must be the example file verbatim inside one yaml fence. `make readme`
// regenerates it. The block used to live in README.md; it moved to keep the
// README consumer-sized, and scripts/readme-config.sh writes the new location.
func TestConfigDocReferenceMatchesExample(t *testing.T) {
	root := moduleRoot(t)
	doc, err := os.ReadFile(filepath.Join(root, configDoc))
	require.NoError(t, err)
	example, err := os.ReadFile(filepath.Join(root, "antwatcher.example.yml"))
	require.NoError(t, err)

	text := string(doc)
	begin := strings.Index(text, configReferenceBegin)
	end := strings.Index(text, configReferenceEnd)
	require.NotEqual(t, -1, begin, "%s lacks the %q marker", configDoc, configReferenceBegin)
	require.NotEqual(t, -1, end, "%s lacks the %q marker", configDoc, configReferenceEnd)
	require.Less(t, begin, end, "config reference markers are out of order")

	block := text[begin:end]
	// Drop the marker line itself, then expect exactly one yaml fence.
	_, block, found := strings.Cut(block, "\n")
	require.True(t, found)
	require.True(t, strings.HasPrefix(block, "```yaml\n"), "config reference must start with a yaml fence")
	require.True(t, strings.HasSuffix(block, "```\n"), "config reference must end with a closing fence")
	body := strings.TrimSuffix(strings.TrimPrefix(block, "```yaml\n"), "```\n")

	assert.Equal(t, string(example), body,
		"%s configuration reference differs from antwatcher.example.yml; run `make readme`", configDoc)
}

// TestREADMEListsEveryShippedDriver keeps the README's driver table in step with
// the drivers wired into the binary: every driver package imported by
// cmd/antwatcher/drivers.go must be named in the README by its Name constant
// (the configuration spelling), not by its package name.
//
// It still reads README.md after the reference material moved into docs/,
// deliberately: docs/configuration.md names every driver inside the generated
// block, so drift there is already impossible, while the README's compact table
// is hand-written and is the only place a new driver can be forgotten.
func TestREADMEListsEveryShippedDriver(t *testing.T) {
	root := moduleRoot(t)
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	require.NoError(t, err)

	var checked int
	for _, file := range sourceFiles(t, filepath.Join(root, "cmd", "antwatcher"), false) {
		if filepath.Base(file.path) != "drivers.go" {
			continue
		}
		for _, imp := range file.file.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if !isDriverPackage(path) {
				continue
			}
			name := driverName(t, packageDir(t, path))
			checked++
			assert.Contains(t, string(readme), "`"+name+"`", "README.md does not mention driver %q (%s)", name, path)
		}
	}
	require.Positive(t, checked, "cmd/antwatcher/drivers.go imports no driver package")
}

// driverName returns the value of the package-level `const Name = "..."` of
// the driver package at dir.
func driverName(t *testing.T, dir string) string {
	t.Helper()
	for _, file := range sourceFiles(t, dir, false) {
		for _, decl := range file.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, ident := range vs.Names {
					if ident.Name != "Name" || i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					name, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					return name
				}
			}
		}
	}
	t.Fatalf("driver package %s has no `const Name = \"...\"`", dir)
	return ""
}
