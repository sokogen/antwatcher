package event

import (
	"embed"
	"path"
	"sort"
	"strings"
)

//go:embed testdata/*.json
var fixtureFS embed.FS

// fixtureT is the subset of testing.TB LoadFixture needs. Taking an interface
// keeps the "testing" package (and its flags) out of production binaries while
// still letting any package's tests call event.LoadFixture(t, name).
type fixtureT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// LoadFixture returns the recorded GitHub webhook body testdata/<name>.json. The
// ".json" suffix is optional. Fixtures are embedded, so tests in other packages
// can use them without depending on the working directory.
func LoadFixture(t fixtureT, name string) []byte {
	t.Helper()
	if !strings.HasSuffix(name, ".json") {
		name += ".json"
	}
	data, err := fixtureFS.ReadFile(path.Join("testdata", name))
	if err != nil {
		t.Fatalf("event fixture %q: %v", name, err)
	}
	return data
}

// Fixtures lists the available fixture names (without the ".json" suffix) in
// sorted order, for table-driven tests that must cover every recorded payload.
func Fixtures() []string {
	entries, err := fixtureFS.ReadDir("testdata")
	if err != nil {
		panic("event: embedded testdata missing: " + err.Error())
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, strings.TrimSuffix(e.Name(), ".json"))
	}
	sort.Strings(names)
	return names
}
