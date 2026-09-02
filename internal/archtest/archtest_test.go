// Package archtest pins the architecture rules of the core pipeline as
// tests: which packages may depend on a concrete driver, which GitHub APIs
// may be called, where retry logic may live, and that every sink driver
// classifies its errors. The checks read the module source through go/ast
// and the import graph through `go list -deps`, so they need no network and
// fail the moment a rule is broken, wherever the offending code lives.
package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const module = "github.com/sokogen/antwatcher"

// Driver packages: the only concrete bus and sink drivers. Everything else,
// except the cmd wiring, must build without them.
var busDrivers = []string{
	module + "/internal/bus/gochannel",
	module + "/internal/bus/natsjs",
}

func isDriverPackage(importPath string) bool {
	return slices.Contains(busDrivers, importPath) || strings.Contains(importPath, "/drivers/")
}

func isCmdPackage(importPath string) bool {
	return strings.HasPrefix(importPath, module+"/cmd/")
}

// moduleRoot walks up from the package directory to the go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "go.mod not found above %s", dir)
		dir = parent
	}
}

// goList runs `go list` with args from the module root and returns the
// lines of its output.
func goList(t *testing.T, args ...string) []string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Dir = moduleRoot(t)
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		require.NoError(t, err, "go list %s: %s", strings.Join(args, " "), stderr)
	}
	return strings.Fields(string(out))
}

// modulePackages lists every package of the module.
func modulePackages(t *testing.T) []string {
	t.Helper()
	return goList(t, "./...")
}

// moduleDeps returns the module-internal packages in the non-test build
// closure of pkg, excluding pkg itself.
func moduleDeps(t *testing.T, pkg string) []string {
	t.Helper()
	var deps []string
	for _, dep := range goList(t, "-deps", "-f", "{{.ImportPath}}", pkg) {
		if dep != pkg && strings.HasPrefix(dep, module+"/") {
			deps = append(deps, dep)
		}
	}
	return deps
}

// packageDir maps an import path of the module to its directory.
func packageDir(t *testing.T, importPath string) string {
	t.Helper()
	return filepath.Join(moduleRoot(t), filepath.FromSlash(strings.TrimPrefix(importPath, module+"/")))
}

// sourceFile is one parsed non-test Go file.
type sourceFile struct {
	path string // relative to the module root, slash separated
	file *ast.File
}

// sourceFiles parses every non-test Go file under dir (recursively when
// recursive is set), skipping this package.
func sourceFiles(t *testing.T, dir string, recursive bool) []sourceFile {
	t.Helper()
	root := moduleRoot(t)
	self, err := os.Getwd()
	require.NoError(t, err)
	fset := token.NewFileSet()
	var files []sourceFile
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch {
			case path == dir:
				return nil
			case !recursive, d.Name() == ".git", d.Name() == "bin", d.Name() == "testdata", path == self:
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, sourceFile{path: filepath.ToSlash(rel), file: f})
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, files, "no Go files under %s", dir)
	return files
}

// allSourceFiles parses every non-test Go file of the module.
func allSourceFiles(t *testing.T) []sourceFile {
	t.Helper()
	return sourceFiles(t, moduleRoot(t), true)
}

// selectorName renders X.Sel for an expression rooted in identifiers, for
// example "time.Sleep", "c.gh.Actions.ListWorkflowRuns", or "" when the
// chain starts in something else (a call, an index, a literal).
func selectorName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		base := selectorName(x.X)
		if base == "" {
			return ""
		}
		return base + "." + x.Sel.Name
	default:
		return ""
	}
}

// calls returns every "X.Sel" chain that is called in f, with a receiver
// that is not an identifier chain rendered as "*". Comments never match:
// only the syntax tree is inspected.
func calls(f *ast.File) []string {
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		name := selectorName(sel)
		if name == "" {
			name = "*." + sel.Sel.Name
		}
		out = append(out, name)
		return true
	})
	return out
}

// selectors returns every "X.Sel" chain referenced in f, called or not.
func selectors(f *ast.File) []string {
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if name := selectorName(sel); name != "" {
				out = append(out, name)
			}
		}
		return true
	})
	return out
}

// imports returns the import paths of f.
func imports(f *ast.File) []string {
	out := make([]string, 0, len(f.Imports))
	for _, imp := range f.Imports {
		out = append(out, strings.Trim(imp.Path.Value, `"`))
	}
	return out
}

// TestOnlyCmdDependsOnConcreteDrivers: the receiver, the sink root and every
// class package, the recovery loop, and every other non-driver package
// build without any bus or sink driver. The cmd wiring is the one place
// that imports them (cmd/antwatcher/drivers.go). Drivers themselves are
// exempt but must not import one another either.
func TestOnlyCmdDependsOnConcreteDrivers(t *testing.T) {
	core := []string{
		module + "/internal/receiver",
		module + "/internal/sink",
		module + "/internal/recovery",
	}
	pkgs := modulePackages(t)
	for _, want := range core {
		require.Contains(t, pkgs, want, "core package renamed; update the rule")
	}
	require.NotEmpty(t, slices.DeleteFunc(slices.Clone(pkgs), func(p string) bool { return !isDriverPackage(p) }), "no driver packages found; update the rule")

	for _, pkg := range pkgs {
		if isCmdPackage(pkg) {
			continue
		}
		for _, dep := range moduleDeps(t, pkg) {
			assert.False(t, isDriverPackage(dep), "%s depends on the concrete driver %s", pkg, dep)
		}
	}
}

// TestActionsRESTAPIIsNeverUsed: runs, jobs, and steps come from webhooks
// only. go-github is confined to internal/ghclient and that package talks
// to the hook deliveries and hooks endpoints alone.
func TestActionsRESTAPIIsNeverUsed(t *testing.T) {
	allowed := []string{
		"Organizations.ListHookDeliveries",
		"Organizations.RedeliverHookDelivery",
		"Organizations.ListHooks",
		"Repositories.ListHookDeliveries",
		"Repositories.RedeliverHookDelivery",
		"Repositories.ListHooks",
	}
	var used []string
	for _, sf := range allSourceFiles(t) {
		inGHClient := strings.HasPrefix(sf.path, "internal/ghclient/")
		for _, imp := range imports(sf.file) {
			if strings.HasPrefix(imp, "github.com/google/go-github/") {
				assert.True(t, inGHClient, "%s imports go-github; only internal/ghclient may", sf.path)
			}
		}
		for _, name := range selectors(sf.file) {
			parts := strings.Split(name, ".")
			assert.NotContains(t, parts, "Actions", "%s references %s: the Actions REST API is never used", sf.path, name)
			// service.Method chains on the go-github client: <recv>.gh.<Service>.<Method>
			if i := slices.Index(parts, "gh"); inGHClient && i >= 0 && len(parts) == i+3 {
				used = append(used, parts[i+1]+"."+parts[i+2])
			}
		}
	}
	sort.Strings(used)
	used = slices.Compact(used)
	require.NotEmpty(t, used, "no go-github calls found in internal/ghclient; the rule no longer matches the code")
	for _, call := range used {
		assert.Contains(t, allowed, call, "internal/ghclient calls go-github %s, outside the hook deliveries surface", call)
	}
}

// TestHandlersNeverSleepRetryOrAckThemselves: the retry policy is "short
// bounded retries in the destination client, long-term retry by the
// broker". The router therefore has no Retry or PoisonQueue middleware, no
// handler sleeps, and no sink code acks or nacks a message itself: the
// handler's return value alone decides, so a failed message is never acked.
func TestHandlersNeverSleepRetryOrAckThemselves(t *testing.T) {
	allowedMiddleware := []string{"middleware.Recoverer", "middleware.RecoveredPanicError"}
	for _, sf := range allSourceFiles(t) {
		for _, name := range selectors(sf.file) {
			if strings.HasPrefix(name, "middleware.") {
				assert.Contains(t, allowedMiddleware, name, "%s uses Watermill %s", sf.path, name)
			}
		}
	}

	sinkTree := packageDir(t, module+"/internal/sink")
	processing := sourceFiles(t, sinkTree, true)
	processing = append(processing, sourceFiles(t, packageDir(t, module+"/internal/receiver"), false)...)
	processing = append(processing, sourceFiles(t, packageDir(t, module+"/internal/recovery"), false)...)
	for _, sf := range processing {
		for _, name := range calls(sf.file) {
			assert.NotEqual(t, "time.Sleep", name, "%s sleeps: handlers never sleep, the broker paces redelivery", sf.path)
			for _, forbidden := range []string{".Ack", ".Nack"} {
				assert.False(t, strings.HasSuffix(name, forbidden), "%s calls %s: only the handler return value acks or nacks", sf.path, name)
			}
		}
	}
}

// TestEverySinkDriverClassifiesErrors: each sink driver package, or the
// shared client or class package it delegates its failures to, marks the
// errors a retry cannot fix with sink.Permanent. A driver whose closure
// contains no classification at all would make every failure retryable by
// accident rather than by decision.
func TestEverySinkDriverClassifiesErrors(t *testing.T) {
	foundation := []string{
		module + "/internal/bus",
		module + "/internal/config",
		module + "/internal/event",
		module + "/internal/metrics",
		module + "/internal/model",
		module + "/internal/sink",
	}
	classifies := func(pkg string) []string {
		var where []string
		for _, sf := range sourceFiles(t, packageDir(t, pkg), false) {
			for _, name := range calls(sf.file) {
				if name == "sink.Permanent" || name == "sink.Permanentf" {
					where = append(where, sf.path)
					break
				}
			}
		}
		return where
	}

	var drivers []string
	for _, pkg := range modulePackages(t) {
		if strings.Contains(pkg, "/drivers/") {
			drivers = append(drivers, pkg)
		}
	}
	require.Len(t, drivers, 6, "sink driver packages: %v", drivers)

	for _, driver := range drivers {
		t.Run(strings.TrimPrefix(driver, module+"/internal/sink/"), func(t *testing.T) {
			candidates := append([]string{driver}, moduleDeps(t, driver)...)
			var where []string
			for _, pkg := range candidates {
				if slices.Contains(foundation, pkg) {
					continue
				}
				where = append(where, classifies(pkg)...)
			}
			assert.NotEmpty(t, where, "%s and the packages it delegates to never call sink.Permanent", driver)
			t.Logf("classified in %s", strings.Join(where, ", "))
		})
	}
}
