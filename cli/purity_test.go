package cli_test

import (
	"go/build"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestImportDiscipline pins cli's production dependency budget. cli is the first package to compose
// the others, but it composes only the four it is allowed to: the domain types, the wire client
// (transport), the safe archiver, and the human unit vocabulary its listing renders through. It
// must never reach below the client into the store, the api, or even the wire constants — the
// client hides the protocol — so any non-stdlib import outside that set is a coupling break.
//
// units is a leaf with no buff imports of its own, so admitting it couples cli to nothing new;
// it is here because the server's config parses through the same vocabulary, and a listing that
// renders a span the server's --ttl would reject is the exact drift a shared package exists to
// prevent.
//
// The check allows any standard-library package (a stdlib import path's first segment carries no
// dot) plus exactly clip, client, archive, and units. Enumerating the stdlib set would be churn
// across this package's two-step build; what matters is that nothing from store/api/wire or a third
// party creeps in, which this catches directly.
//
// build.ImportDir separates production imports from test-only ones, so this file's own imports —
// and the api and store the flow tests pull in to stand up a real server — are not counted against
// the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowedBuff := map[string]bool{
		"github.com/srevn/buff/clip":    true,
		"github.com/srevn/buff/client":  true,
		"github.com/srevn/buff/archive": true,
		"github.com/srevn/buff/units":   true,
	}
	for _, imp := range pkg.Imports {
		if isStdlib(imp) || allowedBuff[imp] {
			continue
		}
		t.Errorf("cli imports %q, outside {clip, client, archive, units} + stdlib", imp)
	}
}

// isStdlib reports whether an import path names a standard-library package. A stdlib path's first
// segment has no dot ("net/http", "io"); a module path's does ("github.com/...").
func isStdlib(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}

// TestNoThirdPartyDeps guards the design's defining property: a near-empty go.mod whose only
// require is the single curated runtime dependency. cli itself adds none — its tar pipe is plain
// stdlib goroutines — but the server wiring brings golang.org/x/sync for the errgroup its goroutine
// group is built on, the one module the spec sanctions. That path is allowed; any other require is
// an unsanctioned dependency that crept in and must be justified before it ships.
func TestNoThirdPartyDeps(t *testing.T) {
	data, err := os.ReadFile(goModPath())
	if err != nil {
		t.Fatal(err)
	}
	const sanctioned = "golang.org/x/sync"
	for _, path := range requiredModules(string(data)) {
		if path != sanctioned {
			t.Errorf("go.mod requires %q, outside the sanctioned set {%s}", path, sanctioned)
		}
	}
}

// requiredModules extracts the module path of every require directive in a go.mod, handling both
// the single-line form (require path version) and the block form (require ( ... )). It is a small
// hand parser rather than a dependency on golang.org/x/mod, which would itself be a require this
// very test exists to guard against.
func requiredModules(gomod string) []string {
	var paths []string
	inBlock := false
	for raw := range strings.SplitSeq(gomod, "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case inBlock && line == ")":
			inBlock = false
		case inBlock:
			if f := strings.Fields(line); len(f) > 0 && !strings.HasPrefix(f[0], "//") {
				paths = append(paths, f[0])
			}
		case line == "require (":
			inBlock = true
		case strings.HasPrefix(line, "require "):
			if f := strings.Fields(line); len(f) >= 2 {
				paths = append(paths, f[1]) // f[0] is "require"
			}
		}
	}
	return paths
}

// goModPath returns the absolute path of the module's go.mod, derived from this test file's own
// compiled location rather than a working-directory-relative "../go.mod". The absolute form is what
// makes go's test cache track the file: a relative read outside the package directory is cached
// but not invalidated when go.mod changes, so a require slipping in could be masked by a stale pass
// on a warm cache; reading the resolved path lets the testlog record it and re-run the guard when
// go.mod actually changes.
func goModPath() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "go.mod")
}
