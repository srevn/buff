package units_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestImportDiscipline pins units as a leaf. It is the one package both edges of the binary share
// — the client's CLI and the server's config each parse and render through it — so it must depend
// on neither, and on nothing else in the module either. An import from inside buff would quietly
// turn a shared vocabulary into a coupling channel between the two edges, and the first one to try
// it will be the one that drags the store or the wire into a package whose entire job is formatting
// text.
//
// The check allows any standard-library package (a stdlib import path's first segment carries no
// dot) and nothing else, which also keeps the module's near-empty go.mod honest from this side.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range pkg.Imports {
		if isStdlib(imp) {
			continue
		}
		t.Errorf("units imports %q; it must be stdlib-only", imp)
	}
}

// isStdlib reports whether an import path names a standard-library package. A stdlib path's first
// segment has no dot ("strconv", "unicode/utf8"); a module path's does ("github.com/...").
func isStdlib(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}
