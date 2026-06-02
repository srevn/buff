package wire_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline keeps wire a zero-import contract package. If it imported
// anything — net/http for its status constants is the tempting one — the server and
// client would no longer share an inert, drift-proof contract. Integer statuses exist
// precisely so net/http need never be imported here.
//
// build.ImportDir reports production imports separately from test-only imports, so
// this file's own go/build and testing imports are not counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkg.Imports) != 0 {
		t.Errorf("wire must import nothing, but its production code imports %v", pkg.Imports)
	}
}
