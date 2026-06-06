package buffer_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline keeps the followable buffer a leaf of the domain core: its production code
// may import only context, errors, io, os, sync, and clip — pure stdlib leaves plus the domain
// package. The budget exists to bar a coupling import (net/http, the store) that would break
// the inward-only dependency arrows; errors is a pure leaf that lets the buffer return a typed
// negative-offset error, and os is the disk backing's file descriptors — the bytes a backing keeps.
// os carries no layering risk here because the disk backing receives its descriptors from above (an
// injected opener and a handed-in append descriptor); it never learns the store's on-disk layout,
// the *os.Root, or the data directory, so the package stays ignorant of where in the tree its bytes
// live.
//
// build.ImportDir reports production imports separately from test-only imports, so this file's
// own go/build and testing imports — and every white-box test backing — are not counted against
// the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"context":                    true,
		"errors":                     true,
		"io":                         true,
		"os":                         true,
		"sync":                       true,
		"github.com/srevn/buff/clip": true,
	}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("buffer imports %q, outside the allowed set {context, errors, io, os, sync, clip}", imp)
		}
	}
}
