package clip_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline keeps clip a pure leaf: its production code may import only
// errors, time, and regexp. Anything else — fmt, a unicode table, or worst of all a
// sibling buff package — would make this shared vocabulary couple behaviour between
// the server and the client, the precise thing a pure leaf exists to prevent.
//
// build.ImportDir parses the package source in the current directory (go test runs
// with the package directory as the working directory) and reports production imports
// in Imports, separately from test-only imports — so this file's own go/build and
// testing imports are not counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"errors": true, "time": true, "regexp": true}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("clip imports %q, which is outside the allowed pure set {errors, time, regexp}", imp)
		}
	}
}
