package clip_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline keeps clip a pure leaf: its production code may import only errors, time,
// regexp, and unicode/utf8. The last earns its place precisely because it is pure — the canonical
// UTF-8 primitive, with no IO, no global state, and no version-specific data — and the domain
// legitimately needs it: ValidFilename requires valid UTF-8 so a basename round-trips through the
// JSON serializers (meta.json, the list response) that would otherwise silently coerce it. What
// stays out is the large unicode category *tables* (their data shifts between Unicode versions, a
// behaviour two sides could drift on), fmt (whose formatting could echo hostile input), and worst
// of all a sibling buff package — each would make this shared vocabulary couple behaviour between
// the server and the client, the precise thing a pure leaf exists to prevent.
//
// build.ImportDir parses the package source in the current directory (go test runs with the package
// directory as the working directory) and reports production imports in Imports, separately from
// test-only imports — so this file's own go/build and testing imports are not counted against the
// package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"errors": true, "time": true, "regexp": true, "unicode/utf8": true}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("clip imports %q, which is outside the allowed pure set {errors, time, regexp, unicode/utf8}", imp)
		}
	}
}
