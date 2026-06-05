package tty

import (
	"go/build"
	"strings"
	"testing"
)

// TestImportDiscipline pins tty as a stdlib-only leaf. Terminal detection is the lowest-level thing
// buff does — one ioctl, or a file-mode check where that is absent — so it must depend on nothing but
// the standard library: no buff package (it sits below them all and must never reach back up), and no
// third party (the module's defining rule, which an x/term would break by pulling x/sys with it).
// build.ImportDir reads this platform's production imports only, so the test files' own go/build and
// testing imports are not counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range pkg.Imports {
		if !isStdlib(imp) {
			t.Errorf("tty imports %q; it must stay a stdlib-only leaf (os, syscall, unsafe)", imp)
		}
	}
}

// isStdlib reports whether an import path names a standard-library package: a stdlib path's first
// segment carries no dot ("syscall", "os"), a module path's does ("github.com/...").
func isStdlib(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}
