package archive_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline keeps archive a pure leaf: its production code may import only the standard-
// library packages below and — the load-bearing part — no buff package, not clip, wire, or store.
// archive is consumed by cli but depends on nothing of buff's own, which is what lets it be tested
// in isolation against hostile input; a stray sibling import would couple the safe-tar logic to the
// system it is deliberately kept apart from.
//
// time is on the list for one specific reason: the creation side zeroes the access and change times
// that tar.FileInfoHeader copies out of the OS stat, assigning a zero time.Time — without which the
// byte stream would embed a varying atime and stop being deterministic.
//
// build.ImportDir reports production imports separately from test-only ones, so this file's own
// go/build and testing imports are not counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"archive/tar":   true,
		"context":       true,
		"crypto/rand":   true,
		"encoding/hex":  true,
		"errors":        true,
		"fmt":           true,
		"io":            true,
		"io/fs":         true,
		"os":            true,
		"path":          true,
		"path/filepath": true,
		"sort":          true,
		"time":          true,
	}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("archive imports %q, outside the allowed stdlib-only set", imp)
		}
	}
}
