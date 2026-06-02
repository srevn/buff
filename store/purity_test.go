package store_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline records the store's production dependency budget for this phase and
// bars an accidental coupling import. The store may lean on pure stdlib leaves, the domain
// package, and the followable buffer it sits directly on — nothing that would pull HTTP or the
// CLI into the concurrency spine. The quota adds sync/atomic for its lock-free counters; the
// disk medium adds os (the os.Root it writes through), encoding/json (the durable metadata
// record), and errors (classifying directory states). It does not hash names: a generation's
// directory is named by its globally-unique id alone, so the medium needs no crypto/sha256, and a
// clip's name lives only in the record and the RAM registry. Startup recovery adds two more:
// hash/crc32 for the optional content checksum it computes at finalize and verifies at boot, and
// log/slog for the loud warnings it must emit when it quarantines a generation it cannot
// interpret. Notably absent is syscall: the durable fsync is (*os.File).Sync(), which on darwin
// issues the full-device flush inside the Go runtime, so the medium needs no platform-specific code.
//
// build.ImportDir separates production imports from test-only ones, so this file's own
// go/build and testing imports, and everything the white-box and contract tests pull in
// (including os and path/filepath), are not counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"context":                    true,
		"errors":                     true,
		"fmt":                        true,
		"io":                         true,
		"hash/crc32":                 true,
		"log/slog":                   true,
		"os":                         true,
		"sync":                       true,
		"sync/atomic":                true,
		"time":                       true,
		"crypto/rand":                true,
		"encoding/binary":            true,
		"encoding/hex":               true,
		"encoding/json":              true,
		"github.com/srevn/buff/clip": true,
		"github.com/srevn/buff/store/internal/buffer": true,
	}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("store imports %q, outside this phase's allowed set", imp)
		}
	}
}
