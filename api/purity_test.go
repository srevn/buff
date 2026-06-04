package api_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline pins api's production dependency budget and bars an accidental coupling
// import. api is the HTTP edge: it may use stdlib and the three packages it bridges between — the
// domain types (clip), the store seam it relays to, and the wire contract it speaks — but never
// client, cli, or archive, and never the store's internal buffer. It takes the Store interface
// and constructs no store, so a stray import of a concrete backing would be a layering break this
// test catches. The stdlib set is exactly what the handlers, framing, deadlines, recovery, and
// JSON shaping need; net/url is the filename percent-codec, log/slog the 5xx-and-panic logger, and
// context carries request cancellation into the upload's body read.
//
// build.ImportDir separates production imports from test-only ones, so this file's own go/build
// and testing imports — and everything the white-box and end-to-end tests pull in — are not
// counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"cmp":                         true,
		"context":                     true,
		"encoding/json":               true,
		"errors":                      true,
		"fmt":                         true,
		"io":                          true,
		"log/slog":                    true,
		"net/http":                    true,
		"net/url":                     true,
		"slices":                      true,
		"strconv":                     true,
		"sync":                        true,
		"time":                        true,
		"github.com/srevn/buff/clip":  true,
		"github.com/srevn/buff/store": true,
		"github.com/srevn/buff/wire":  true,
	}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("api imports %q, outside this phase's allowed set", imp)
		}
	}
}
