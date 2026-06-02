package client_test

import (
	"go/build"
	"testing"
)

// TestImportDiscipline pins the client's production dependency budget. The client is a pure
// wire peer of the server: it may use stdlib and the two leaf packages both sides share —
// the domain types and the protocol constants — but never the store, the api, the archive,
// or the cli. Importing the api in particular would couple the client to one server and
// break the rule that the two agree only through the wire. The stdlib set is exactly what
// the requests, the header and JSON codecs, the completion body, and the typed errors need.
//
// build.ImportDir separates production imports from test-only ones, so this file's own
// go/build and testing imports — and the api and store the other tests pull in to stand up a
// real server — are not counted against the package.
func TestImportDiscipline(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"context":                    true,
		"encoding/json":              true,
		"errors":                     true,
		"fmt":                        true,
		"io":                         true,
		"net/http":                   true,
		"net/url":                    true,
		"strconv":                    true,
		"strings":                    true,
		"time":                       true,
		"github.com/srevn/buff/clip": true,
		"github.com/srevn/buff/wire": true,
	}
	for _, imp := range pkg.Imports {
		if !allowed[imp] {
			t.Errorf("client imports %q, outside this package's allowed set", imp)
		}
	}
}
