package wire_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/srevn/buff/wire"
)

// TestHeadersEnumeratesDeclaredConsts is to wire.Headers what TestRowsEnumeratesDeclaredVars is to
// Rows: it makes the canonical list authoritative rather than a hand-kept one a header can fall out
// of. The spelling pin in wire_test.go ranges Headers, so it is blind to a header const declared but
// left out of Headers — the very omission that once dropped Buff-Executable from the count. This
// shuts that door the way purity_test.go shuts the import door: by reading the package's own source.
// It parses every production file, collects every package-level const whose name begins "Header"
// (the prefix every header const carries and no other const does) and the identifiers listed in the
// Headers literal, and asserts the two sets are equal. So "add a Header const, forget Headers" is a
// build failure here, which then forces the spelling pin in wire_test.go — the drift chain made
// unforgettable, not merely unlikely.
func TestHeadersEnumeratesDeclaredConsts(t *testing.T) {
	// build.ImportDir lists production files only (no _test.go), so this parses just the contract
	// source, never itself. The package dir is the working directory under go test, the same
	// assumption purity_test.go and the Rows source test make.
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}

	declared := map[string]bool{} // package-level consts named Header*
	listed := map[string]bool{}   // distinct identifiers inside the Headers literal
	listedCount := 0              // total entries in Headers, so a duplicate reference is visible
	foundHeaders := false

	fset := token.NewFileSet()
	for _, file := range pkg.GoFiles {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			switch gen.Tok {
			case token.CONST: // the header const block: collect every Header* name it declares
				for _, spec := range gen.Specs {
					for _, name := range spec.(*ast.ValueSpec).Names {
						if strings.HasPrefix(name.Name, "Header") {
							declared[name.Name] = true
						}
					}
				}
			case token.VAR: // var Headers = []string{HeaderX, ...}: collect the names it lists
				for _, spec := range gen.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						if name.Name != "Headers" || i >= len(vs.Values) {
							continue
						}
						lit, ok := vs.Values[i].(*ast.CompositeLit)
						if !ok {
							t.Fatalf("Headers is not a composite literal (%T)", vs.Values[i])
						}
						foundHeaders = true
						for _, e := range lit.Elts {
							id, ok := e.(*ast.Ident)
							if !ok {
								t.Errorf("a Headers element is not a bare const reference (%T); Headers must name the consts, never re-spell their values", e)
								continue
							}
							listed[id.Name] = true
							listedCount++
						}
					}
				}
			}
		}
	}

	if !foundHeaders {
		t.Fatal("found no Headers var; expected wire.Headers to enumerate the header consts")
	}
	if listedCount != len(listed) {
		t.Errorf("Headers has %d entries but only %d are distinct — a header is listed twice", listedCount, len(listed))
	}
	// Cross-check the parsed source against the value the package exports at runtime, so a parsing
	// blind spot cannot let the two disagree unnoticed.
	if listedCount != len(wire.Headers) {
		t.Errorf("source Headers lists %d entries but wire.Headers has %d at runtime", listedCount, len(wire.Headers))
	}
	for name := range declared {
		if !listed[name] {
			t.Errorf("const %s is a Header* name but is missing from Headers", name)
		}
	}
	for name := range listed {
		if !declared[name] {
			t.Errorf("Headers lists %s, which is not a declared Header* const", name)
		}
	}
}
