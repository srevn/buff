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

// TestFeaturesEnumeratesDeclaredConsts is to wire.Features what TestHeadersEnumeratesDeclaredConsts
// is to Headers: it makes the advertised set authoritative rather than a hand-kept one a capability
// can fall out of. The value pin in wire_test.go ranges Features, so it is blind to a Feature const
// declared but left out of Features — the same omission that once dropped a header from the count.
// This shuts that door the way purity_test.go shuts the import door: by reading the package's own
// source. It parses every production file, collects every package-level const whose name begins
// "Feature" (the prefix every capability const carries and no other const does — the Features var is
// a token.VAR, so the const scan never picks it up) and the identifiers listed in the Features
// literal, and asserts the two sets are equal. So "add a Feature const, forget Features" is a build
// failure here, which then forces the value pin in wire_test.go. Features is diagnostic-only — a
// miss is a cosmetic /health under-report, nothing gates on it — but the drift guard is the same
// discipline Rows and Headers carry, applied uniformly across the vocabulary.
func TestFeaturesEnumeratesDeclaredConsts(t *testing.T) {
	// build.ImportDir lists production files only (no _test.go), so this parses just the contract
	// source, never itself. The package dir is the working directory under go test, the same
	// assumption purity_test.go and the Headers source test make.
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}

	declared := map[string]bool{} // package-level consts named Feature*
	listed := map[string]bool{}   // distinct identifiers inside the Features literal
	listedCount := 0              // total entries in Features, so a duplicate reference is visible
	foundFeatures := false

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
			case token.CONST: // the feature const block: collect every Feature* name it declares
				for _, spec := range gen.Specs {
					for _, name := range spec.(*ast.ValueSpec).Names {
						if strings.HasPrefix(name.Name, "Feature") {
							declared[name.Name] = true
						}
					}
				}
			case token.VAR: // var Features = []string{FeatureX, ...}: collect the names it lists
				for _, spec := range gen.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						if name.Name != "Features" || i >= len(vs.Values) {
							continue
						}
						lit, ok := vs.Values[i].(*ast.CompositeLit)
						if !ok {
							t.Fatalf("Features is not a composite literal (%T)", vs.Values[i])
						}
						foundFeatures = true
						for _, e := range lit.Elts {
							id, ok := e.(*ast.Ident)
							if !ok {
								t.Errorf("a Features element is not a bare const reference (%T); Features must name the consts, never re-spell their values", e)
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

	if !foundFeatures {
		t.Fatal("found no Features var; expected wire.Features to enumerate the feature consts")
	}
	if listedCount != len(listed) {
		t.Errorf("Features has %d entries but only %d are distinct — a feature is listed twice", listedCount, len(listed))
	}
	// Cross-check the parsed source against the value the package exports at runtime, so a parsing
	// blind spot cannot let the two disagree unnoticed.
	if listedCount != len(wire.Features) {
		t.Errorf("source Features lists %d entries but wire.Features has %d at runtime", listedCount, len(wire.Features))
	}
	for name := range declared {
		if !listed[name] {
			t.Errorf("const %s is a Feature* name but is missing from Features", name)
		}
	}
	for name := range listed {
		if !declared[name] {
			t.Errorf("Features lists %s, which is not a declared Feature* const", name)
		}
	}
}
