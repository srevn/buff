package wire_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"testing"

	"github.com/srevn/buff/wire"
)

// TestRowsEnumeratesDeclaredVars is what makes wire.Rows authoritative rather than just a fourth
// hand-maintained list. Go cannot enumerate a package's exported vars at runtime, so the coverage
// tests in api/ and client/ can only range wire.Rows — they are blind to a row var that was added
// but left out of Rows, which would silently relocate the very gap Rows exists to close. This
// test shuts that door the way purity_test.go shuts the import door: by reading the package's own
// source. It parses every production file, collects the names of all package-level ErrInfo vars
// and the identifiers listed in the []ErrInfo Rows literal, and asserts the two sets are equal. So
// "add a var, forget Rows" is a build failure here, which forces the value pin (wire_test.go) and
// then the map coverage (api/, client/) — every link in the drift chain made unforgettable, not
// just unlikely.
func TestRowsEnumeratesDeclaredVars(t *testing.T) {
	// build.ImportDir lists production files only (no _test.go), so this test parses just the
	// contract source, never itself. The package dir is the working directory under go test, the same
	// assumption purity_test.go makes.
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}

	declared := map[string]bool{} // package-level vars of type ErrInfo
	listed := map[string]bool{}   // distinct identifiers inside the []ErrInfo Rows literal
	listedCount := 0              // total entries in Rows, so a duplicate reference is visible
	foundRows := false

	fset := token.NewFileSet()
	for _, file := range pkg.GoFiles {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)

				// An explicit `var ErrX ErrInfo [= ...]` names ErrInfo rows up front, whatever the value form;
				// the untyped `var ErrX = ErrInfo{...}` form is caught by its value below.
				if id, ok := vs.Type.(*ast.Ident); ok && id.Name == "ErrInfo" {
					for _, name := range vs.Names {
						declared[name.Name] = true
					}
				}

				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.CompositeLit)
					if !ok {
						continue
					}
					switch typ := lit.Type.(type) {
					case *ast.Ident: // var ErrX = ErrInfo{...}
						if typ.Name == "ErrInfo" {
							declared[name.Name] = true
						}
					case *ast.ArrayType: // var Rows = []ErrInfo{ErrX, ...}
						if elt, ok := typ.Elt.(*ast.Ident); !ok || elt.Name != "ErrInfo" {
							continue
						}
						if foundRows {
							t.Fatal("found more than one []ErrInfo literal; this test assumes exactly one (Rows)")
						}
						foundRows = true
						for _, e := range lit.Elts {
							id, ok := e.(*ast.Ident)
							if !ok {
								t.Errorf("a Rows element is not a bare var reference (%T); Rows must name the vars, never re-spell their values", e)
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

	if !foundRows {
		t.Fatal("found no []ErrInfo literal; expected wire.Rows to enumerate the rows")
	}
	if listedCount != len(listed) {
		t.Errorf("Rows has %d entries but only %d are distinct — a row is listed twice", listedCount, len(listed))
	}
	// Cross-check the parsed source against the value the package actually exports, so a parsing blind
	// spot cannot let the two disagree unnoticed.
	if listedCount != len(wire.Rows) {
		t.Errorf("source Rows lists %d entries but wire.Rows has %d at runtime", listedCount, len(wire.Rows))
	}
	for name := range declared {
		if !listed[name] {
			t.Errorf("var %s is an ErrInfo row but is missing from Rows", name)
		}
	}
	for name := range listed {
		if !declared[name] {
			t.Errorf("Rows lists %s, which is not a declared ErrInfo var", name)
		}
	}
}
