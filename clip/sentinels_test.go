package clip_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"testing"

	"github.com/srevn/buff/clip"
)

// TestSentinelsEnumeratesDeclared is what makes clip.Sentinels authoritative rather than a fourth
// hand-maintained list. Go cannot enumerate a package's vars at runtime, so the completeness tests
// in api/ and cli/ can only range clip.Sentinels — they are blind to a sentinel that was declared
// but left out of it, which would silently relocate the very gap Sentinels exists to close. This
// test shuts that door the way purity_test.go shuts the import door: by reading the package's own
// source. It parses every production file, collects the names of all package-level vars made with
// errors.New and the identifiers listed in the []error Sentinels literal, and asserts the two sets
// are equal. So "add a sentinel, forget Sentinels" is a build failure here, which in turn forces
// the forward-map coverage (api/) and the exit coverage (cli/) — every link made unforgettable.
//
// Keying on errors.New is exact for clip, not a heuristic: the package's purity test forbids
// fmt, so errors.New is the only constructor a sentinel can take, and any sentinel must be one. A
// sentinel built from a clip-local error type would escape this keying — exactly as a non-ErrInfo
// row would escape wire's row test — but that form is foreclosed here, so it is the documented
// boundary, not a latent gap.
func TestSentinelsEnumeratesDeclared(t *testing.T) {
	// build.ImportDir lists production files only (no _test.go), so this parses just the package
	// source, never itself. The package dir is the working directory under go test, the same
	// assumption purity_test.go makes.
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}

	declared := map[string]bool{} // package-level vars constructed with errors.New
	listed := map[string]bool{}   // distinct identifiers inside the []error Sentinels literal
	listedCount := 0              // total entries, so a duplicate reference is visible
	foundList := false

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
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					switch v := vs.Values[i].(type) {
					case *ast.CallExpr: // var ErrX = errors.New("...") — the form every sentinel takes
						sel, ok := v.Fun.(*ast.SelectorExpr)
						if !ok {
							continue
						}
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "errors" && sel.Sel.Name == "New" {
							declared[name.Name] = true
						}
					case *ast.CompositeLit: // var Sentinels = []error{ErrX, ...}
						at, ok := v.Type.(*ast.ArrayType)
						if !ok {
							continue
						}
						if elt, ok := at.Elt.(*ast.Ident); !ok || elt.Name != "error" {
							continue
						}
						if foundList {
							t.Fatal("found more than one []error literal; this test assumes exactly one (Sentinels)")
						}
						foundList = true
						for _, e := range v.Elts {
							id, ok := e.(*ast.Ident)
							if !ok {
								t.Errorf("a Sentinels element is not a bare var reference (%T); Sentinels must name the vars, never re-spell them", e)
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

	if !foundList {
		t.Fatal("found no []error literal; expected clip.Sentinels to enumerate the sentinels")
	}
	if listedCount != len(listed) {
		t.Errorf("Sentinels has %d entries but only %d are distinct — a sentinel is listed twice", listedCount, len(listed))
	}
	// Cross-check the parsed source against the value the package actually exports, so a parsing blind
	// spot cannot let the two disagree unnoticed.
	if listedCount != len(clip.Sentinels) {
		t.Errorf("source Sentinels lists %d entries but clip.Sentinels has %d at runtime", listedCount, len(clip.Sentinels))
	}
	for name := range declared {
		if !listed[name] {
			t.Errorf("var %s is an errors.New sentinel but is missing from Sentinels", name)
		}
	}
	for name := range listed {
		if !declared[name] {
			t.Errorf("Sentinels lists %s, which is not a declared errors.New sentinel", name)
		}
	}
}
