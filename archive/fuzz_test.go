package archive

import (
	"path/filepath"
	"strings"
	"testing"
)

// FuzzExtractPath asserts the security invariant of safeName, the validator every untrusted
// tar entry name passes through before it becomes a filesystem path: any name it accepts
// must resolve to a path that stays inside the destination — local, relative, and free of a
// ".." element. Re-deriving that postcondition here, independently of the implementation
// (the same approach clip's fuzz targets take), is what would catch a regression that let an
// escaping name through, which would be a path traversal in the paste path — the one bug
// class this surface exists to prevent.
func FuzzExtractPath(f *testing.F) {
	seeds := []string{
		"a/b", "../x", "/x", "a/../b", "a/../../b", "a\x00b", ".", "..", "",
		"a/", "./a", "a//b", "a/b/", "café/x", "//", strings.Repeat("a/", 40) + "x",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		rel, err := safeName(name)
		if err != nil {
			return // only accepted names must satisfy the contract
		}
		if !filepath.IsLocal(rel) {
			t.Fatalf("safeName(%q) = %q, which is not a local path", name, rel)
		}
		if filepath.IsAbs(rel) {
			t.Fatalf("safeName(%q) = %q, which is absolute", name, rel)
		}
		for part := range strings.SplitSeq(rel, string(filepath.Separator)) {
			if part == ".." {
				t.Fatalf("safeName(%q) = %q, which contains a %q element", name, rel, "..")
			}
		}
	})
}
