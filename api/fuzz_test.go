package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/srevn/buff/wire"
)

// FuzzFilenameCodec fuzzes the real PUT filename path — parsePut's percent-decode-then-validate of a
// Buff-Filename header — over arbitrary header bytes, and asserts the security postcondition
// independently of the implementation, the same shape clip's and archive's fuzz targets take. This
// is the highest-severity wire surface: a paste path-traversal smuggled through percent-encoding,
// e.g. "..%2F..%2Fetc%2Fpasswd" decoding to "../../etc/passwd". A composite-path fuzz no isolated
// validator covers: FuzzValidFilename proves the post-decode validator, but the decode→validate
// step the api boundary actually runs is the thing that, if miswired, leaks a traversal. By driving
// the production parsePut rather than the validator alone, a wiring regression — decoding a filename
// without running ValidFilename — fails this target, not just a re-proof of the validator.
func FuzzFilenameCodec(f *testing.F) {
	// Seeds: the percent-encoded UTF-8 the codec must preserve, the +-vs-space trap that rules out the
	// query codec, encoded traversal and separators, an encoded NUL, a malformed escape, the special
	// names, and a plain basename. Coverage-expanding inputs the fuzzer finds land under testdata/fuzz.
	seeds := []string{
		"caf%C3%A9.pdf", "a+b.txt", "..%2Fx", "%2e%2e%2fpasswd", "a%2Fb", "a%5Cb",
		"a%00b", "%XY", "%2", "", ".", "..", "report.pdf", "  ", "%2e",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, header string) {
		r := httptest.NewRequest(http.MethodPut, "/v1/clips/x", nil)
		r.Header.Set(wire.HeaderFilename, header)
		m, _, err := parsePut(r)
		if err != nil {
			return // rejected — only an accepted filename must satisfy the contract
		}
		name := m.Filename
		if name == "" {
			return // an absent or empty header carries no filename: nothing is written, nothing to check
		}
		// An accepted, non-empty filename must be a single safe basename: not the traversal names, no
		// path separator on either OS, and no control byte. This is exactly what the store relies on
		// when it later writes the bytes under this name on a consumer's disk.
		if name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
			t.Fatalf("parsePut accepted unsafe basename %q from header %q", name, header)
		}
		for i := 0; i < len(name); i++ {
			if c := name[i]; c < 0x20 || c == 0x7f {
				t.Fatalf("parsePut accepted control byte %#x in %q from header %q", c, name, header)
			}
		}
	})
}
