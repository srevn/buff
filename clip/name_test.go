package clip_test

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/srevn/buff/clip"
)

func TestValidName(t *testing.T) {
	tests := []struct {
		desc  string
		in    string
		valid bool
	}{
		{"empty", "", false},
		{"single char", "a", true},
		{"the default slot", "default", true},
		{"max length 128", strings.Repeat("a", 128), true},
		{"over length 129", strings.Repeat("a", 129), false},
		{"leading dot", ".hidden", false},
		{"leading hyphen", "-flag", false},
		{"trailing dot", "a.", true},
		{"trailing hyphen", "a-", true},
		{"trailing underscore", "a_", true},
		{"interior punctuation", "a.b_c-d", true},
		{"slash", "a/b", false},
		{"colon", "a:b", false},
		{"space", "a b", false},
		{"non-ascii", "café", false},
		{"interior newline", "a\nb", false},
		{"trailing newline", "abc\n", false},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			err := clip.ValidName(tt.in)
			switch {
			case tt.valid && err != nil:
				t.Errorf("ValidName(%q) = %v, want nil", tt.in, err)
			case !tt.valid && !errors.Is(err, clip.ErrNameInvalid):
				t.Errorf("ValidName(%q) = %v, want ErrNameInvalid", tt.in, err)
			}
		})
	}
}

func TestValidFilename(t *testing.T) {
	tests := []struct {
		desc  string
		in    string
		valid bool
	}{
		{"empty", "", false},
		{"dot", ".", false},
		{"dotdot", "..", false},
		{"simple", "a", true},
		{"with extension", "report.pdf", true},
		{"utf-8 two-byte", "café.pdf", true},
		{"utf-8 three-byte", "日本語.pdf", true},
		{"utf-8 four-byte", "🦀.rs", true},
		{"max length 255", strings.Repeat("a", 255), true},
		{"over length 256", strings.Repeat("a", 256), false},
		{"posix separator", "a/b", false},
		{"windows separator", "a\\b", false},
		{"nul byte", "a\x00b", false},
		{"c0 control", "a\x1fb", false},
		{"del byte", "a\x7f", false},
		{"leading dotdot run", "..foo", true},
		{"trailing dotdot run", "foo..", true},
		{"latin-1 high byte", "caf\xe9.txt", false},
		{"lone high byte", "a\x80b", false},
		{"lone continuation", "\x80", false},
		{"truncated lead byte", "\xc3", false},
		{"overlong encoding", "\xc0\xaf", false},
		{"utf-16 surrogate", "\xed\xa0\x80", false},
		{"invalid lead 0xf5", "\xf5", false},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			err := clip.ValidFilename(tt.in)
			switch {
			case tt.valid && err != nil:
				t.Errorf("ValidFilename(%q) = %v, want nil", tt.in, err)
			case !tt.valid && !errors.Is(err, clip.ErrFilenameInvalid):
				t.Errorf("ValidFilename(%q) = %v, want ErrFilenameInvalid", tt.in, err)
			}
		})
	}
}

// FuzzValidName asserts the security invariant rather than equivalence to a reference
// implementation: every name the validator accepts must independently satisfy the namespace rule.
// Re-deriving the rule here, byte by byte and without the regex, is what would catch a regex typo
// (a wrong length bound, a stray metacharacter).
func FuzzValidName(f *testing.F) {
	seeds := []string{
		"", "a", "default", "a.b_c-d", ".hidden", "-flag",
		"a/b", "a:b", "a b", "café", "a\nb", "abc\n",
		strings.Repeat("a", 128), strings.Repeat("a", 129),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if clip.ValidName(s) != nil {
			return // only accepted names must satisfy the contract
		}
		if n := len(s); n < 1 || n > 128 {
			t.Fatalf("accepted name %q of length %d", s, n)
		}
		for i := 0; i < len(s); i++ {
			c := s[i]
			alnum := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
			if i == 0 {
				if !alnum {
					t.Fatalf("accepted name %q with non-alphanumeric first byte %#x", s, c)
				}
				continue
			}
			if !alnum && c != '.' && c != '_' && c != '-' {
				t.Fatalf("accepted name %q with invalid byte %#x at index %d", s, c, i)
			}
		}
	})
}

// FuzzValidFilename asserts the security invariant that makes the validator a safe boundary: any
// filename it accepts must be usable as a single basename written to a consumer's disk. A bug here
// would be a path traversal in the paste path, so the postcondition — re-derived independently of
// the implementation — is the point.
func FuzzValidFilename(f *testing.F) {
	seeds := []string{
		"", ".", "..", "ok.txt", "café.pdf", "日本語.pdf", "🦀.rs", "..foo", "foo..",
		"a/b", "a\\b", "a\x00b", "a\x1fb", "a\x7f",
		"a\x80b", "caf\xe9.txt", "\x80", "\xc3", "\xc0\xaf", "\xed\xa0\x80", "\xf5",
		strings.Repeat("a", 255), strings.Repeat("a", 256),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if clip.ValidFilename(s) != nil {
			return // only accepted filenames must satisfy the contract
		}
		if s == "" || len(s) > 255 || s == "." || s == ".." {
			t.Fatalf("accepted unsafe filename %q", s)
		}
		for i := 0; i < len(s); i++ {
			if c := s[i]; c == '/' || c == '\\' || c < 0x20 || c == 0x7f {
				t.Fatalf("accepted filename %q with unsafe byte %#x at index %d", s, c, i)
			}
		}
		// The fidelity half of the contract, orthogonal to the safety scan above: an accepted name must
		// be valid UTF-8, because the durable meta.json record and the list response serialize it through
		// encoding/json, which silently coerces invalid UTF-8 to U+FFFD with no error. A non-UTF-8 name
		// would not survive that round trip — the silent corruption this gate closes.
		if !utf8.ValidString(s) {
			t.Fatalf("accepted filename %q that is not valid UTF-8", s)
		}
	})
}
