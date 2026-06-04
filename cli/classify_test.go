package cli

import (
	"bytes"
	"testing"
	"unicode/utf8"
)

// TestIsText pins the two-signal classifier across the spectrum a terminal paste meets: readable
// text (including ANSI-coloured logs and a UTF-8 rune the peek window split mid-encoding) reads as
// text and shows; NUL-bearing or non-UTF-8 payloads (binary formats, UTF-16, mid-content Latin-1)
// read as binary and save. The cut-tail cases are the load-bearing ones — they are why a long CJK
// clip whose window boundary lands inside a rune is still shown rather than mistaken for binary.
func TestIsText(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want bool
	}{
		{"ascii", []byte("hello, world"), true},
		{"ansi colour escape", []byte("\x1b[31mred\x1b[0m"), true},
		{"tab newline carriage-return", []byte("a\tb\nc\r\n"), true},
		{"utf-8 multibyte", []byte("price: €5 — ¥9"), true},
		{"utf-8 rune cut to its lead byte", []byte("log line ends in €"[:len("log line ends in €")-2]), true},
		{"utf-8 rune cut to lead plus one", []byte("log line ends in €"[:len("log line ends in €")-1]), true},
		{"empty", []byte{}, true},
		{"nil", nil, true},

		{"nul byte", []byte("ab\x00cd"), false},
		{"latin-1 high byte mid content", []byte("r\xe9sum\xe9!"), false},
		{"png signature", []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}, false},
		{"utf-16le with bom", []byte{0xff, 0xfe, 'h', 0x00, 'i', 0x00}, false},
		{"jpeg start of image", []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10}, false},
		{"complete but invalid trailing byte", []byte("hello\xff"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isText(tc.in); got != tc.want {
				t.Errorf("isText(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// FuzzIsText fuzzes the classifier like the other parsers in the codebase. It pins the two hard
// guarantees the disposition rests on — a NUL is always binary, and valid UTF-8 free of NUL is
// always text — and that no input panics. Everything between those poles (a cut tail, a malformed
// middle) is the classifier's judgement, exercised by the table above rather than asserted here.
func FuzzIsText(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("hello"), {0x00}, []byte("€"), {0xff, 0xd8}, {}, []byte("a\tb\n"), {0xe2, 0x82},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, p []byte) {
		got := isText(p) // the contract is that this never panics for any input
		hasNUL := bytes.IndexByte(p, 0) >= 0
		if hasNUL && got {
			t.Errorf("isText(%q) = true, want false: a NUL byte is always binary", p)
		}
		if utf8.Valid(p) && !hasNUL && !got {
			t.Errorf("isText(%q) = false, want true: valid UTF-8 with no NUL is text", p)
		}
	})
}
