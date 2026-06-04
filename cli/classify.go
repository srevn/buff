package cli

import (
	"bytes"
	"unicode/utf8"
)

// peekWindow is how many leading bytes a terminal paste inspects to tell a text clip it can show
// from a binary one it should save. git reads about the same to make the same call: a text-looking
// header over a binary body can fool any bounded window, but here the bytes are arriving regardless,
// so widening it buys nothing — a larger window only postpones the inevitable misread of a prefix.
const peekWindow = 8192

// isText reports whether a clip's leading bytes read as human-text safe to show on a terminal, as
// opposed to binary better saved to a file. Two signals, with nothing left to tune: a NUL byte, or
// invalid UTF-8 (forgiving a single trailing rune the window cut), is binary; anything else is
// text. ANSI-coloured logs pass for free — ESC is valid UTF-8 and carries no NUL. The one standing
// trade is non-UTF-8 text (UTF-16, Latin-1), which a UTF-8 test cannot distinguish from binary and
// so saves rather than shows; it is recovered losslessly with -o - or by reading the saved file.
//
// It lives in cli, not clip: clip is the vocabulary both the server and the client share, and a
// content classifier there would stand as a permanent affordance to cross the line the server
// holds — it relays opaque bytes and never interprets them. cli is invisible to the server, so
// keeping the sole classifier here makes "the server never classifies content" a structural fact
// rather than a convention one edit could quietly undo.
func isText(prefix []byte) bool {
	if bytes.IndexByte(prefix, 0) >= 0 {
		// utf8.Valid accepts NUL, so this is not subsumed by the UTF-8 check below — it is what
		// catches a UTF-16 document or any NUL-bearing payload that is otherwise valid UTF-8.
		return false
	}
	return validUTF8AllowingCutTail(prefix)
}

// validUTF8AllowingCutTail is utf8.Valid loosened to forgive one trailing rune the peek window
// split mid-encoding, so a long UTF-8 clip whose window boundary lands inside a multi-byte rune
// (common for CJK text) still reads as text instead of being mistaken for binary. Only a genuine
// cut at the very end is forgiven: the head must be wholly valid and the final fragment must be an
// incomplete rune, not a complete-but-invalid one. Invalidity anywhere but that cut tail is binary.
func validUTF8AllowingCutTail(p []byte) bool {
	if utf8.Valid(p) {
		return true
	}
	// A cut rune's first byte lies within the last few bytes: a 4-byte rune split after its third
	// byte starts three back, the deepest a cut can reach. Walk back to the last rune start in that
	// window and ask whether everything before it is valid and the fragment from it is merely
	// incomplete — a complete-but-invalid fragment, or no rune start at all, stays binary.
	for i := len(p) - 1; i >= 0 && i >= len(p)-3; i-- {
		if utf8.RuneStart(p[i]) {
			return utf8.Valid(p[:i]) && !utf8.FullRune(p[i:])
		}
	}
	return false
}
