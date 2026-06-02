package clip

import "regexp"

// nameRe is the flat slot namespace: one leading alphanumeric followed by up to 127
// more characters drawn from alphanumerics, dot, underscore, and hyphen — 1 to 128
// ASCII characters, no slashes or colons. RE2 matching is linear in the input length
// with no backtracking, so it is safe to run on adversarial input. In Go, $ anchors
// to the end of the text rather than the end of a line, so a trailing newline cannot
// slip past it.
var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidName reports whether name is a usable slot name, returning ErrNameInvalid if
// not. The name never becomes an on-disk path component — a generation's directory is
// named by its id alone — so this rule can be widened later, to Unicode or hierarchical
// names, with no change to the storage layout.
func ValidName(name string) error {
	if nameRe.MatchString(name) {
		return nil
	}
	return ErrNameInvalid
}

// ValidFilename reports whether name is safe to store and later restore as a single
// basename on a consumer's disk, returning ErrFilenameInvalid if not. The caller
// passes an already-percent-decoded string; decoding happens at the wire boundary,
// never here, and this never shares a code path with ValidName.
//
// It rejects rather than sanitises: a value that is not already a safe basename is an
// error, never silently reduced to one. Quietly rewriting "../../etc/passwd" to
// "passwd" would mask a sender's mistake or attack instead of surfacing it.
//
// Rejected are the empty string; anything longer than 255 bytes; the special names
// "." and ".."; and any byte that is a path separator ('/' or '\\', barring traversal
// on both POSIX and Windows), a C0 control (< 0x20, which includes the NUL that
// truncates C strings and paths), or DEL (0x7f). The scan is over bytes, not runes,
// so multi-byte UTF-8 (any byte >= 0x80) passes through untouched and a filename like
// "café.pdf" is accepted.
func ValidFilename(name string) error {
	if name == "" || len(name) > 255 || name == "." || name == ".." {
		return ErrFilenameInvalid
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; c == '/' || c == '\\' || c < 0x20 || c == 0x7f {
			return ErrFilenameInvalid
		}
	}
	return nil
}
