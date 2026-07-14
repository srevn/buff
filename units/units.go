// Package units is the vocabulary a person uses to say "how long" and "how big" to buff, in both
// directions: the parsers a typed value enters through and the renderers a listed value leaves by.
//
// The two directions live in one package because they are one contract — everything a renderer
// emits, the matching parser accepts — and that contract cannot hold if the halves sit in different
// packages, free to drift. They did drift: the size renderer lived in the client's listing and the
// size parser lived in the server's config, so the listing printed 1.0KiB and the config parser
// rejected it as malformed. Nothing detected that, because no test could see both halves at once.
// Keeping them adjacent is what makes the round-trip checkable, and units_test.go checks it.
//
// This vocabulary is deliberately not the wire's. A span on the wire is a Go duration string and
// an instant is RFC3339, because both ends of the wire are code and an exact machine round-trip is
// worth more there than legibility. These are the units a person types and reads, so they carry the
// units a person thinks in — days and weeks, gibibytes — and no protocol depends on them.
package units

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Day and Week extend Go's duration units with the two spans retention is actually written in.
// Both are fixed multiples of an hour, never calendar steps: a TTL is a span added to a finalize
// instant, not date arithmetic, so a "day" that shortened to 23h across a DST boundary would
// silently expire a clip an hour early. Fixed is the whole point — do not make these calendar-
// aware.
const (
	Day  = 24 * time.Hour
	Week = 7 * Day
)

// maxInt64f is int64's ceiling as a float64. math.MaxInt64 is not exactly representable in a
// float64 — it rounds *up*, to 2^63 — so a scaled size must be strictly below this value to convert
// without overflowing, and the guard below is >= rather than >. Getting that backwards converts an
// out-of-range float to int64, which Go leaves implementation-defined, and lands a garbage cap.
const maxInt64f = float64(math.MaxInt64)

// ParseDuration reads a span in the units a person writes retention in. It is an exact superset of
// time.ParseDuration — every string the standard parser accepts parses here to the identical value
// — extended with d (a fixed 24h) and w (a fixed 168h), which Go's duration grammar has never had
// and whose absence is why a one-week TTL otherwise has to be typed as 168h.
//
// A day or week term is rewritten into its equivalent in hours and the whole rewritten string is
// handed to the standard parser, so overflow, fractional mantissas, and every unit below an hour
// stay exactly as Go defines them instead of being reimplemented here. The arithmetic this function
// does not do is arithmetic it cannot get wrong.
//
// The sign is left to the standard parser too, so a negative span parses just as it does in Go.
// Rejecting one is a policy each caller owns and states in its own terms — a negative --ttl is
// a client usage error, a negative BUFF_TTL is a server boot error — and is not a fact about the
// grammar, which is signed because time.Duration is.
func ParseDuration(s string) (time.Duration, error) {
	t := strings.TrimSpace(s)
	// No new unit means nothing to rewrite, and handing the string straight to the standard parser
	// is what makes the superset property true by construction rather than by test. Go's units are
	// lowercase and none contains a d or a w, so this cannot misfire on ns/us/ms/s/m/h.
	if !strings.ContainsAny(t, "dw") {
		return time.ParseDuration(t)
	}

	var rewritten strings.Builder
	rest := t
	if rest != "" && (rest[0] == '-' || rest[0] == '+') {
		rewritten.WriteByte(rest[0])
		rest = rest[1:]
	}
	for rest != "" {
		i := 0
		for i < len(rest) && isMantissaByte(rest[i]) {
			i++
		}
		mantissa := rest[:i]
		rest = rest[i:]

		j := 0
		for j < len(rest) && !isMantissaByte(rest[j]) {
			j++
		}
		unit := rest[:j]
		rest = rest[j:]

		var hours float64
		switch unit {
		case "d":
			hours = float64(Day / time.Hour)
		case "w":
			hours = float64(Week / time.Hour)
		default:
			// Not one of ours: copy the term through untouched and let the standard parser accept or reject
			// it, so an unknown unit yields Go's own error rather than a second dialect of it.
			rewritten.WriteString(mantissa)
			rewritten.WriteString(unit)
			continue
		}
		f, err := strconv.ParseFloat(mantissa, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		// 'f' with -1 precision is the shortest form that round-trips exactly, so the rewrite loses
		// nothing; an absurd mantissa scales to +Inf here and the standard parser rejects the result.
		rewritten.WriteString(strconv.FormatFloat(f*hours, 'f', -1, 64))
		rewritten.WriteString("h")
	}
	return time.ParseDuration(rewritten.String())
}

// isMantissaByte reports whether b can appear in a duration term's number — the exact complement of
// the bytes that can appear in its unit. That complement is what advances the scanner: a term is a
// run of one kind followed by a run of the other, so each iteration consumes at least one byte and
// the loop cannot spin on an input it does not understand.
func isMantissaByte(b byte) bool { return (b >= '0' && b <= '9') || b == '.' }

// Duration renders a non-negative span in the largest whole unit that fits, weeks down to seconds,
// with a flat 0s below a second. It truncates rather than rounds, which is what keeps a remaining
// span honest: a clip with 59m left reads "in 59m", never the "in 1h" that rounding would inflate
// it to and that a reader would act on as more time than they have.
//
// Every unit it emits is one ParseDuration accepts, so a span read off a listing can be typed
// straight back at --ttl. Holding that is why the vocabulary is this package's and not Go's, whose
// duration grammar stops at hours and would render a fortnight-old clip in three-digit hours.
func Duration(d time.Duration) string {
	switch {
	case d >= Week:
		return fmt.Sprintf("%dw", int64(d/Week))
	case d >= Day:
		return fmt.Sprintf("%dd", int64(d/Day))
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	case d >= time.Second:
		return fmt.Sprintf("%ds", int64(d/time.Second))
	default:
		return "0s"
	}
}

// ParseSize reads a byte count with an optional binary unit: a bare integer is bytes, and a K, M,
// G, T, P, or E suffix multiplies by the matching power of 1024. The suffix is case-insensitive and
// tolerates a trailing i and/or B, so 1G, 1Gi, 1GB, and 1GiB all mean one gibibyte — every unit is
// binary, the only interpretation a byte store needs, with no decimal-versus-binary ambiguity to
// surprise an operator.
//
// A fractional mantissa is accepted, but only with a unit. That is what closes the gap to Size,
// which renders to one decimal: 1.5GiB is a string a listing actually prints, so a parser rejecting
// it would be a cap that cannot read its own output. A fraction of a bare byte is meaningless and
// stays an error.
//
// Zero means unlimited, matching the store's cap semantics. A negative value is rejected here
// rather than left to the caller — unlike a duration, whose sign is real, a negative byte count is
// not a value any caller could want — as is a malformed one, or one that overflows int64.
func ParseSize(s string) (int64, error) {
	u := strings.TrimSpace(s)
	if u == "" {
		return 0, errors.New("invalid size")
	}
	// Shed an optional trailing "B"/"iB" so the unit letter is last, then read the multiplier off that
	// letter. The order matters: B before i, so "iB" sheds fully.
	u = strings.TrimSuffix(u, "B")
	u = strings.TrimSuffix(u, "b")
	u = strings.TrimSuffix(u, "i")
	u = strings.TrimSuffix(u, "I")
	mult := int64(1)
	if n := len(u); n > 0 {
		switch u[n-1] {
		case 'K', 'k':
			mult = 1 << 10
		case 'M', 'm':
			mult = 1 << 20
		case 'G', 'g':
			mult = 1 << 30
		case 'T', 't':
			mult = 1 << 40
		case 'P', 'p':
			mult = 1 << 50
		case 'E', 'e':
			mult = 1 << 60
		}
		if mult != 1 {
			u = u[:n-1]
		}
	}
	u = strings.TrimSpace(u)

	if strings.Contains(u, ".") {
		return fractionalSize(u, mult)
	}
	v, err := strconv.ParseInt(u, 10, 64)
	if err != nil {
		return 0, errors.New("invalid size")
	}
	if v < 0 {
		return 0, errors.New("size must not be negative")
	}
	if mult != 1 && v > math.MaxInt64/mult {
		return 0, errors.New("size out of range")
	}
	return v * mult, nil
}

// fractionalSize scales a decimal mantissa by a binary unit. It is the one path that leaves integer
// arithmetic, so it carries the overflow check float64 makes easy to get wrong: the product is
// compared against int64's ceiling *before* the conversion, because converting an out-of-range
// float64 to int64 is implementation-defined in Go and would otherwise store a garbage cap in
// silence.
func fractionalSize(mantissa string, mult int64) (int64, error) {
	// A fraction is only meaningful as a fraction of something. Without a unit there is nothing to
	// scale, and "1.5" bytes is a typo, not a size.
	if mult == 1 {
		return 0, errors.New("invalid size")
	}
	f, err := strconv.ParseFloat(mantissa, 64)
	if err != nil {
		return 0, errors.New("invalid size") // also catches an overflowing mantissa, which yields ±Inf
	}
	if f < 0 {
		return 0, errors.New("size must not be negative")
	}
	if scaled := f * float64(mult); scaled < maxInt64f {
		return int64(scaled), nil
	}
	return 0, errors.New("size out of range")
}

// Size renders a byte count in binary units, so a listing reads at a glance rather than in raw
// bytes. Sub-kibibyte sizes stay exact in bytes; larger ones round to one decimal of the largest
// unit that fits — a precision ParseSize accepts back, which is what lets a size read off a listing
// be pasted into a cap.
func Size(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
