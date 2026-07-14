package units_test

import (
	"math"
	"testing"
	"time"

	"github.com/srevn/buff/units"
)

// FuzzParseDuration pins the property the whole vocabulary rests on: every string Go's own
// parser accepts must parse here to the identical value. It cannot be checked by example, because
// the inputs it must hold for are *every* string time.ParseDuration accepts — and if it ever
// breaks, a --ttl or a BUFF_TTL that has worked for years silently changes meaning, which is the
// worst failure this package could have. The scanner must also survive any input at all without
// panicking, since it reads a byte at a time from a string an operator typed.
//
// Only the one direction is asserted. A string Go rejects may well be accepted here — 7d is the
// point of the exercise — so a rejection upstream tells us nothing and the case simply ends.
func FuzzParseDuration(f *testing.F) {
	seeds := []string{
		"0", "1h", "24h", "1h30m", "500ms", "-5s", "+2h", "1.5h", "100ns", "1µs", "1us", "1m",
		"7d", "2w", "1d12h", "2w3d4h5m", "1.5d", "0.5w", "-1d", "+1w", "1w1ns", "  24h  ",
		"", "d", "w", "1x", "1d2x", ".d", "1..d", "abcd", "1d-2h", "1D", "1W", "1dw", "..", "-",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, err := units.ParseDuration(s) // must not panic, whatever the bytes
		want, stdErr := time.ParseDuration(s)
		if stdErr != nil {
			return
		}
		if err != nil || got != want {
			t.Fatalf("ParseDuration(%q) = %v, %v; time.ParseDuration gives %v — the superset broke", s, got, err, want)
		}
	})
}

// FuzzDurationReparses pins the round-trip in the direction a person actually travels it: read a
// span off a listing, type it back at --ttl. Every string the renderer emits must parse, and must
// parse to no more time than the span it came from — the truncation contract, which is what stops a
// clip with 59m left from reading as an hour.
func FuzzDurationReparses(f *testing.F) {
	for _, ns := range []int64{0, 1, int64(time.Second), int64(90 * time.Second), int64(time.Hour),
		int64(units.Day), int64(units.Week), int64(397 * time.Hour), math.MaxInt64} {
		f.Add(ns)
	}
	f.Fuzz(func(t *testing.T, ns int64) {
		if ns < 0 {
			return // the renderer's domain is a non-negative span; a negative one has no rendering
		}
		d := time.Duration(ns)
		s := units.Duration(d)
		got, err := units.ParseDuration(s)
		if err != nil {
			t.Fatalf("Duration(%v) = %q, which ParseDuration rejects: %v", d, s, err)
		}
		if got > d {
			t.Fatalf("Duration(%v) = %q, which reparses to %v — more time than the span holds", d, s, got)
		}
	})
}

// FuzzParseSize pins the byte grammar's robustness across the float path the fractional mantissa
// added: any input at all, no panic, and never a negative or a garbage cap smuggled out through
// an out-of-range float64-to-int64 conversion — which Go leaves implementation-defined and which a
// silent wrong answer here would turn into a store that ignores its own limit.
func FuzzParseSize(f *testing.F) {
	seeds := []string{
		"0", "1024", "1K", "1GiB", "1.5G", "1.0KiB", "8.0EiB", "9E", "1E", "1P",
		"", "-1", "-1.5G", "abc", "K", "1X", "1.5", "1..5G", "1e400", "99999999999999T", "  5K  ",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		n, err := units.ParseSize(s) // must not panic, whatever the bytes
		if err == nil && n < 0 {
			t.Fatalf("ParseSize(%q) = %d, nil — a negative cap is not a size", s, n)
		}
	})
}

// FuzzSizeReparses is the size half of the round-trip: a size read off a listing must be one a cap
// accepts. The bound is the collision TestSizeCeiling documents — a byte count near int64's ceiling
// renders as 8.0EiB, which is more than int64 holds and which the parser must therefore reject — so
// the property is asserted over the range a store can actually occupy, which half of int64's range
// covers with room to spare over any real disk.
func FuzzSizeReparses(f *testing.F) {
	for _, n := range []int64{0, 46, 688, 1023, 1024, 1536, 1 << 20, 3 << 30, 1 << 40, 1 << 50} {
		f.Add(n)
	}
	f.Fuzz(func(t *testing.T, n int64) {
		if n < 0 || n > math.MaxInt64/2 {
			return
		}
		s := units.Size(n)
		if _, err := units.ParseSize(s); err != nil {
			t.Fatalf("Size(%d) = %q, which ParseSize rejects: %v", n, s, err)
		}
	})
}
