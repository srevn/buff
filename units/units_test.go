package units_test

import (
	"math"
	"testing"
	"time"

	"github.com/srevn/buff/units"
)

// TestParseDuration pins the grammar: Go's own units carried through unchanged, d and w added on
// top, and the two composing freely in one string. The negative cases are the ones that decide the
// scanner is a scanner and not a substring search — a bare unit with no number, an interior sign,
// a doubled dot — plus the deliberate case-sensitivity, since Go's h/m/s are lowercase-only and a
// vocabulary where 1D worked but 1H did not would be worse than one where neither does.
func TestParseDuration(t *testing.T) {
	ok := []struct {
		in   string
		want time.Duration
	}{
		{"0", 0},
		{"24h", 24 * time.Hour},
		{"1h30m", 90 * time.Minute},
		{"500ms", 500 * time.Millisecond},
		{"-5s", -5 * time.Second}, // signed, exactly as Go's grammar is; the policy lives in the callers
		{"1d", units.Day},
		{"7d", 7 * units.Day},
		{"1w", units.Week},
		{"2w", 2 * units.Week},
		{"30d", 30 * units.Day},
		{"1d12h", 36 * time.Hour}, // a new unit composes with an old one
		{"2w3d4h", 17*units.Day + 4*time.Hour},
		{"1.5d", 36 * time.Hour}, // fractional mantissa, as Go allows for its own units
		{"0.5w", 84 * time.Hour},
		{"-1d", -units.Day},
		{"1w1ns", units.Week + time.Nanosecond}, // the ladder's ends in one string
		{"  24h  ", 24 * time.Hour},             // trimmed, as the server's config grammar always has been
	}
	for _, c := range ok {
		if got, err := units.ParseDuration(c.in); err != nil || got != c.want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v, nil", c.in, got, err, c.want)
		}
	}
	bad := []string{"", "5", "abc", "d", "w", "1x", "1d2x", ".d", "1..d", "1d-2h", "1D", "1W", "1dd"}
	for _, in := range bad {
		if got, err := units.ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) = %v, nil; want error", in, got)
		}
	}
}

// TestDuration pins the rendering ladder: the largest whole unit that fits, truncated to it, with a
// sub-second sliver floored to 0s. The boundary rows are the contract — one nanosecond under a unit
// must render in the unit below, never round up into a span the clip does not have.
func TestDuration(t *testing.T) {
	for _, c := range []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{time.Second, "1s"},
		{90 * time.Second, "1m"}, // truncates: never 2m
		{59 * time.Minute, "59m"},
		{time.Hour, "1h"},
		{units.Day - time.Nanosecond, "23h"},
		{units.Day, "1d"},
		{397 * time.Hour, "2w"},
		{units.Week - time.Nanosecond, "6d"},
		{units.Week, "1w"},
		{3 * units.Week, "3w"},
	} {
		if got := units.Duration(c.in); got != c.want {
			t.Errorf("Duration(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestParseSize pins the byte grammar, including the two extensions that let it read Size's output:
// a fractional mantissa (1.5G) and the P/E units the renderer's ladder can reach. A fraction with
// no unit stays an error — it is a typo, not a size — as does anything overflowing int64.
func TestParseSize(t *testing.T) {
	ok := []struct {
		in   string
		want int64
	}{
		{"0", 0}, {"1024", 1024}, {"1K", 1 << 10}, {"1k", 1 << 10}, {"1Ki", 1 << 10},
		{"1KiB", 1 << 10}, {"1KB", 1 << 10}, {"2M", 2 << 20}, {"3G", 3 << 30}, {"1T", 1 << 40},
		{"1g", 1 << 30}, {"1Gi", 1 << 30}, {"  5K  ", 5 << 10}, {"1P", 1 << 50}, {"1E", 1 << 60},
		{"1.5G", 3 << 29}, {"1.0KiB", 1 << 10}, {"0.5M", 1 << 19}, {"2.5GiB", 5 << 29},
	}
	for _, c := range ok {
		if got, err := units.ParseSize(c.in); err != nil || got != c.want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
	}
	bad := []string{"", "-1", "-1K", "-1.5G", "abc", "K", "1X", "1.5", "1..5G", "99999999999999T", "9E"}
	for _, in := range bad {
		if got, err := units.ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) = %d, nil; want error", in, got)
		}
	}
}

// TestSize pins the byte rendering: exact below a kibibyte, one decimal of the largest binary unit
// above it.
func TestSize(t *testing.T) {
	for _, c := range []struct {
		in   int64
		want string
	}{
		{0, "0B"}, {46, "46B"}, {1023, "1023B"}, {1024, "1.0KiB"}, {1536, "1.5KiB"},
		{688, "688B"}, {1 << 20, "1.0MiB"}, {3 << 30, "3.0GiB"}, {1 << 40, "1.0TiB"},
	} {
		if got := units.Size(c.in); got != c.want {
			t.Errorf("Size(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestRenderedValuesReparse is the reason the two directions share a package: every string a
// renderer emits must be one the matching parser accepts. Nothing enforced that while the halves
// sat in different packages, and they drifted — the listing printed a size the config parser called
// malformed. This is the regression test for that class of drift, walked over the full ladder of
// both vocabularies, and it is what a reader can point at to know the round-trip is real.
func TestRenderedValuesReparse(t *testing.T) {
	durations := []time.Duration{
		0, time.Second, 90 * time.Second, 59 * time.Minute, time.Hour, 23 * time.Hour,
		units.Day, 6 * units.Day, units.Week, 397 * time.Hour, 52 * units.Week,
	}
	for _, d := range durations {
		s := units.Duration(d)
		got, err := units.ParseDuration(s)
		if err != nil {
			t.Errorf("Duration(%v) = %q, which ParseDuration rejects: %v", d, s, err)
			continue
		}
		// Truncating, never inflating, is the renderer's contract: what it prints must never claim more
		// time than the span actually holds, or a reader acts on time they do not have.
		if got > d {
			t.Errorf("Duration(%v) = %q, which reparses to %v — more time than there is", d, s, got)
		}
	}

	sizes := []int64{0, 46, 688, 1023, 1024, 1536, 1 << 20, 3 << 30, 1 << 40, 1 << 50, 1 << 60}
	for _, n := range sizes {
		s := units.Size(n)
		if _, err := units.ParseSize(s); err != nil {
			t.Errorf("Size(%d) = %q, which ParseSize rejects: %v", n, s, err)
		}
	}
}

// TestSizeCeiling documents the one place the round-trip cannot hold, so that a reader meets
// it here rather than in a fuzz failure. A byte count within a hair of int64's ceiling renders
// as 8.0EiB — float64 cannot even represent MaxInt64, and rounds it up — and 8 exbibytes is by
// definition more than int64 holds, so the parser must reject it. The renderer's top unit and the
// type's ceiling collide there. It is unreachable in practice: no store holds eight exbibytes, and
// the caps that use this grammar are gibibytes.
func TestSizeCeiling(t *testing.T) {
	const s = "8.0EiB"
	if got := units.Size(math.MaxInt64); got != s {
		t.Fatalf("Size(MaxInt64) = %q, want %q", got, s)
	}
	if got, err := units.ParseSize(s); err == nil {
		t.Errorf("ParseSize(%q) = %d, nil; want an out-of-range error", s, got)
	}
}
