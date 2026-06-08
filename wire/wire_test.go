package wire_test

import (
	"strconv"
	"testing"

	"github.com/srevn/buff/wire"
)

// TestErrInfoTable pins each row's exact sentinel and status and proves the rows are internally
// consistent (distinct sentinels, plausible statuses). It is a deliberate change-detector: both
// sides derive their maps from this table, so any edit to a value is a wire change that must be
// made on purpose, never slipped in. The pin table is tied to wire.Rows below — same length, every
// enumerated row pinned — so adding or removing a row also forces a deliberate update here, which a
// count over the test's own list could not catch.
func TestErrInfoTable(t *testing.T) {
	rows := []struct {
		got          wire.ErrInfo
		wantSentinel string
		wantStatus   int
	}{
		{wire.ErrNotFound, "not_found", 404},
		{wire.ErrConsumed, "consumed", 410},
		{wire.ErrBusy, "busy", 409},
		{wire.ErrClosed, "closed", 409},
		{wire.ErrPrecondition, "precondition_failed", 412},
		{wire.ErrTooLarge, "too_large", 413},
		{wire.ErrNoSpace, "no_space", 507},
		{wire.ErrNameBad, "name_invalid", 400},
		{wire.ErrBadReq, "bad_request", 400},
		{wire.ErrInternal, "internal", 500},
		{wire.ErrUnavailable, "unavailable", 503},
	}
	seen := make(map[string]bool)
	for _, r := range rows {
		if r.got.Sentinel != r.wantSentinel || r.got.Status != r.wantStatus {
			t.Errorf("row = %+v, want {Sentinel:%q Status:%d}", r.got, r.wantSentinel, r.wantStatus)
		}
		if r.got.Sentinel == "" {
			t.Errorf("row %+v has an empty sentinel", r.got)
		}
		if r.got.Status < 100 || r.got.Status >= 600 {
			t.Errorf("row %+v has an implausible HTTP status", r.got)
		}
		if seen[r.got.Sentinel] {
			t.Errorf("duplicate sentinel %q", r.got.Sentinel)
		}
		seen[r.got.Sentinel] = true
	}
	// Tie the pin table to wire.Rows: equal length, and every enumerated row pinned above. An added or
	// removed row breaks this until its value is pinned on purpose — the deliberate-change property a
	// count over this test's own list could never provide. Distinctness above runs over the pin table,
	// but since it must equal wire.Rows, it guards the canonical set too.
	if len(rows) != len(wire.Rows) {
		t.Fatalf("pinned %d rows, but wire.Rows enumerates %d", len(rows), len(wire.Rows))
	}
	pinned := make(map[wire.ErrInfo]bool, len(rows))
	for _, r := range rows {
		pinned[r.got] = true
	}
	for _, row := range wire.Rows {
		if !pinned[row] {
			t.Errorf("wire.Rows enumerates %+v, which is not pinned in this table", row)
		}
	}
}

// TestFeatures pins each capability string and proves the advertised set is internally consistent
// (distinct, non-empty), the feature analogue of TestErrInfoTable. Both sides read these strings
// off the wire, so any edit to a value is a protocol change to make on purpose, never slip in. The
// pin table is tied to wire.Features by length and membership, so adding or removing a feature also
// forces a deliberate update here.
func TestFeatures(t *testing.T) {
	pins := []struct {
		got  string
		want string
	}{
		{wire.FeatureFollow, "follow"},
		{wire.FeatureConsumeOnce, "consume-once"},
		{wire.FeatureWait, "wait"},
		{wire.FeatureConditionalWrite, "conditional-write"},
		{wire.FeatureFollowNext, "follow-next"},
	}
	seen := make(map[string]bool)
	for _, p := range pins {
		if p.got != p.want {
			t.Errorf("feature = %q, want %q", p.got, p.want)
		}
		if p.got == "" {
			t.Error("a feature string is empty")
		}
		if seen[p.got] {
			t.Errorf("duplicate feature %q", p.got)
		}
		seen[p.got] = true
	}
	// Tie the pin table to wire.Features: equal length, every advertised feature pinned above — the
	// deliberate-change property a count over this test's own list could not give.
	if len(pins) != len(wire.Features) {
		t.Fatalf("pinned %d features, but wire.Features advertises %d", len(pins), len(wire.Features))
	}
	for _, f := range wire.Features {
		if !seen[f] {
			t.Errorf("wire.Features advertises %q, which is not pinned in this table", f)
		}
	}
}

// TestHeaderNames pins each literal header spelling and proves they are pairwise distinct. A typo
// in one of these is a silent interop break — the server and client would simply fail to find each
// other's header — so the contract is asserted, not trusted. The pin table is tied to wire.Headers
// by length and membership, the way TestErrInfoTable ties to Rows and TestFeatures to Features, so
// adding or removing a header forces a deliberate update here; TestHeadersEnumeratesDeclaredConsts
// then proves Headers itself omits no declared header — together closing the count over a hand-kept
// list that once silently dropped Buff-Executable.
func TestHeaderNames(t *testing.T) {
	pins := []struct {
		got  string
		want string
	}{
		{wire.HeaderKind, "Buff-Kind"},
		{wire.HeaderFilename, "Buff-Filename"},
		{wire.HeaderExecutable, "Buff-Executable"},
		{wire.HeaderTTL, "Buff-TTL"},
		{wire.HeaderKeep, "Buff-Keep"},
		{wire.HeaderConsume, "Buff-Consume"},
		{wire.HeaderGeneration, "Buff-Generation"},
		{wire.HeaderFinalized, "Buff-Finalized"},
		{wire.HeaderSize, "Buff-Size"},
		{wire.HeaderExpires, "Buff-Expires"},
		{wire.HeaderStatus, "Buff-Status"},
		{wire.HeaderError, "Buff-Error"},
		{wire.HeaderIfMatch, "If-Match"},
		{wire.HeaderWait, "Buff-Wait"},
		{wire.HeaderFollowNext, "Buff-Follow-Next"},
	}
	seen := make(map[string]bool)
	for _, h := range pins {
		if h.got != h.want {
			t.Errorf("header = %q, want %q", h.got, h.want)
		}
		if h.got == "" {
			t.Error("a header name is empty")
		}
		if seen[h.got] {
			t.Errorf("duplicate header name %q", h.got)
		}
		seen[h.got] = true
	}
	// Tie the pin table to wire.Headers: equal length, every enumerated header pinned above — the
	// deliberate-change property the old fixed count over this test's own list could not give.
	if len(pins) != len(wire.Headers) {
		t.Fatalf("pinned %d headers, but wire.Headers enumerates %d", len(pins), len(wire.Headers))
	}
	for _, h := range wire.Headers {
		if !seen[h] {
			t.Errorf("wire.Headers enumerates %q, which is not pinned in this table", h)
		}
	}
}

// TestRoutesAndStatus pins the route prefixes and the completion sentinel, and proves the clip
// route is built from the version prefix so the two cannot drift apart.
func TestRoutesAndStatus(t *testing.T) {
	if wire.V1 != "/v1" {
		t.Errorf("V1 = %q, want %q", wire.V1, "/v1")
	}
	if wire.PathClips != "/v1/clips" {
		t.Errorf("PathClips = %q, want %q", wire.PathClips, "/v1/clips")
	}
	if wire.PathHealth != "/health" {
		t.Errorf("PathHealth = %q, want %q", wire.PathHealth, "/health")
	}
	if wire.StatusComplete != "complete" {
		t.Errorf("StatusComplete = %q, want %q", wire.StatusComplete, "complete")
	}
}

// TestBoolEncoding pins the request/response boolean encode-split — the one value-coupling in a
// package built on symbol-coupling. Both sides reference FlagOn and BoolTrue by symbol, so a value
// edit to either passes every round-trip test that names the symbol — the exact drift wire exists to
// prevent. Pinning the literals here makes such an edit a deliberate wire change, the service
// TestRoutesAndStatus gives StatusComplete. The third assertion guards the coupling the header
// comments carry only as prose: the always-present response booleans (Buff-Finalized, Buff-Consume)
// are emitted with strconv.FormatBool and decoded against BoolTrue, so BoolTrue must equal
// FormatBool(true) or that decode silently fails for every finalized read.
func TestBoolEncoding(t *testing.T) {
	if wire.FlagOn != "1" {
		t.Errorf("FlagOn = %q, want %q", wire.FlagOn, "1")
	}
	if wire.BoolTrue != "true" {
		t.Errorf("BoolTrue = %q, want %q", wire.BoolTrue, "true")
	}
	if wire.BoolTrue != strconv.FormatBool(true) {
		t.Errorf("BoolTrue = %q, want FormatBool(true) %q — response booleans decode against it", wire.BoolTrue, strconv.FormatBool(true))
	}
}
