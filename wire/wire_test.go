package wire_test

import (
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

// TestHeaderNames pins the literal header spellings and proves they are pairwise distinct. A typo
// in one of these is a silent interop break — the server and client would simply fail to find each
// other's header — so the contract is asserted, not trusted.
func TestHeaderNames(t *testing.T) {
	headers := []struct {
		got  string
		want string
	}{
		{wire.HeaderKind, "Buff-Kind"},
		{wire.HeaderFilename, "Buff-Filename"},
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
		{wire.HeaderForce, "Buff-Force"},
	}
	seen := make(map[string]bool)
	for _, h := range headers {
		if h.got != h.want {
			t.Errorf("header = %q, want %q", h.got, h.want)
		}
		if seen[h.got] {
			t.Errorf("duplicate header name %q", h.got)
		}
		seen[h.got] = true
	}
	if len(seen) != 13 {
		t.Errorf("got %d distinct header names, want 13", len(seen))
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
