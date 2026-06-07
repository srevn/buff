package client_test

import (
	"context"
	"slices"
	"testing"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/store"
)

// TestHealth probes /health through the client and checks the operational report it decodes:
// liveness, the advertised api versions, and the capability list a caller consults before relying
// on an optional feature. Unlike the content methods there is no domain type behind it, so this
// guards the one place the client decodes /health directly.
func TestHealth(t *testing.T) {
	_, c := memClient(t, store.Config{})
	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if h.Status != "ok" {
		t.Errorf("status = %q, want ok", h.Status)
	}
	if h.Version == "" {
		t.Error("Health reports an empty version")
	}
	if !slices.Contains(h.API, "v1") {
		t.Errorf("api = %v, want it to advertise v1", h.API)
	}
	if len(h.Features) == 0 {
		t.Error("Health reports no features")
	}
}

// TestHealthConditionalWrite pins the typed capability predicate the cli gates a conditional write
// on. A current server advertises conditional-write, so the predicate is true; a peer that does
// not list it reads false — the fail-safe that makes the cli refuse a conditional write rather than
// risk a silent unconditional replace against an older server. The predicate is how the cli asks a
// domain question without naming the wire feature string it may not import.
func TestHealthConditionalWrite(t *testing.T) {
	_, c := memClient(t, store.Config{})
	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !h.ConditionalWrite() {
		t.Errorf("a current server does not report conditional-write: features = %v", h.Features)
	}
	if (client.Health{}).ConditionalWrite() {
		t.Error("ConditionalWrite() is true for a server advertising no features")
	}
}

// TestHealthFollowNext pins the typed capability predicate the cli gates a follow-next read on,
// the read-side twin of TestHealthConditionalWrite. A current server advertises follow-next, so the
// predicate is true; a peer that does not list it reads false — the fail-safe that makes the cli
// refuse a follow-next rather than silently return the current value against an older server.
func TestHealthFollowNext(t *testing.T) {
	_, c := memClient(t, store.Config{})
	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !h.FollowNext() {
		t.Errorf("a current server does not report follow-next: features = %v", h.Features)
	}
	if (client.Health{}).FollowNext() {
		t.Error("FollowNext() is true for a server advertising no features")
	}
}
