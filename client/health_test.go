package client_test

import (
	"context"
	"slices"
	"testing"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
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

// TestHealthMissing pins the capability-check primitive the cli gates on. A current server
// advertises the gated capabilities, so Missing reports none absent; a server advertising nothing
// reports every requested capability absent, in the order asked — the fail-closed default that
// makes the cli refuse rather than risk a silent unconditional replace or an ordinary read against
// an older server. Missing is how the cli asks a domain question without naming the wire feature
// strings it may not import: it forwards the opaque names a PutOpts or GetOpts reports through
// Requires.
func TestHealthMissing(t *testing.T) {
	_, c := memClient(t, store.Config{})
	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	req := []string{wire.FeatureConditionalWrite, wire.FeatureFollowNext}
	if miss := h.Missing(req); len(miss) != 0 {
		t.Errorf("a current server is missing %v of %v", miss, req)
	}
	// A server advertising no features is missing everything asked of it, preserving order — the fail-
	// closed default that makes an absent capability list refuse, never silently pass.
	if miss := (client.Health{}).Missing(req); !slices.Equal(miss, req) {
		t.Errorf("empty Health Missing(%v) = %v, want all of them", req, miss)
	}
	// Nothing required ⇒ nothing missing, so an ordinary copy or paste pays no gate.
	if miss := h.Missing(nil); len(miss) != 0 {
		t.Errorf("Missing(nil) = %v, want none", miss)
	}
}
