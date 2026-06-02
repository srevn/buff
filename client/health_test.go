package client_test

import (
	"context"
	"slices"
	"testing"

	"github.com/srevn/buff/store"
)

// TestHealth probes /health through the client and checks the operational report it decodes:
// liveness, the advertised api versions, and the capability list a caller consults before
// relying on an optional feature. Unlike the content methods there is no domain type behind
// it, so this guards the one place the client decodes /health directly.
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
