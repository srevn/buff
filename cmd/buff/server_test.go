package main

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
)

// TestRuntimeClose pins the symmetric teardown for a runtime built but never Run — the path that
// would otherwise leak the bound listener and the open data root. Close releases both and returns
// nil; the data root is then closed (an operation through it fails); and a second Close is a
// harmless no-op, the idempotence that lets the call sites defer Close unconditionally without
// double-faulting against Run's own teardown.
func TestRuntimeClose(t *testing.T) {
	c, err := configFromEnv(getenvFrom(map[string]string{"BUFF_DATA_DIR": t.TempDir()}))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	c.Addr = "127.0.0.1:0" // bind an ephemeral port, like the e2e harness, so the test never clashes
	c.Fsync = false
	rt, err := newRuntime(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}

	// Close without ever calling Run: the build-but-no-Run path. It must release both resources and
	// report no fault, since on this path Close is the first and only closer.
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The data root is released: an operation through the closed root now fails.
	if _, err := rt.root.Open("."); err == nil {
		t.Error("data root still open after Close")
	}
	// Idempotent: a second Close — as happens when Run already tore both down — reports no fault.
	if err := rt.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestNewRuntimeListenClash exercises newRuntime's one post-root error branch: a listen address
// already in use fails after the data root is opened, so the disarm-on-success cleanup is what must
// release it. The assertion is that the failure is reached and reported naming the listen step — the
// closure itself is the idiom's guarantee (one deferred close covering every error path), which no
// portable fd-count check can add to here, so this pins the reached-and-reported half and the idiom
// carries the rest.
func TestNewRuntimeListenClash(t *testing.T) {
	// Pre-bind an ephemeral port and hand newRuntime its address, so its own net.Listen clashes.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pre-bind: %v", err)
	}
	defer busy.Close()

	c, err := configFromEnv(getenvFrom(map[string]string{"BUFF_DATA_DIR": t.TempDir()}))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	c.Addr = busy.Addr().String() // the already-bound address: newRuntime's net.Listen will fail
	c.Fsync = false

	rt, err := newRuntime(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		_ = rt.Close()
		t.Fatal("newRuntime succeeded on a busy address, want a listen error")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error = %v, want one naming the listen step", err)
	}
}
