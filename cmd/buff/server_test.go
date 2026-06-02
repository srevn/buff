package main

import (
	"io"
	"log/slog"
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
