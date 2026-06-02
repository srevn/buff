package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// stubServer answers every request with a fixed status and, when non-empty, a Buff-Error
// sentinel and a one-line body — the exact shape the server's error path emits — so the
// reverse map can be exercised in isolation from a real store.
func stubServer(t *testing.T, status int, sentinel string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sentinel != "" {
			w.Header().Set(wire.HeaderError, sentinel)
		}
		w.WriteHeader(status)
		if sentinel != "" {
			fmt.Fprintln(w, sentinel)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestReverseMap drives every canonical wire row through a stub server and asserts the
// client decodes the status-and-sentinel back to its domain error. It is the inverse of the
// server's forward map, built from the same rows, so a drift on either side fails here.
func TestReverseMap(t *testing.T) {
	cases := []struct {
		info wire.ErrInfo
		want error
	}{
		{wire.ErrNotFound, clip.ErrNotFound},
		{wire.ErrConsumed, clip.ErrConsumed},
		{wire.ErrBusy, clip.ErrBusy},
		{wire.ErrClosed, clip.ErrClosed},
		{wire.ErrTooLarge, clip.ErrTooLarge},
		{wire.ErrNoSpace, clip.ErrNoSpace},
		{wire.ErrNameBad, clip.ErrNameInvalid},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.info.Sentinel, func(t *testing.T) {
			c := newClient(t, stubServer(t, tc.info.Status, tc.info.Sentinel).URL)
			_, _, err := c.Get(ctx, "x")
			if !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want errors.Is %v", err, tc.want)
			}
		})
	}
}

// TestUnmappedStatus covers the responses with no faithful single domain error: the sentinels
// that map from more than one server cause (bad_request, internal) or that the client deliberately
// keeps unmapped (unavailable, the shutdown 503 — a transient condition a caller retries rather
// than matching), and a response with no Buff-Error at all (a server-generated 405 or 404, or a
// proxy error). Each becomes a generic HTTPError that preserves the status and sentinel and is
// never mistaken for a clip sentinel.
func TestUnmappedStatus(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		sentinel string
	}{
		{"bad request", http.StatusBadRequest, wire.ErrBadReq.Sentinel},
		{"internal", http.StatusInternalServerError, wire.ErrInternal.Sentinel},
		{"unavailable", http.StatusServiceUnavailable, wire.ErrUnavailable.Sentinel},
		{"native 405", http.StatusMethodNotAllowed, ""},
		{"native 404", http.StatusNotFound, ""},
		{"proxy 502", http.StatusBadGateway, ""},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, stubServer(t, tc.status, tc.sentinel).URL)
			_, _, err := c.Get(ctx, "x")
			var he *client.HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("err = %v, want *client.HTTPError", err)
			}
			if he.Status != tc.status {
				t.Errorf("status = %d, want %d", he.Status, tc.status)
			}
			if he.Sentinel != tc.sentinel {
				t.Errorf("sentinel = %q, want %q", he.Sentinel, tc.sentinel)
			}
			// An unmapped status must never masquerade as a domain sentinel.
			for _, s := range []error{clip.ErrNotFound, clip.ErrNameInvalid, clip.ErrConsumed, clip.ErrBusy} {
				if errors.Is(err, s) {
					t.Errorf("unmapped status leaked as domain error %v", s)
				}
			}
		})
	}
}

// TestUnreachable points the client at an address nothing listens on and asserts every
// method that round-trips reports ErrUnreachable — distinct from any clip sentinel — so the
// CLI can route a transport failure to its own exit code.
func TestUnreachable(t *testing.T) {
	// Bind then immediately release a loopback port, so a connection to it is refused.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	c := newClient(t, "http://"+addr)
	ctx := context.Background()

	t.Run("get", func(t *testing.T) {
		_, _, err := c.Get(ctx, "x")
		assertUnreachable(t, err)
	})
	t.Run("put", func(t *testing.T) {
		_, err := c.Put(ctx, "x", bytes.NewReader([]byte("y")), clip.Meta{Kind: clip.KindText}, client.PutOpts{})
		assertUnreachable(t, err)
	})
	t.Run("stat", func(t *testing.T) {
		_, err := c.Stat(ctx, "x")
		assertUnreachable(t, err)
	})
	t.Run("list", func(t *testing.T) {
		_, err := c.List(ctx)
		assertUnreachable(t, err)
	})
	t.Run("delete", func(t *testing.T) {
		assertUnreachable(t, c.Delete(ctx, "x"))
	})
}

// assertUnreachable fails unless err is ErrUnreachable and not any clip sentinel.
func assertUnreachable(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, client.ErrUnreachable) {
		t.Errorf("err = %v, want ErrUnreachable", err)
	}
	if errors.Is(err, clip.ErrNotFound) {
		t.Error("transport failure leaked as a clip sentinel")
	}
}

// TestPutCapAuthority proves a cap enforced mid-upload is reported as its real status, not a
// transport reset: the streaming body does not finish, yet the client honours the
// already-arrived 413 or 507. The body comfortably exceeds the tiny cap so the rejection
// arrives over the loopback before the write completes, the reliable path Go takes.
func TestPutCapAuthority(t *testing.T) {
	ctx := context.Background()
	body := func() *bytes.Reader { return bytes.NewReader(bytes.Repeat([]byte("x"), 4096)) }

	t.Run("per-clip too large", func(t *testing.T) {
		_, c := memClient(t, store.Config{MaxClip: 5})
		_, err := c.Put(ctx, "big", body(), clip.Meta{Kind: clip.KindText}, client.PutOpts{})
		if !errors.Is(err, clip.ErrTooLarge) {
			t.Errorf("err = %v, want ErrTooLarge", err)
		}
		if errors.Is(err, client.ErrUnreachable) {
			t.Error("a cap rejection surfaced as a transport error, not its status")
		}
	})

	t.Run("total no space", func(t *testing.T) {
		_, c := memClient(t, store.Config{MaxTotal: 5})
		_, err := c.Put(ctx, "big", body(), clip.Meta{Kind: clip.KindText}, client.PutOpts{})
		if !errors.Is(err, clip.ErrNoSpace) {
			t.Errorf("err = %v, want ErrNoSpace", err)
		}
	})
}
