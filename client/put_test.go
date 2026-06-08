package client_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// TestPutConsumeUnconfirmed pins Put's fail-closed guard on consume-once. The PUT 200 reports the
// clip the server stored, so a requested consume-once absent from that report means a stripping
// intermediary or a non-implementing server left a persistent, re-readable clip where the caller
// meant an ephemeral one. Put must then fail loud and best-effort delete the leaked clip — never
// report it as the ephemeral one it is not. The stub servers send a response a conforming buff
// server never would, which is exactly the peer this guard exists for; the round-trip suites cover
// the honoured path against a real server.
func TestPutConsumeUnconfirmed(t *testing.T) {
	ctx := context.Background()

	t.Run("unconfirmed fails closed and cleans up", func(t *testing.T) {
		var deleted atomic.Bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPut:
				// A 200 with a generation and size but no Buff-Consume echo: the consume-once was dropped
				// inbound or never implemented, so the server stored an ordinary, persistent clip.
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set(wire.HeaderGeneration, "gen-1")
				w.Header().Set(wire.HeaderSize, "6")
				w.Header().Set(wire.HeaderFinalized, wire.BoolTrue)
				w.WriteHeader(http.StatusOK)
			case http.MethodDelete:
				deleted.Store(true)
				w.WriteHeader(http.StatusNoContent)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		}))
		defer ts.Close()

		_, err := newClient(t, ts.URL).Put(ctx, "sec", strings.NewReader("secret"), clip.Meta{Kind: clip.KindBytes}, client.PutOpts{ConsumeOnce: true})
		if !errors.Is(err, client.ErrConsumeUnconfirmed) {
			t.Errorf("Put = %v, want ErrConsumeUnconfirmed", err)
		}
		if !deleted.Load() {
			t.Error("fail-closed did not issue a cleanup DELETE for the unconfirmed consume-once clip")
		}
	})

	t.Run("confirmed echo passes clean", func(t *testing.T) {
		var deleted atomic.Bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPut:
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set(wire.HeaderGeneration, "gen-1")
				w.Header().Set(wire.HeaderSize, "6")
				w.Header().Set(wire.HeaderFinalized, wire.BoolTrue)
				w.Header().Set(wire.HeaderConsume, wire.BoolTrue) // honoured and echoed
				w.WriteHeader(http.StatusOK)
			case http.MethodDelete:
				deleted.Store(true)
				w.WriteHeader(http.StatusNoContent)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		}))
		defer ts.Close()

		cl, err := newClient(t, ts.URL).Put(ctx, "sec", strings.NewReader("secret"), clip.Meta{Kind: clip.KindBytes}, client.PutOpts{ConsumeOnce: true})
		if err != nil {
			t.Fatalf("Put with an echoed consume-once = %v, want nil", err)
		}
		if !cl.ConsumeOnce {
			t.Error("returned clip does not report the consume-once the server confirmed")
		}
		if deleted.Load() {
			t.Error("a confirmed consume-once must not trigger a cleanup DELETE")
		}
	})
}
