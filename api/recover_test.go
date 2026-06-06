package api_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// quiet returns options with a discarding logger, so the panic a recover test deliberately provokes
// does not clutter the test output.
func quiet() api.Options {
	return api.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// TestRecoverPreStream provokes a panic before any response byte is written. The backstop turns
// it into a real 500 with the constant internal body — the only safe reply when nothing has been
// sent yet.
func TestRecoverPreStream(t *testing.T) {
	ts := newServer(t, openPanicStore{}, quiet())
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/x", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrInternal.Sentinel {
		t.Errorf("Buff-Error = %q, want internal", got)
	}
	if string(body) == "" {
		t.Error("expected the internal sentinel body")
	}
}

// TestRecoverPostStream provokes a panic after the body has started. The status is already gone, so
// the backstop converts it to an abrupt reset: the client sees a 200 with a short body and a read
// error, never a misleading clean end. A live clip is used so the per-chunk flush sends the headers
// before the panic — the genuine "response already started" condition.
func TestRecoverPostStream(t *testing.T) {
	st := stubStore{
		openRC:   &panicReader{left: 3},
		openClip: clip.Clip{Name: "x", Generation: "g", Finalized: false},
	}
	ts := newServer(t, st, quiet())
	resp, err := http.Get(ts.URL + "/v1/clips/x")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (headers sent before the panic)", resp.StatusCode)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Error("expected a truncated read after a post-stream panic, got a clean read")
	}
}

// openPanicStore panics from Open, standing in for an unexpected fault before the response starts.
// The other methods come from the embedded stub and are unused here.
type openPanicStore struct{ stubStore }

func (openPanicStore) Open(ctx context.Context, name string, o store.GetOpts) (io.ReadCloser, clip.Clip, error) {
	panic("unexpected fault before any write")
}

// panicReader yields a few bytes and then panics, standing in for a backing that faults mid-stream
// after the response has started.
type panicReader struct{ left int }

func (p *panicReader) Read(b []byte) (int, error) {
	if p.left > 0 {
		p.left--
		b[0] = 'x'
		return 1, nil
	}
	panic("unexpected fault mid-stream")
}

func (p *panicReader) Close() error { return nil }
