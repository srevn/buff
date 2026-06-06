package api_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// stallBody sends one chunk and then blocks forever (until released at cleanup), standing in for a
// client that opens an upload and then stops sending.
type stallBody struct {
	chunk   []byte
	sent    bool
	release <-chan struct{}
}

func (s *stallBody) Read(p []byte) (int, error) {
	if !s.sent {
		s.sent = true
		return copy(p, s.chunk), nil
	}
	<-s.release
	return 0, io.EOF
}

// slowBody streams many chunks with a gap between each, standing in for an upload that keeps making
// progress but takes longer overall than an absolute cap allows.
type slowBody struct {
	left int
	gap  time.Duration
}

func (s *slowBody) Read(p []byte) (int, error) {
	if s.left <= 0 {
		return 0, io.EOF
	}
	s.left--
	time.Sleep(s.gap)
	return copy(p, []byte("chunk")), nil
}

// streamingPut issues a chunked PUT with the given body and a generous client-side safety timeout,
// returning how long the call took. The safety timeout is far larger than any deadline under test,
// so it only fires if the server-side deadline fails to — which the elapsed-time assertion then
// catches.
func streamingPut(t *testing.T, url string, body io.Reader) (*http.Response, time.Duration, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.TransferEncoding = []string{"chunked"}
	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()
	resp, err := client.Do(req)
	return resp, time.Since(start), err
}

// TestUploadIdleStall checks that a stalled upload trips the per-request idle deadline: the PUT is
// cut promptly rather than hanging, and nothing is finalized.
func TestUploadIdleStall(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{UploadIdle: 100 * time.Millisecond})
	release := make(chan struct{})
	defer close(release)

	resp, elapsed, err := streamingPut(t, ts.URL+"/v1/clips/stalled", &stallBody{chunk: []byte("opening bytes"), release: release})
	if err == nil {
		if resp.StatusCode == http.StatusOK {
			t.Errorf("stalled upload returned 200; the idle deadline did not cut it")
		}
		resp.Body.Close()
	}
	if elapsed > 3*time.Second {
		t.Errorf("stalled upload took %v; the idle deadline did not fire (client safety timeout caught it)", elapsed)
	}

	g := do(t, http.MethodGet, ts.URL+"/v1/clips/stalled", nil, nil)
	g.Body.Close()
	if g.StatusCode != http.StatusNotFound {
		t.Errorf("GET after stalled upload = %d, want 404 (not finalized)", g.StatusCode)
	}
}

// TestUploadMaxIndependentOfIdle checks that the absolute maximum caps an upload on its own and
// cuts even an actively progressing upload — the property idle alone cannot provide. The idle bound
// is set generous (far above the whole test), not disabled: it can no longer be disabled, so the
// test states what it actually proves — the max caps independently of a live idle bound that never
// trips here.
func TestUploadMaxIndependentOfIdle(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{UploadIdle: 10 * time.Second, UploadMax: 200 * time.Millisecond})

	// Twenty 40ms chunks would take ~800ms of active streaming; the 200ms cap must cut it early, long
	// before the generous 10s idle bound (which a chunk every 40ms keeps alive anyway) matters.
	resp, elapsed, err := streamingPut(t, ts.URL+"/v1/clips/toolong", &slowBody{left: 20, gap: 40 * time.Millisecond})
	if err == nil {
		if resp.StatusCode == http.StatusOK {
			t.Errorf("over-long upload returned 200; the max deadline did not cut it")
		}
		resp.Body.Close()
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("over-long upload took %v; the max deadline did not fire independently of the idle bound", elapsed)
	}

	g := do(t, http.MethodGet, ts.URL+"/v1/clips/toolong", nil, nil)
	g.Body.Close()
	if g.StatusCode != http.StatusNotFound {
		t.Errorf("GET after capped upload = %d, want 404 (not finalized)", g.StatusCode)
	}
}

// TestUploadIdleAllowsActiveTransfer is the negative control: an upload that keeps making progress
// within the idle window finishes cleanly even though it lasts well beyond one window.
func TestUploadIdleAllowsActiveTransfer(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{UploadIdle: 150 * time.Millisecond})

	// Eight 40ms chunks span ~320ms — longer than the 150ms idle window, but never idle for it.
	resp, _, err := streamingPut(t, ts.URL+"/v1/clips/active", &slowBody{left: 8, gap: 40 * time.Millisecond})
	if err != nil {
		t.Fatalf("active upload errored: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("active upload = %d, want 200 (a steady transfer must not trip the idle deadline)", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != "" {
		t.Errorf("active upload carried Buff-Error %q", got)
	}

	g := do(t, http.MethodGet, ts.URL+"/v1/clips/active", nil, nil)
	body := readBody(t, g)
	if g.StatusCode != http.StatusOK || len(body) != 8*len("chunk") {
		t.Errorf("GET after active upload = %d, %d bytes; want 200 and %d bytes", g.StatusCode, len(body), 8*len("chunk"))
	}
}

// TestDownloadIdleStall is the download-side twin of TestUploadIdleStall: a stalled *reader*
// trips the per-request idle *write* deadline. A finalized clip larger than any socket buffer is
// served to a client that reads the headers and then stops draining. The server fills the socket,
// parks on the write, and its idle write deadline tears the stream — so the read ends truncated,
// far short of the declared size. Were the bound disabled the parked write would simply wait out
// the pause and then deliver the whole clip once reading resumed, so the truncation is the proof
// the same knob guards idleResetWriter, the symmetric write side, end to end. (The upload side
// is TestUploadIdleStall.)
func TestDownloadIdleStall(t *testing.T) {
	const size = 8 << 20 // larger than any reasonable localhost socket buffer, so the server's write blocks
	st := store.NewMemory(store.Config{})
	wr, err := st.Create(context.Background(), "big", clip.Meta{Kind: clip.KindFile}, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := wr.Write(bytes.Repeat([]byte("x"), size)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ts := newServer(t, st, api.Options{UploadIdle: 200 * time.Millisecond})

	// A generous client timeout that must never be the thing that ends the read, or the test would
	// pass for the wrong reason: the server's idle write deadline, not this, is on trial.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(ts.URL + "/v1/clips/big")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Stall well past the idle window: read nothing while the server's write parks on the full socket
	// and its idle deadline fires. Only then drain — by which point a working bound has already torn
	// the stream, while a disabled one would still be parked, ready to finish on resume.
	time.Sleep(800 * time.Millisecond)

	n, readErr := io.Copy(io.Discard, resp.Body)
	if readErr == nil {
		t.Errorf("stalled download read cleanly (%d bytes); the idle write deadline did not cut it", n)
	}
	if n >= int64(size) {
		t.Errorf("delivered %d of %d bytes; a torn stream must be truncated", n, size)
	}
}
