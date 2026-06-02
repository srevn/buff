package api_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/clip"
)

// capHandler captures slog records for assertion. The server goroutine appends while the test reads
// after draining the server, so the mutex keeps the race detector quiet and the reads well-defined.
type capHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}
func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

// only returns the single captured record carrying msg, failing unless there is exactly one — the
// access log emits one line per request, clean or torn.
func (h *capHandler) only(t *testing.T, msg string) slog.Record {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	var found []slog.Record
	for _, r := range h.records {
		if r.Message == msg {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("captured %d %q records, want exactly 1", len(found), msg)
	}
	return found[0]
}

// count reports how many captured records carry msg.
func (h *capHandler) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Message == msg {
			n++
		}
	}
	return n
}

// attrVal extracts one attribute value from a record, failing if the key is absent.
func attrVal(t *testing.T, r slog.Record, key string) slog.Value {
	t.Helper()
	var v slog.Value
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	if !found {
		t.Fatalf("record has no %q attribute", key)
	}
	return v
}

// abortReader yields a few bytes then reports clip.ErrAborted, standing in for a follower whose live
// generation is torn mid-stream. Unlike a panicking reader it drives the intended-abort path — the
// GET handler turns the copy error into an http.ErrAbortHandler reset — which is the live-follow
// truncation the access-log seam must survive without swallowing.
type abortReader struct{ left int }

func (a *abortReader) Read(b []byte) (int, error) {
	if a.left > 0 {
		a.left--
		b[0] = 'x'
		return 1, nil
	}
	return 0, clip.ErrAborted
}
func (a *abortReader) Close() error { return nil }

// finalizedText is a stub serving one small finalized text clip — the common case the access tests
// reuse.
func finalizedText() stubStore {
	return stubStore{
		openRC:   io.NopCloser(strings.NewReader("hello")),
		openClip: clip.Clip{Name: "x", Generation: "g", Meta: clip.Meta{Kind: clip.KindText}, Size: 5, Finalized: true},
	}
}

// TestAccessLogGET asserts a clean finalized GET emits exactly one access line carrying the method,
// slot, status, the clip's declared size, its kind, and aborted=false. The server is drained before
// the assertion so the access-log defer has certainly run.
func TestAccessLogGET(t *testing.T) {
	h := &capHandler{}
	ts := newServer(t, finalizedText(), api.Options{Logger: slog.New(h), AccessLog: true})

	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/x", nil, nil)
	if body := readBody(t, resp); string(body) != "hello" {
		t.Fatalf("body = %q, want hello", body)
	}
	ts.Close() // every handler goroutine, and its access-log defer, has returned

	r := h.only(t, "request")
	if r.Level != slog.LevelInfo {
		t.Errorf("level = %v, want INFO", r.Level)
	}
	if got := attrVal(t, r, "mode").String(); got != http.MethodGet {
		t.Errorf("mode = %q, want GET", got)
	}
	if got := attrVal(t, r, "name").String(); got != "x" {
		t.Errorf("name = %q, want x", got)
	}
	if got := attrVal(t, r, "status").Int64(); got != http.StatusOK {
		t.Errorf("status = %d, want 200", got)
	}
	if got := attrVal(t, r, "size").Int64(); got != 5 {
		t.Errorf("size = %d, want 5", got)
	}
	if got := attrVal(t, r, "kind").String(); got != string(clip.KindText) {
		t.Errorf("kind = %q, want text", got)
	}
	if attrVal(t, r, "aborted").Bool() {
		t.Error("aborted = true, want false on a clean GET")
	}
}

// TestAccessLogTornLiveFollow is the gating property: a torn live-follow both resets the client's
// stream and still emits its access line, marked aborted, through the same recover frame. The reader
// hands over two bytes then aborts; the handler turns that into an http.ErrAbortHandler reset, and
// the access-log defer runs on the unwind without recovering — so the client sees a truncated read
// and the log records status 200, the streamed byte count, and aborted=true.
func TestAccessLogTornLiveFollow(t *testing.T) {
	h := &capHandler{}
	st := stubStore{
		openRC:   &abortReader{left: 2},
		openClip: clip.Clip{Name: "x", Generation: "g", Meta: clip.Meta{Kind: clip.KindText}, Finalized: false},
	}
	ts := newServer(t, st, api.Options{Logger: slog.New(h), AccessLog: true})

	resp, err := http.Get(ts.URL + "/v1/clips/x")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (headers flushed before the abort)", resp.StatusCode)
	}
	_, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr == nil {
		t.Error("expected a truncated read on a torn live-follow, got a clean read")
	}
	ts.Close()

	r := h.only(t, "request")
	if got := attrVal(t, r, "status").Int64(); got != http.StatusOK {
		t.Errorf("status = %d, want 200", got)
	}
	if !attrVal(t, r, "aborted").Bool() {
		t.Error("aborted = false, want true on a torn stream")
	}
	if got := attrVal(t, r, "size").Int64(); got != 2 {
		t.Errorf("size = %d, want 2 (streamed bytes; a live clip has no Buff-Size)", got)
	}
	if got := attrVal(t, r, "kind").String(); got != string(clip.KindText) {
		t.Errorf("kind = %q, want text", got)
	}
}

// TestAccessLogOffByDefault asserts the seam is silent unless enabled: with AccessLog unset (the
// zero Options default) an ordinary request emits no access line. The server is drained first, so a
// missing record means never-emitted, not not-yet-emitted.
func TestAccessLogOffByDefault(t *testing.T) {
	h := &capHandler{}
	ts := newServer(t, finalizedText(), api.Options{Logger: slog.New(h)}) // AccessLog defaults to false

	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/x", nil, nil)
	_ = readBody(t, resp)
	ts.Close()

	if n := h.count("request"); n != 0 {
		t.Errorf("captured %d access records with AccessLog off, want 0", n)
	}
}
