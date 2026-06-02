package api_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// newServer starts a test HTTP server over st with options o and tears it down at test end.
func newServer(t *testing.T, st store.Store, o api.Options) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(api.New(st, o))
	t.Cleanup(ts.Close)
	return ts
}

// do issues a request and returns the response; the caller closes the body. A transport error
// fails the test, since these tests expect a reply (even an error reply) for every request.
func do(t *testing.T, method, url string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// readBody reads and closes a response body.
func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

// put uploads body to a slot and returns the response.
func put(t *testing.T, ts *httptest.Server, name string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	return do(t, http.MethodPut, ts.URL+wire.PathClips+"/"+name, bytes.NewReader(body), headers)
}

func TestPutGetRoundTrip(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	payload := []byte("hello, buff")

	resp := put(t, ts, "greeting", payload, map[string]string{wire.HeaderKind: string(clip.KindText)})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	gen := resp.Header.Get(wire.HeaderGeneration)
	if gen == "" {
		t.Error("PUT response missing Buff-Generation")
	}
	if got := resp.Header.Get(wire.HeaderSize); got != strconv.Itoa(len(payload)) {
		t.Errorf("PUT Buff-Size = %q, want %d", got, len(payload))
	}
	readBody(t, resp)

	resp = do(t, http.MethodGet, ts.URL+"/v1/clips/greeting", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != int64(len(payload)) {
		t.Errorf("GET Content-Length = %d, want %d", resp.ContentLength, len(payload))
	}
	if len(resp.TransferEncoding) != 0 {
		t.Errorf("finalized GET should not be chunked, got %v", resp.TransferEncoding)
	}
	if got := resp.Header.Get(wire.HeaderFinalized); got != "true" {
		t.Errorf("Buff-Finalized = %q, want true", got)
	}
	if got := resp.Header.Get(wire.HeaderGeneration); got != gen {
		t.Errorf("GET generation = %q, want %q (same as PUT)", got, gen)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream", got)
	}
	body := readBody(t, resp)
	if !bytes.Equal(body, payload) {
		t.Errorf("GET body = %q, want %q", body, payload)
	}
	if got := resp.Trailer.Get(wire.HeaderStatus); got != "" {
		t.Errorf("finalized GET must carry no trailer, got Buff-Status %q", got)
	}
}

func TestPutChunked(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	payload := []byte("chunked upload, no content-length")

	// A body of unknown length forces chunked transfer; the server reads to EOF regardless.
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/clips/c", unknownLen{bytes.NewReader(payload)})
	if err != nil {
		t.Fatal(err)
	}
	req.TransferEncoding = []string{"chunked"}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT chunked: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT chunked status = %d, want 200", resp.StatusCode)
	}
	readBody(t, resp)

	resp = do(t, http.MethodGet, ts.URL+"/v1/clips/c", nil, nil)
	if got := readBody(t, resp); !bytes.Equal(got, payload) {
		t.Errorf("round-trip = %q, want %q", got, payload)
	}
}

func TestPutEmpty(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	resp := put(t, ts, "empty", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT empty status = %d, want 200", resp.StatusCode)
	}
	readBody(t, resp)

	resp = do(t, http.MethodGet, ts.URL+"/v1/clips/empty", nil, nil)
	if resp.ContentLength != 0 {
		t.Errorf("empty GET Content-Length = %d, want 0", resp.ContentLength)
	}
	if got := readBody(t, resp); len(got) != 0 {
		t.Errorf("empty GET body = %q, want empty", got)
	}
}

// TestPutShortContentLength sends a body shorter than its declared Content-Length and
// half-closes, so the server's body read ends early. That is a client truncation: a best-effort
// 400, and crucially no finalize — a later GET must be a not-found.
func TestPutShortContentLength(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	short := "this body is shorter than its declared length"
	fmt.Fprintf(conn, "PUT /v1/clips/short HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\nConnection: close\r\n\r\n%s", short)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrBadReq.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrBadReq.Sentinel)
	}

	resp2 := do(t, http.MethodGet, ts.URL+"/v1/clips/short", nil, nil)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("GET after truncated PUT = %d, want 404 (not finalized)", resp2.StatusCode)
	}
}

func TestPutCapPerClip(t *testing.T) {
	st := store.NewMemory(store.Config{MaxClip: 5})
	ts := newServer(t, st, api.Options{})
	resp := put(t, ts, "big", []byte("ten bytes!"), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrTooLarge.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrTooLarge.Sentinel)
	}
}

func TestPutCapTotal(t *testing.T) {
	st := store.NewMemory(store.Config{MaxTotal: 5})
	ts := newServer(t, st, api.Options{})
	resp := put(t, ts, "big", []byte("ten bytes!"), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("status = %d, want 507", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrNoSpace.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrNoSpace.Sentinel)
	}
}

func TestCreateClipCountCap(t *testing.T) {
	st := store.NewMemory(store.Config{MaxClips: 1})
	ts := newServer(t, st, api.Options{})
	if resp := put(t, ts, "first", []byte("a"), nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("first PUT = %d, want 200", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	resp := put(t, ts, "second", []byte("b"), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("second PUT = %d, want 507", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrNoSpace.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrNoSpace.Sentinel)
	}
}

// TestPutBusy puts to a name that already has a live generation, opened directly on the shared
// store, so the create is refused as busy.
func TestPutBusy(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	wr, err := st.Create(context.Background(), "busy", clip.Meta{Kind: clip.KindText}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer wr.Abort()

	resp := put(t, ts, "busy", []byte("second writer"), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrBusy.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrBusy.Sentinel)
	}
}

func TestPutBadName(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	// "_bad" fails ValidName (a name must start with an alphanumeric); the store rejects it.
	resp := put(t, ts, "_bad", []byte("x"), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrNameBad.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrNameBad.Sentinel)
	}
}

func TestBadHeaders(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{"bad kind", map[string]string{wire.HeaderKind: "video"}},
		{"negative ttl", map[string]string{wire.HeaderTTL: "-1m"}},
		{"garbage ttl", map[string]string{wire.HeaderTTL: "whenever"}},
		{"traversal filename", map[string]string{wire.HeaderFilename: "../etc/passwd"}},
		{"bad percent filename", map[string]string{wire.HeaderFilename: "a%ZZ"}},
		{"non-one keep", map[string]string{wire.HeaderKeep: "true"}},
		{"non-one consume", map[string]string{wire.HeaderConsume: "0"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := put(t, ts, "h", []byte("x"), c.headers)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", resp.StatusCode)
			}
			if got := resp.Header.Get(wire.HeaderError); got != wire.ErrBadReq.Sentinel {
				t.Errorf("Buff-Error = %q, want %q", got, wire.ErrBadReq.Sentinel)
			}
		})
	}
}

func TestGetNotFound(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/ghost", nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderError); got != wire.ErrNotFound.Sentinel {
		t.Errorf("Buff-Error = %q, want %q", got, wire.ErrNotFound.Sentinel)
	}
	if body := readBody(t, resp); strings.TrimSpace(string(body)) != wire.ErrNotFound.Sentinel {
		t.Errorf("body = %q, want the sentinel", body)
	}
}

func TestHeadAndFilename(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	payload := []byte("a report's bytes")
	// A UTF-8 filename round-trips through the percent-codec on both directions.
	if resp := put(t, ts, "doc", payload, map[string]string{
		wire.HeaderKind:     string(clip.KindFile),
		wire.HeaderFilename: "caf%C3%A9.pdf",
	}); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d, want 200", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	resp := do(t, http.MethodHead, ts.URL+"/v1/clips/doc", nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != int64(len(payload)) {
		t.Errorf("HEAD Content-Length = %d, want %d", resp.ContentLength, len(payload))
	}
	if got := resp.Header.Get(wire.HeaderKind); got != string(clip.KindFile) {
		t.Errorf("Buff-Kind = %q, want file", got)
	}
	if got := resp.Header.Get(wire.HeaderFilename); got != "caf%C3%A9.pdf" {
		t.Errorf("Buff-Filename = %q, want the re-encoded café.pdf", got)
	}
	if body := readBody(t, resp); len(body) != 0 {
		t.Errorf("HEAD body = %q, want empty", body)
	}
}

func TestDelete(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})

	t.Run("finalized", func(t *testing.T) {
		if resp := put(t, ts, "d1", []byte("bye"), nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT = %d", resp.StatusCode)
		} else {
			resp.Body.Close()
		}
		resp := do(t, http.MethodDelete, ts.URL+"/v1/clips/d1", nil, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("DELETE = %d, want 204", resp.StatusCode)
		}
		if g := do(t, http.MethodGet, ts.URL+"/v1/clips/d1", nil, nil); g.StatusCode != http.StatusNotFound {
			t.Errorf("GET after DELETE = %d, want 404", g.StatusCode)
			g.Body.Close()
		} else {
			g.Body.Close()
		}
	})

	t.Run("missing", func(t *testing.T) {
		resp := do(t, http.MethodDelete, ts.URL+"/v1/clips/never", nil, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("DELETE missing = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("live only is not found and is left running", func(t *testing.T) {
		wr, err := st.Create(context.Background(), "liveonly", clip.Meta{Kind: clip.KindText}, store.PutOpts{})
		if err != nil {
			t.Fatal(err)
		}
		defer wr.Abort()
		resp := do(t, http.MethodDelete, ts.URL+"/v1/clips/liveonly", nil, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("DELETE of live-only = %d, want 404", resp.StatusCode)
		}
		// The live generation is untouched: it still finalizes.
		if _, err := wr.Write([]byte("still alive")); err != nil {
			t.Errorf("live writer disturbed by DELETE: %v", err)
		}
	})
}

func TestListEmpty(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips", nil, nil)
	body := readBody(t, resp)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	// The array must render as [] (non-nil), never null.
	if !bytes.Contains(body, []byte(`"clips":[]`)) {
		t.Errorf("empty list body = %s, want clips:[]", body)
	}
	var env struct {
		Clips []map[string]any `json:"clips"`
		Next  string           `json:"next"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Clips == nil || len(env.Clips) != 0 || env.Next != "" {
		t.Errorf("envelope = %+v, want empty non-nil clips and empty next", env)
	}
}

func TestListSorted(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	for _, name := range []string{"banana", "apple", "cherry"} {
		if resp := put(t, ts, name, []byte(name), nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s = %d", name, resp.StatusCode)
		} else {
			resp.Body.Close()
		}
	}
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips", nil, nil)
	body := readBody(t, resp)
	var env struct {
		Clips []struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
		} `json:"clips"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := []string{}
	for _, c := range env.Clips {
		got = append(got, c.Name)
	}
	want := []string{"apple", "banana", "cherry"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("list order = %v, want %v", got, want)
	}
}

func TestHealthz(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{Version: "buff/test"})
	resp := do(t, http.MethodGet, ts.URL+"/healthz", nil, nil)
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", resp.StatusCode)
	}
	var doc struct {
		Status   string   `json:"status"`
		Version  string   `json:"version"`
		API      []string `json:"api"`
		Features []string `json:"features"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc.Status != "ok" || doc.Version != "buff/test" {
		t.Errorf("doc = %+v", doc)
	}
	if strings.Join(doc.API, ",") != "v1" {
		t.Errorf("api = %v, want [v1]", doc.API)
	}
	if strings.Join(doc.Features, ",") != "follow,consume-once" {
		t.Errorf("features = %v", doc.Features)
	}
}

// TestErrorMap drives every domain sentinel through a stub store and asserts the status, the
// Buff-Error header, and a constant body — the one forward mapping, end to end. The unknown
// error must become a 500 whose body is the bare sentinel, never the cause.
func TestErrorMap(t *testing.T) {
	cases := []struct {
		err        error
		wantStatus int
		wantErr    string
	}{
		{clip.ErrNotFound, 404, "not_found"},
		{clip.ErrConsumed, 410, "consumed"},
		{clip.ErrBusy, 409, "busy"},
		{clip.ErrTooLarge, 413, "too_large"},
		{clip.ErrNoSpace, 507, "no_space"},
		{clip.ErrNameInvalid, 400, "name_invalid"},
		{clip.ErrFilenameInvalid, 400, "bad_request"},
		{fmt.Errorf("open x: %w", clip.ErrConsumed), 410, "consumed"},
		{errors.New("secret-internal-detail"), 500, "internal"},
	}
	for _, c := range cases {
		t.Run(c.wantErr, func(t *testing.T) {
			ts := newServer(t, stubStore{openErr: c.err}, quiet())
			resp := do(t, http.MethodGet, ts.URL+"/v1/clips/x", nil, nil)
			body := readBody(t, resp)
			if resp.StatusCode != c.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, c.wantStatus)
			}
			if got := resp.Header.Get(wire.HeaderError); got != c.wantErr {
				t.Errorf("Buff-Error = %q, want %q", got, c.wantErr)
			}
			if strings.TrimSpace(string(body)) != c.wantErr {
				t.Errorf("body = %q, want sentinel %q", body, c.wantErr)
			}
			if c.wantStatus == 500 && bytes.Contains(body, []byte("secret-internal-detail")) {
				t.Error("internal cause leaked into the response body")
			}
		})
	}
}

// TestMethodAndPath documents the v1 behaviour for the routes Go answers itself: a wrong method
// on a known path is a 405, an unknown path a 404. Both lack a Buff-Error header, which the
// client treats generically.
func TestMethodAndPath(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	if resp := do(t, http.MethodPost, ts.URL+"/v1/clips/x", nil, nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST a clip = %d, want 405", resp.StatusCode)
		resp.Body.Close()
	} else {
		resp.Body.Close()
	}
	if resp := do(t, http.MethodGet, ts.URL+"/v2/clips/x", nil, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown path = %d, want 404", resp.StatusCode)
		resp.Body.Close()
	} else {
		resp.Body.Close()
	}
}

// unknownLen hides a reader's length so the HTTP client must use chunked transfer encoding.
type unknownLen struct{ r io.Reader }

func (u unknownLen) Read(p []byte) (int, error) { return u.r.Read(p) }

// stubStore forces a chosen error or value from each Store method, so a handler's mapping and
// framing can be exercised in isolation from a real store. A nil error with nil values is a
// degenerate success the error tests never take.
type stubStore struct {
	createErr error
	createW   store.Writer
	openErr   error
	openRC    io.ReadCloser
	openClip  clip.Clip
	statErr   error
	statClip  clip.Clip
	deleteErr error
	listClips []clip.Clip
	listErr   error
}

func (s stubStore) Create(ctx context.Context, name string, m clip.Meta, o store.PutOpts) (store.Writer, error) {
	return s.createW, s.createErr
}
func (s stubStore) Open(ctx context.Context, name string, o store.GetOpts) (io.ReadCloser, clip.Clip, error) {
	return s.openRC, s.openClip, s.openErr
}
func (s stubStore) Stat(ctx context.Context, name string) (clip.Clip, error) {
	return s.statClip, s.statErr
}
func (s stubStore) Delete(ctx context.Context, name string) error { return s.deleteErr }
func (s stubStore) List(ctx context.Context) ([]clip.Clip, error) { return s.listClips, s.listErr }
