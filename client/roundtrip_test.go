package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// The client tests live in an external package so they may stand up a real api server over
// a real store and exercise the client across an actual HTTP connection — the production
// import guard checks the client package itself, not its tests, exactly as the api tests
// import the store. Round-trips run against api.New over a memory store; the synthetic
// completion and reverse-map cases use hand-built servers in the other test files.

// newServer starts an api server over st and tears it down at test end.
func newServer(t *testing.T, st store.Store) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(api.New(st, api.Options{}))
	t.Cleanup(ts.Close)
	return ts
}

// newClient builds a client for base, failing the test if construction does.
func newClient(t *testing.T, base string) *client.Client {
	t.Helper()
	c, err := client.New(base, nil)
	if err != nil {
		t.Fatalf("client.New(%q): %v", base, err)
	}
	return c
}

// memClient is the common case: a memory store, an api server over it, and a client for it.
func memClient(t *testing.T, c store.Config) (store.Store, *client.Client) {
	t.Helper()
	st := store.NewMemory(c)
	return st, newClient(t, newServer(t, st).URL)
}

// readFullTimeout reads exactly len(buf) bytes or fails, with a ceiling so a broken live
// stream fails the test rather than hanging it.
func readFullTimeout(t *testing.T, r io.Reader, buf []byte, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := io.ReadFull(r, buf); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read of %d bytes: %v", len(buf), err)
		}
	case <-time.After(d):
		t.Fatalf("read of %d bytes timed out after %v", len(buf), d)
	}
}

// TestRoundTripText is the happy path: a text PUT returns a finalized clip with a generation
// and size, and a GET returns the same generation and the exact bytes with a clean end.
func TestRoundTripText(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()
	payload := []byte("hello, buff")

	put, err := c.Put(ctx, "greet", bytes.NewReader(payload), clip.Meta{Kind: clip.KindText}, client.PutOpts{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Generation == "" {
		t.Error("Put returned an empty generation")
	}
	if put.Size != int64(len(payload)) {
		t.Errorf("Put size = %d, want %d", put.Size, len(payload))
	}
	if !put.Finalized {
		t.Error("Put result is not finalized")
	}

	rc, cl, err := c.Get(ctx, "greet")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	if cl.Generation != put.Generation {
		t.Errorf("Get generation = %q, want %q", cl.Generation, put.Generation)
	}
	if !cl.Finalized {
		t.Error("Get clip is not finalized")
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("body = %q, want %q", got, payload)
	}
}

// TestRoundTripFilename round-trips a file clip's remembered basename through the percent
// codec on both directions, including the two values the wrong codec would corrupt: a
// non-ASCII name and one containing a '+'. The query codec turns '+' into a space; the path
// codec the client and server share preserves it.
func TestRoundTripFilename(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()
	cases := []struct{ slot, filename string }{
		{"doc", "café.pdf"},
		{"plus", "a+b.txt"},
		{"space", "my report.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.filename, func(t *testing.T) {
			meta := clip.Meta{Kind: clip.KindFile, Filename: tc.filename}
			if _, err := c.Put(ctx, tc.slot, bytes.NewReader([]byte("x")), meta, client.PutOpts{}); err != nil {
				t.Fatalf("Put: %v", err)
			}
			cl, err := c.Stat(ctx, tc.slot)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if cl.Meta.Filename != tc.filename {
				t.Errorf("filename round-trip = %q, want %q", cl.Meta.Filename, tc.filename)
			}
			if cl.Meta.Kind != clip.KindFile {
				t.Errorf("kind = %q, want file", cl.Meta.Kind)
			}
		})
	}
}

// TestRoundTripOpts checks each write option survives to a Stat: a TTL yields a set expiry,
// Keep yields none, and consume-once is reported so a caller can warn that a Get spends it —
// and the Stat itself, being a HEAD, must not spend it.
func TestRoundTripOpts(t *testing.T) {
	ctx := context.Background()

	t.Run("ttl sets expiry", func(t *testing.T) {
		_, c := memClient(t, store.Config{})
		if _, err := c.Put(ctx, "ttl", bytes.NewReader([]byte("x")), clip.Meta{Kind: clip.KindText}, client.PutOpts{TTL: time.Hour}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		cl, err := c.Stat(ctx, "ttl")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if cl.ExpiresAt.IsZero() {
			t.Error("a TTL clip reports no expiry")
		}
	})

	t.Run("keep has no expiry", func(t *testing.T) {
		_, c := memClient(t, store.Config{DefaultTTL: time.Hour})
		if _, err := c.Put(ctx, "keep", bytes.NewReader([]byte("x")), clip.Meta{Kind: clip.KindText}, client.PutOpts{Keep: true}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		cl, err := c.Stat(ctx, "keep")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if !cl.ExpiresAt.IsZero() {
			t.Errorf("a kept clip reports an expiry %v", cl.ExpiresAt)
		}
	})

	t.Run("consume-once reported and not spent by stat", func(t *testing.T) {
		_, c := memClient(t, store.Config{})
		put, err := c.Put(ctx, "sec", bytes.NewReader([]byte("the secret")), clip.Meta{Kind: clip.KindText}, client.PutOpts{ConsumeOnce: true})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		// The Put result echoes the consume-once choice the caller set, which a 200 confirms.
		if !put.ConsumeOnce {
			t.Error("Put result does not reflect the consume-once option it set")
		}
		cl, err := c.Stat(ctx, "sec")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if !cl.ConsumeOnce {
			t.Error("consume-once not reported on Stat")
		}
		// The HEAD did not claim it, so the one Get still delivers.
		rc, _, err := c.Get(ctx, "sec")
		if err != nil {
			t.Fatalf("Get after Stat: %v", err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != "the secret" {
			t.Errorf("consume-once delivery = %q, want the secret", got)
		}
	})
}

// TestList covers the empty store (a non-nil empty slice) and a populated one (every clip
// present with its metadata), decoding the server's JSON envelope into domain clips.
func TestList(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()

	got, err := c.List(ctx)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if got == nil {
		t.Error("List returned a nil slice for an empty store, want non-nil empty")
	}
	if len(got) != 0 {
		t.Errorf("List empty = %d clips, want 0", len(got))
	}

	for _, name := range []string{"banana", "apple", "cherry"} {
		if _, err := c.Put(ctx, name, bytes.NewReader([]byte(name)), clip.Meta{Kind: clip.KindText}, client.PutOpts{}); err != nil {
			t.Fatalf("Put %s: %v", name, err)
		}
	}
	// One clip carries every List JSON field the plain text clips leave at a zero value — a
	// file kind, a filename, an expiry, and consume-once. The list JSON field names are the one
	// part of the wire contract not anchored in a shared constant, so this is where a drift
	// between the client's decode tags and the server's encoder tags must surface rather than
	// pass silently.
	rich := clip.Meta{Kind: clip.KindFile, Filename: "café.pdf"}
	if _, err := c.Put(ctx, "report", bytes.NewReader([]byte("data")), rich, client.PutOpts{TTL: time.Hour, ConsumeOnce: true}); err != nil {
		t.Fatalf("Put report: %v", err)
	}
	clips, err := c.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(clips) != 4 {
		t.Fatalf("List = %d clips, want 4", len(clips))
	}
	byName := map[string]clip.Clip{}
	for _, cl := range clips {
		byName[cl.Name] = cl
	}
	for _, name := range []string{"banana", "apple", "cherry"} {
		cl, ok := byName[name]
		if !ok {
			t.Errorf("List missing %q", name)
			continue
		}
		if !cl.Finalized {
			t.Errorf("%q not reported finalized", name)
		}
		if cl.Size != int64(len(name)) {
			t.Errorf("%q size = %d, want %d", name, cl.Size, len(name))
		}
		if cl.Generation == "" {
			t.Errorf("%q has no generation", name)
		}
		if cl.CreatedAt.IsZero() || cl.FinalizedAt.IsZero() {
			t.Errorf("%q has zero created/finalized time", name)
		}
	}

	// The rich clip's non-default fields must survive the List decode intact — each is a
	// distinct JSON tag the server's encoder must agree with.
	rep, ok := byName["report"]
	if !ok {
		t.Fatal(`List missing the rich clip "report"`)
	}
	if rep.Meta.Kind != clip.KindFile {
		t.Errorf("report kind = %q, want file", rep.Meta.Kind)
	}
	if rep.Meta.Filename != "café.pdf" {
		t.Errorf("report filename = %q, want café.pdf", rep.Meta.Filename)
	}
	if rep.ExpiresAt.IsZero() {
		t.Error("report has a TTL but List reports no expiry")
	}
	if !rep.ConsumeOnce {
		t.Error("report is consume-once but List reports it false")
	}
}

// TestDelete deletes a finalized clip and confirms it is then a not-found, and that deleting
// a name that never existed is itself a not-found.
func TestDelete(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()

	if _, err := c.Put(ctx, "gone", bytes.NewReader([]byte("bye")), clip.Meta{Kind: clip.KindText}, client.PutOpts{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(ctx, "gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := c.Get(ctx, "gone"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
	}
	if err := c.Delete(ctx, "never"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Delete missing: err = %v, want ErrNotFound", err)
	}
}

// TestPutChunked streams a body of unknown length so the request is chunked, proving the
// client does not require a length and the server reads it to a clean end either way.
func TestPutChunked(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()
	payload := []byte("a chunked upload with no content length")

	// unknownLen hides the length so net/http must use chunked transfer encoding.
	if _, err := c.Put(ctx, "ch", unknownLen{bytes.NewReader(payload)}, clip.Meta{Kind: clip.KindText}, client.PutOpts{}); err != nil {
		t.Fatalf("Put chunked: %v", err)
	}
	rc, _, err := c.Get(ctx, "ch")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	if got, _ := io.ReadAll(rc); !bytes.Equal(got, payload) {
		t.Errorf("round-trip = %q, want %q", got, payload)
	}
}

// unknownLen hides a reader's length from net/http, forcing a chunked request body.
type unknownLen struct{ r io.Reader }

func (u unknownLen) Read(p []byte) (int, error) { return u.r.Read(p) }

// TestNameAuthorityServerSide confirms the client does not pre-validate names: an invalid
// name reaches the server escaped as one path segment and comes back as name_invalid,
// keeping the namespace authority on the server with no second validator to drift.
func TestNameAuthorityServerSide(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()
	// "_bad" fails the server's ValidName (a name must start alphanumeric) yet is a clean
	// single path segment, so it tests the rejection without path-routing ambiguity.
	_, err := c.Put(ctx, "_bad", bytes.NewReader([]byte("x")), clip.Meta{Kind: clip.KindText}, client.PutOpts{})
	if !errors.Is(err, clip.ErrNameInvalid) {
		t.Errorf("Put bad name: err = %v, want ErrNameInvalid", err)
	}
}

// TestPutDefaultsKind proves Put fills an absent kind with text at the wire boundary, so the
// returned clip and the server's stored state agree on the concrete kind rather than the client
// handing back an empty kind the server silently defaulted. The follow-up Stat reads the kind back
// off the server, which confirms the wire header carried text too — not just the returned value.
func TestPutDefaultsKind(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()

	put, err := c.Put(ctx, "nokind", bytes.NewReader([]byte("x")), clip.Meta{}, client.PutOpts{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Meta.Kind != clip.KindText {
		t.Errorf("returned kind = %q, want text", put.Meta.Kind)
	}
	cl, err := c.Stat(ctx, "nokind")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if cl.Meta.Kind != clip.KindText {
		t.Errorf("stored kind = %q, want text — the wire header did not carry the default", cl.Meta.Kind)
	}
}

// TestListPaginationRefused proves List fails loud when a server hands back a non-empty pagination
// cursor this client cannot follow. Returning only the first page would be the silent truncation
// the completion discipline forbids, so the capability gap must surface as an error, not a partial
// list. A small stub stands in for a future paginating server v1 never is.
func TestListPaginationRefused(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = io.WriteString(w, `{"clips":[],"next":"more"}`)
	}))
	defer ts.Close()

	c := newClient(t, ts.URL)
	_, err := c.List(context.Background())
	if err == nil {
		t.Fatal("List of a paginated response returned a nil error")
	}
	if !strings.Contains(err.Error(), "paginated") {
		t.Errorf("err = %v, want it to name the pagination capability gap", err)
	}
}
