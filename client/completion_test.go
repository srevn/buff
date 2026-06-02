package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// rawServer hijacks the connection and writes a fixed raw HTTP/1.1 response, so the
// completion rule can be tested against responses a real buff server never emits — a short
// body, a chunked stream with no completion trailer, a body framed only by connection close
// — to prove the rule holds even against a non-buff intermediary.
func rawServer(t *testing.T, raw string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, raw)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// rawResp assembles a raw HTTP/1.1 response from a status line, header lines, and a body, so
// the synthetic cases below read clearly as the wire bytes they are.
func rawResp(status string, headers []string, body string) string {
	s := "HTTP/1.1 " + status + "\r\n"
	for _, h := range headers {
		s += h + "\r\n"
	}
	return s + "\r\n" + body
}

// TestCompletionFinalizedFullRead is the finalized success: an exact Content-Length read to
// its end is a clean, complete read.
func TestCompletionFinalizedFullRead(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()
	payload := []byte("a complete finalized clip")
	if _, err := c.Put(ctx, "done", bytes.NewReader(payload), clip.Meta{Kind: clip.KindText}, client.PutOpts{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, _, err := c.Get(ctx, "done")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read err = %v, want nil (complete)", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("body = %q, want %q", got, payload)
	}
}

// TestCompletionEmptyClip checks the zero-byte boundary: a Content-Length of 0 is a declared
// length, so the empty read is complete, not torn.
func TestCompletionEmptyClip(t *testing.T) {
	_, c := memClient(t, store.Config{})
	ctx := context.Background()
	if _, err := c.Put(ctx, "empty", bytes.NewReader(nil), clip.Meta{Kind: clip.KindText}, client.PutOpts{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, _, err := c.Get(ctx, "empty")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	n, err := io.Copy(io.Discard, rc)
	if err != nil {
		t.Errorf("empty read err = %v, want nil", err)
	}
	if n != 0 {
		t.Errorf("empty read = %d bytes, want 0", n)
	}
}

// TestCompletionLiveFinalize follows a live clip to a clean finalize. The whole read returns
// nil only because the body observed the complete trailer — that is what nil from the copy
// proves, the inverse of the server's clean-end framing.
func TestCompletionLiveFinalize(t *testing.T) {
	st, c := memClient(t, store.Config{})
	ctx := context.Background()
	wr, err := st.Create(ctx, "live", clip.Meta{Kind: clip.KindText}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	p1, p2 := []byte("part-one;"), []byte("part-two")
	if _, err := wr.Write(p1); err != nil {
		t.Fatal(err)
	}

	rc, cl, err := c.Get(ctx, "live")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	if cl.Finalized {
		t.Error("live clip reported finalized at open")
	}

	got1 := make([]byte, len(p1))
	readFullTimeout(t, rc, got1, 3*time.Second)
	if !bytes.Equal(got1, p1) {
		t.Fatalf("first chunk = %q, want %q", got1, p1)
	}

	if _, err := wr.Write(p2); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}

	rest, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("live read err = %v, want nil (complete)", err)
	}
	if !bytes.Equal(rest, p2) {
		t.Errorf("rest = %q, want %q", rest, p2)
	}
}

// TestCompletionLiveAbort tears a live follow mid-stream. The torn stream must surface as
// ErrAborted, never as a clean end — the property that stops a truncated follow from looking
// complete.
func TestCompletionLiveAbort(t *testing.T) {
	st, c := memClient(t, store.Config{})
	ctx := context.Background()
	wr, err := st.Create(ctx, "torn", clip.Meta{Kind: clip.KindText}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	p1 := []byte("partial-data-")
	if _, err := wr.Write(p1); err != nil {
		t.Fatal(err)
	}

	rc, _, err := c.Get(ctx, "torn")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got1 := make([]byte, len(p1))
	readFullTimeout(t, rc, got1, 3*time.Second)

	if err := wr.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, rc); !errors.Is(err, clip.ErrAborted) {
		t.Errorf("aborted read err = %v, want ErrAborted", err)
	}
}

// TestCompletionSynthetic drives the cases a real buff server never produces but a foreign
// intermediary might, each of which must be judged torn: a short Content-Length body, a
// cleanly-framed chunked stream missing its completion trailer, and a body delimited only by
// connection close with no length and no trailer.
func TestCompletionSynthetic(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		raw  string
		// cause, when set, is the transport error the torn read must also wrap — proving the
		// completion rule keeps the underlying cause inspectable while clip.ErrAborted stays
		// the matched truncation identity. A short Content-Length surfaces as ErrUnexpectedEOF;
		// the trailerless cases end on a clean io.EOF with no cause to carry.
		cause error
	}{
		{"short content-length", rawResp("200 OK", []string{
			"Buff-Generation: g", "Buff-Finalized: true", "Buff-Size: 100",
			"Content-Length: 100", "Connection: close",
		}, "0123456789"), io.ErrUnexpectedEOF},
		{"missing trailer", rawResp("200 OK", []string{
			"Buff-Generation: g", "Buff-Finalized: false",
			"Transfer-Encoding: chunked", "Trailer: Buff-Status", "Connection: close",
		}, "5\r\nhello\r\n0\r\n\r\n"), nil},
		{"neither signal", rawResp("200 OK", []string{
			"Buff-Generation: g", "Buff-Finalized: false", "Connection: close",
		}, "close-delimited bytes with no length and no trailer"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, rawServer(t, tc.raw).URL)
			rc, _, err := c.Get(ctx, "x")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			defer rc.Close()
			_, err = io.Copy(io.Discard, rc)
			if !errors.Is(err, clip.ErrAborted) {
				t.Errorf("read err = %v, want ErrAborted", err)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Errorf("read err = %v, want it to also wrap %v", err, tc.cause)
			}
		})
	}
}
