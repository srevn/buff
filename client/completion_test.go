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

// rtFunc adapts a function to an http.RoundTripper so a test can inject a non-conforming
// transport through the public New(url, hc) seam — the same seam a caller uses to supply a
// custom *http.Client. http.Client.Do hands the body this RoundTripper returns to the client
// untouched, so a plain io.EOF survives all the way to the completion check, which is exactly
// the truncation a real net/http connection would instead raise as io.ErrUnexpectedEOF.
type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// rtClient builds a client whose every request is answered by rt.
func rtClient(t *testing.T, rt http.RoundTripper) *client.Client {
	t.Helper()
	c, err := client.New("http://buff.invalid", &http.Client{Transport: rt})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

// cleanEOFBody serves its payload and then a clean io.EOF, whatever ContentLength the response
// declares — never the io.ErrUnexpectedEOF a conforming transport raises for a short body. It is
// the non-conforming transport the byte-count tripwire defends against.
type cleanEOFBody struct{ r *strings.Reader }

func (b *cleanEOFBody) Read(p []byte) (int, error) { return b.r.Read(p) }
func (b *cleanEOFBody) Close() error               { return nil }

// eofWithData returns its remaining payload together with io.EOF in a single Read once it fits in
// the caller's buffer — the data-plus-io.EOF form an io.Reader is permitted to use. It pins that
// the tripwire tallies those final bytes, which only happens because the count is taken before the
// error is switched on, not only on a nil-error read.
type eofWithData struct{ data []byte }

func (b *eofWithData) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}
func (b *eofWithData) Close() error { return nil }

// TestCompletionContentLengthTripwire drives the finalized arm of the completion rule through the
// non-conforming transport above: a body that reaches a clean io.EOF regardless of its declared
// Content-Length. A real net/http connection never produces this — a short fixed-length body
// surfaces as io.ErrUnexpectedEOF — so the byte count is the only thing that can catch a count
// that fell short of the declared length while the transport called the end clean. The positive
// controls prove the count never invents a torn read on a body that did deliver its full length.
func TestCompletionContentLengthTripwire(t *testing.T) {
	ctx := context.Background()
	// resp frames a finalized GET: a declared Content-Length and a body, with the metadata headers
	// a real server sends so parseClip has its generation, though completion reads neither.
	resp := func(r *http.Request, contentLength int64, b io.ReadCloser) *http.Response {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Buff-Generation": {"g"}, "Buff-Finalized": {"true"}},
			ContentLength: contentLength,
			Body:          b,
			Request:       r,
		}
	}

	t.Run("short body under a longer declared length is torn", func(t *testing.T) {
		c := rtClient(t, rtFunc(func(r *http.Request) (*http.Response, error) {
			return resp(r, 100, &cleanEOFBody{strings.NewReader("12345")}), nil
		}))
		rc, _, err := c.Get(ctx, "x")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		defer rc.Close()
		if _, err := io.Copy(io.Discard, rc); !errors.Is(err, clip.ErrAborted) {
			t.Errorf("short clean-EOF body: err = %v, want ErrAborted", err)
		}
	})

	t.Run("exact declared length is a clean complete read", func(t *testing.T) {
		const payload = "12345"
		c := rtClient(t, rtFunc(func(r *http.Request) (*http.Response, error) {
			return resp(r, int64(len(payload)), &cleanEOFBody{strings.NewReader(payload)}), nil
		}))
		rc, _, err := c.Get(ctx, "x")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Errorf("exact-length body: err = %v, want nil (no false torn)", err)
		}
		if string(got) != payload {
			t.Errorf("body = %q, want %q", got, payload)
		}
	})

	t.Run("final bytes delivered with io.EOF still count toward the length", func(t *testing.T) {
		const payload = "data-and-eof-in-one-read"
		c := rtClient(t, rtFunc(func(r *http.Request) (*http.Response, error) {
			return resp(r, int64(len(payload)), &eofWithData{data: []byte(payload)}), nil
		}))
		rc, _, err := c.Get(ctx, "x")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Errorf("data-plus-EOF body: err = %v, want nil — the final bytes must be counted", err)
		}
		if string(got) != payload {
			t.Errorf("body = %q, want %q", got, payload)
		}
	})
}

// TestCompletionBothSignalsLengthArmWins feeds a response carrying both a Content-Length and a
// Buff-Status: complete trailer — a pairing HTTP forbids and a real buff server never emits, but a
// malformed intermediary might. complete() checks the length first, so a count short of the declared
// length is torn even though the trailer claims complete: the length arm wins, and the trailer is
// never consulted while a length is present. This pins the precedence that stops a fabricated
// completion trailer from overriding a short fixed-length body.
func TestCompletionBothSignalsLengthArmWins(t *testing.T) {
	ctx := context.Background()
	c := rtClient(t, rtFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Buff-Generation": {"g"}, "Buff-Finalized": {"true"}},
			ContentLength: 100,
			Body:          &cleanEOFBody{strings.NewReader("a body far short of the declared length")},
			Trailer:       http.Header{"Buff-Status": {"complete"}},
			Request:       r,
		}, nil
	}))
	rc, _, err := c.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	if _, err := io.Copy(io.Discard, rc); !errors.Is(err, clip.ErrAborted) {
		t.Errorf("short body with a complete trailer present: err = %v, want ErrAborted (length arm wins)", err)
	}
}
