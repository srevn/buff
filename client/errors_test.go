package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// stubServer answers every request with a fixed status and, when non-empty, a Buff-Error sentinel
// and a one-line body — the exact shape the server's error path emits — so the reverse map can be
// exercised in isolation from a real store.
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

// TestReverseMap drives every canonical wire row through a stub server and asserts the client
// decodes the status-and-sentinel back to its domain error. It is the inverse of the server's
// forward map, built from the same rows, so a drift on either side fails here.
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
			_, _, err := c.Get(ctx, "x", client.GetOpts{})
			if !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want errors.Is %v", err, tc.want)
			}
		})
	}
}

// TestMappedErrorLeadsWithBuff pins the user-facing shape of a mapped refusal: it surfaces as its
// domain error verbatim, so the line leads with "buff:" like every other diagnostic and carries
// no raw wire token. responseError once prefixed the sentinel ("not_found: buff: clip not found"),
// making this the one error that did not lead with buff: — but the domain message already names
// the condition and the exit code already carries the machine identity, so the token added only
// protocol jargon. The bytes a foreign Buff-Error header might carry never reach here: this path
// is taken only when the sentinel exactly matches a known row, so the printed message is the row's
// own constant.
func TestMappedErrorLeadsWithBuff(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
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
	} {
		t.Run(tc.info.Sentinel, func(t *testing.T) {
			c := newClient(t, stubServer(t, tc.info.Status, tc.info.Sentinel).URL)
			_, _, err := c.Get(ctx, "x", client.GetOpts{})
			if err == nil {
				t.Fatal("want an error from a non-2xx response")
			}
			if !strings.HasPrefix(err.Error(), "buff:") {
				t.Errorf("err = %q, want it to lead with buff: like every other diagnostic", err)
			}
			if strings.HasPrefix(err.Error(), tc.info.Sentinel+":") {
				t.Errorf("err = %q, still carries the raw wire-token prefix", err)
			}
			if err.Error() != tc.want.Error() {
				t.Errorf("err = %q, want the domain message %q verbatim", err, tc.want.Error())
			}
		})
	}
}

// TestUnmappedStatus covers the responses with no faithful single domain error: the sentinels that
// map from more than one server cause (bad_request, internal) or that the client deliberately keeps
// unmapped (unavailable, the shutdown 503 — a transient condition a caller retries rather than
// matching), and a response with no Buff-Error at all (a server-generated 405 or 404, or a proxy
// error). Each becomes a generic HTTPError that preserves the status and sentinel and is never
// mistaken for a clip sentinel.
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
			_, _, err := c.Get(ctx, "x", client.GetOpts{})
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

// TestReverseCoverage proves the reverse map partitions the whole wire table: ranging wire.Rows,
// every row either decodes to a domain error or is one of the three deliberately unmapped rows
// that surface as a generic HTTPError. Ranging wire.Rows is what makes it total — a row added to
// the table forces a classification here, mapped or known-absent, rather than slipping through
// silently. The exact per-row identities are pinned by TestReverseMap and TestUnmappedStatus; this
// only proves no row is left unclassified.
func TestReverseCoverage(t *testing.T) {
	// The rows with no faithful single domain counterpart: bad_request and internal each map from more
	// than one server cause, so the inverse cannot pick one; unavailable is a transient 503 a caller
	// retries rather than matches. Each surfaces as a generic HTTPError carrying the status.
	knownAbsent := map[string]bool{
		wire.ErrBadReq.Sentinel:      true,
		wire.ErrInternal.Sentinel:    true,
		wire.ErrUnavailable.Sentinel: true,
	}
	ctx := context.Background()
	for _, row := range wire.Rows {
		t.Run(row.Sentinel, func(t *testing.T) {
			c := newClient(t, stubServer(t, row.Status, row.Sentinel).URL)
			_, _, err := c.Get(ctx, "x", client.GetOpts{})
			var he *client.HTTPError
			if knownAbsent[row.Sentinel] {
				if !errors.As(err, &he) || he.Sentinel != row.Sentinel {
					t.Fatalf("known-absent row %q: want a generic HTTPError, got %v", row.Sentinel, err)
				}
				return
			}
			if errors.As(err, &he) {
				t.Fatalf("mapped row %q: want a domain error, got generic HTTPError %v", row.Sentinel, he)
			}
		})
	}
}

// TestUnreachable points the client at an address nothing listens on and asserts every method that
// round-trips reports ErrUnreachable — distinct from any clip sentinel — so the CLI can route a
// transport failure to its own exit code.
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
		_, _, err := c.Get(ctx, "x", client.GetOpts{})
		assertUnreachable(t, err)
	})
	t.Run("put", func(t *testing.T) {
		_, err := c.Put(ctx, "x", bytes.NewReader([]byte("y")), clip.Meta{Kind: clip.KindBytes}, client.PutOpts{})
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
// transport reset: the streaming body does not finish, yet the client honours the already-arrived
// 413 or 507. The body comfortably exceeds the tiny cap so the rejection arrives over the loopback
// before the write completes, the reliable path Go takes.
func TestPutCapAuthority(t *testing.T) {
	ctx := context.Background()
	body := func() *bytes.Reader { return bytes.NewReader(bytes.Repeat([]byte("x"), 4096)) }

	t.Run("per-clip too large", func(t *testing.T) {
		_, c := memClient(t, store.Config{MaxClip: 5})
		_, err := c.Put(ctx, "big", body(), clip.Meta{Kind: clip.KindBytes}, client.PutOpts{})
		if !errors.Is(err, clip.ErrTooLarge) {
			t.Errorf("err = %v, want ErrTooLarge", err)
		}
		if errors.Is(err, client.ErrUnreachable) {
			t.Error("a cap rejection surfaced as a transport error, not its status")
		}
		// The positive control for the source-watcher: a clean body whose read never fails must not be
		// mistaken for a source fault. A cap rejection breaks the connection write, not the body read, so
		// the recorder stays empty and the status — not ErrSource — is what surfaces.
		if errors.Is(err, client.ErrSource) {
			t.Error("a cap rejection (the source read succeeded) was misread as a source fault")
		}
	})

	t.Run("total no space", func(t *testing.T) {
		_, c := memClient(t, store.Config{MaxTotal: 5})
		_, err := c.Put(ctx, "big", body(), clip.Meta{Kind: clip.KindBytes}, client.PutOpts{})
		if !errors.Is(err, clip.ErrNoSpace) {
			t.Errorf("err = %v, want ErrNoSpace", err)
		}
	})
}

// faultingBody yields its bytes once, then fails every later Read with err — a request body whose
// source (a file, standard input) dies mid-upload. It is the read-side fault Put must tell apart
// from a connection failure.
type faultingBody struct {
	data []byte
	err  error
}

func (f *faultingBody) Read(p []byte) (int, error) {
	if len(f.data) > 0 {
		n := copy(p, f.data)
		f.data = f.data[n:]
		return n, nil
	}
	return 0, f.err
}

// TestPutSourceFault proves a body that faults mid-stream is reported as ErrSource — the caller's
// own source failing — and never as ErrUnreachable, the network. The transport collapses both
// into one failed round-trip; the client tells them apart by watching the body it was handed, so a
// local read failure is not misreported as an unreachable server. The underlying read cause rides
// beneath, so the message names the real fault. Run under -race, this also exercises the lock-free
// cross-goroutine field the recorder relies on: net/http writes it on its body goroutine, Put reads
// it after the round-trip fails.
func TestPutSourceFault(t *testing.T) {
	_, c := memClient(t, store.Config{})
	cause := errors.New("input/output error")
	body := &faultingBody{data: []byte("a partial upload that then faults"), err: cause}
	_, err := c.Put(context.Background(), "x", body, clip.Meta{Kind: clip.KindBytes}, client.PutOpts{})
	if !errors.Is(err, client.ErrSource) {
		t.Errorf("err = %v, want ErrSource", err)
	}
	if errors.Is(err, client.ErrUnreachable) {
		t.Error("a source read fault leaked as ErrUnreachable — the misreport this distinction prevents")
	}
	if !errors.Is(err, cause) {
		t.Errorf("err = %v, want the underlying read cause to ride beneath ErrSource", err)
	}
}

// TestTransportErrorRedactsCredentials points a client whose base carries Basic-auth userinfo at
// a refused port and asserts the resulting transport error redacts the password. The userinfo is a
// working feature — the Transport sends it as Basic auth — so it cannot be rejected, only kept out
// of an error that may reach a terminal or a log, the same refusal that drains a foreign error body
// rather than returning it.
func TestTransportErrorRedactsCredentials(t *testing.T) {
	// Bind then release a loopback port so a connection to it is refused, the same trick
	// TestUnreachable uses to force a transport error with no response.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	c := newClient(t, "http://user:secret@"+addr)
	_, _, err = c.Get(context.Background(), "x", client.GetOpts{})
	if err == nil {
		t.Fatal("Get to a refused port returned a nil error")
	}
	msg := err.Error()
	if strings.Contains(msg, "secret") {
		t.Errorf("transport error leaks the password: %q", msg)
	}
	if !strings.Contains(msg, "xxxxx") {
		t.Errorf("transport error %q does not carry the redaction marker, so the URL was not redacted", msg)
	}
}

// TestHTTPErrorQuotesSentinel is a pure render check: a foreign Buff-Error sentinel — whatever
// bytes a proxy or hostile peer put in the header, here a TAB and a high byte — must come back
// quoted and escaped, so a control byte cannot deface the message and a printable cannot pose as
// the surrounding prose. No server: it exercises HTTPError.Error directly.
func TestHTTPErrorQuotesSentinel(t *testing.T) {
	e := &client.HTTPError{Status: http.StatusBadGateway, Sentinel: "weird\tval\x80"}
	msg := e.Error()
	if !strings.Contains(msg, `\t`) || !strings.Contains(msg, `\x80`) {
		t.Errorf("Error() = %q, want the TAB and high byte rendered as escapes", msg)
	}
	if !strings.Contains(msg, `"weird`) {
		t.Errorf("Error() = %q, want the sentinel quote-delimited", msg)
	}
	// A genuine lowercase-ASCII sentinel survives %q unchanged but for the quotes.
	plain := (&client.HTTPError{Status: http.StatusBadRequest, Sentinel: "bad_request"}).Error()
	if !strings.Contains(plain, `"bad_request"`) {
		t.Errorf("Error() = %q, want the sentinel quoted as \"bad_request\"", plain)
	}
}
