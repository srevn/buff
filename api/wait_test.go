package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// These prove default-wait over HTTP — the relay's third ordering, a consumer arriving before its
// producer, seen from the request edge. The store's wait_test.go already proves the wait mechanic
// deterministically under synctest: waking onto a live write, the consume-once rendezvous, the
// consumed loser that returns rather than parks, and ctx-cancel eviction. These add only what those
// store proofs cannot reach from inside the store: that the GET handler actually opens with Wait
// set (reverting that one word turns these into fast 404s), that a real client disconnect — not
// a hand- canceled context — is what frees a parked waiter, and that a mid-delivery consume-once
// surfaces as a prompt 410. Each runs the GET off the test goroutine under a time ceiling, so a
// lost wake or a mis-gated wait fails fast instead of wedging the suite.

// newWaitGet builds a GET request whose context is canceled when the test ends. A waiting GET is
// run off the test goroutine so its parking is observable; should a regression leave the handler
// parked past the test's ceiling, this cancel frees it at teardown. newServer registers ts.Close
// first, so this later cleanup runs before it (cleanups are LIFO) — the parked request completes,
// and the server's connection drain on Close finds nothing outstanding rather than wedging until
// the go-test timeout. The test then fails at its own ceiling, the honest fast failure.
func newWaitGet(t *testing.T, url string) *http.Request {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build GET %s: %v", url, err)
	}
	return req
}

// TestGetWaitsForCreate is the rendezvous payoff over HTTP: a GET that arrives before its clip
// attaches to the empty name and receives the bytes once they are written. Running it off the test
// goroutine makes its blocking observable — a settle window with the result channel still empty
// is the proof it parked rather than returned a fast 404, the regression a reverted Wait would
// cause. Then a PUT makes the name readable and the woken GET delivers the payload. Framing is
// deliberately not asserted: the PUT finalizes synchronously, so the wake may resolve either the
// still-live generation or the just-finalized one, and both are correct deliveries — the store's
// wait_test.go is where the live-follow arm is pinned.
func TestGetWaitsForCreate(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	payload := []byte("arrived after the wait")

	type result struct {
		body []byte
		code int
		err  error
	}
	got := make(chan result, 1)
	req := newWaitGet(t, ts.URL+wire.PathClips+"/pending")
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			got <- result{err: err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		got <- result{body: body, code: resp.StatusCode, err: err}
	}()

	// The GET must still be parked: nothing is readable at the name, so with Wait set the handler
	// blocks in Open rather than returning a fast 404. A reverted Wait would have a 404 on the channel
	// by the time this settle elapses.
	select {
	case r := <-got:
		t.Fatalf("GET of an absent clip returned early (status %d, err %v); the wait did not engage", r.code, r.err)
	case <-time.After(200 * time.Millisecond):
		// still parked — the wait engaged
	}

	// The write makes the name readable; the parked GET wakes onto it and delivers its bytes.
	if resp := put(t, ts, "pending", payload, nil); resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("PUT = %d, want 200", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("woken GET: %v", r.err)
		}
		if r.code != http.StatusOK {
			t.Errorf("woken GET = %d, want 200", r.code)
		}
		if !bytes.Equal(r.body, payload) {
			t.Errorf("woken GET body = %q, want %q", r.body, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GET did not complete after the clip was written; the wake was lost")
	}
}

// TestGetConsumeOnceMidDrainGone is the wait gate over HTTP: a second reader of a consume-once
// clip that is mid-delivery is told "gone" at once, never made to wait. The one delivery is claimed
// directly on the store and held undrained, which pins the generation in its consumed state with
// current still pointing at it — the deterministic mid-drain window, free of the idle-deadline and
// reader-pace races an HTTP claimant holding its response open would carry. A concurrent GET then
// resolves ErrConsumed, which the gate does not block on (it waits only while a clip is not-here-
// yet), so it must return 410 promptly. Were the gate to wait on any error, this GET would hang to
// the ceiling here — and in production would then be woken by the claimant's cleanup to ErrNotFound
// and wait forever.
func TestGetConsumeOnceMidDrainGone(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})

	if resp := put(t, ts, "secret", []byte("the one secret"), map[string]string{wire.HeaderConsume: "1"}); resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("PUT = %d, want 200", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// Claim the delivery and hold it: current now points at the consumed generation, mid-drain. The
	// reader is closed at test end, running its cleanup.
	rc, _, err := st.Open(context.Background(), "secret", store.GetOpts{})
	if err != nil {
		t.Fatalf("claiming Open: %v", err)
	}
	defer rc.Close()

	type result struct {
		code int
		err  error
	}
	got := make(chan result, 1)
	req := newWaitGet(t, ts.URL+wire.PathClips+"/secret")
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			got <- result{err: err}
			return
		}
		resp.Body.Close()
		got <- result{code: resp.StatusCode}
	}()

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("mid-drain GET: %v", r.err)
		}
		if r.code != http.StatusGone {
			t.Errorf("mid-drain GET = %d, want 410", r.code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("mid-drain GET did not return; the wait gate blocked on ErrConsumed instead of returning it")
	}
}

// openSignaler reports the error each Open returns on a buffered channel, so a test can observe
// a parked wait unblock without racing the handler's own teardown. It embeds store.Store and
// overrides only Open; every other method passes through to the wrapped store unchanged. The
// channel is sized for the single Open one GET makes, so the send never blocks the handler.
type openSignaler struct {
	store.Store
	opened chan error
}

func (o openSignaler) Open(ctx context.Context, name string, opts store.GetOpts) (io.ReadCloser, clip.Clip, error) {
	rc, c, err := o.Store.Open(ctx, name, opts)
	o.opened <- err
	return rc, c, err
}

// TestGetWaitDisconnectUnblocks proves the wait's one guaranteed unblock end to end: a GET on a
// name that never appears holds until the client disconnects, and a disconnect — not a deadline,
// of which the wait has none — is what frees it. A raw connection lets the test close the socket
// while the handler is parked in Open; net/http cancels the request context on a bodyless GET whose
// peer vanished, the wait's select takes ctx.Done, and Open returns context.Canceled. The signaler
// observes that return: silent through the settle window (still parked), then the cancel once the
// socket closes. quiet absorbs the torn-GET line the client-gone reset logs.
func TestGetWaitDisconnectUnblocks(t *testing.T) {
	opened := make(chan error, 1)
	st := openSignaler{Store: store.NewMemory(store.Config{}), opened: opened}
	ts := newServer(t, st, quiet())

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	// A complete, bodyless GET for a name nothing will ever write: the handler routes it into Open and
	// parks. Closing this connection below is the test's stimulus — the disconnect is the wait's only
	// unblock — and on the success path it frees the handler before cleanup, so ts.Close drains clean.
	if _, err := fmt.Fprintf(conn, "GET %s/never HTTP/1.1\r\nHost: x\r\n\r\n", wire.PathClips); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-opened:
		t.Fatalf("Open returned %v before any disconnect; the wait did not engage", err)
	case <-time.After(200 * time.Millisecond):
		// still parked — the wait engaged
	}

	// The disconnect is the only unblock: the wait has no server-side deadline.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-opened:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Open returned %v on disconnect, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Open did not return after the client disconnected; the wait is not ctx-bound")
	}
}
