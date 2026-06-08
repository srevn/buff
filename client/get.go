package client

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// Get opens a clip for reading and returns a reader, a snapshot of the clip's metadata, and an
// error. The reader enforces the completion rule transparently: the caller can simply io.Copy from
// it, and a non-nil result is a truncated read — there is nothing extra to check. The caller must
// close the reader, which frees the connection even on an early or partial read.
//
// A GET resolves the value readable now: a name with nothing readable yet comes back at once as
// ErrNotFound, like any other refusal, with no reader to close. With GetOpts.Wait the server instead
// treats the GET as a rendezvous — it blocks until a write makes the name readable rather than
// 404ing, bounded only by the request context the caller passes — so a consumer can ask to wait for
// a producer arriving after it. Existence is probed without blocking via Stat, which resolves
// through HEAD and never waits, whatever Wait is set to here.
//
// GetOpts carries the read-time options. A caller that sets FollowNext is responsible for pre-
// flighting the server's capability — GetOpts.Requires names it, Health.Missing reports whether the
// server has it — exactly as a conditional Put is; Wait needs no such pre-flight (Requires names
// nothing for it), since an old server honours a wait or 404s honestly rather than diverging.
func (c *Client) Get(ctx context.Context, name string, o GetOpts) (io.ReadCloser, clip.Clip, error) {
	resp, err := c.do(ctx, http.MethodGet, c.clipURL(name), nil, getHeaders(o))
	if err != nil {
		return nil, clip.Clip{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, clip.Clip{}, responseError(resp)
	}
	return newBody(name, resp), parseClip(name, resp.Header), nil
}

// body wraps a GET response body and makes its final Read the place the completion rule is enforced
// — the exact inverse of how the server frames completion. The server arranges that a clean
// finalize, and only a clean finalize, produces either a fully-satisfied Content-Length or a Buff-
// Status: complete trailer; this turns the absence of that signal, at the end of the stream, into
// clip.ErrAborted. So a transparent io.Copy through this reader cannot present a truncated stream
// as a finished one.
type body struct {
	name  string
	resp  *http.Response // its Body is the underlying reader; ContentLength and Trailer carry the completion signal
	count int64          // bytes delivered so far, confirmed against a declared ContentLength at the clean end
}

// newBody wraps resp's body for completion-checked reading.
func newBody(name string, resp *http.Response) *body {
	return &body{name: name, resp: resp}
}

// Read copies through the underlying body and decides completion at its end. A read that returns
// data is passed straight through, its bytes tallied toward the count complete checks. A clean
// io.EOF is the only chance for success: it is honoured only when that count satisfies the declared
// length or a complete trailer is present, and otherwise becomes a torn stream — a finalized body
// that reached a clean end short of its length, or a chunked stream that ended without its complete
// trailer. Any other error is a torn stream too: a short body the transport itself flags as
// io.ErrUnexpectedEOF, an aborted follow or a dropped connection as a read error, and each carries
// the final bytes back alongside the truncation error so a caller copying to a sink emits exactly
// what arrived.
func (b *body) Read(p []byte) (int, error) {
	n, err := b.resp.Body.Read(p)
	b.count += int64(n) // count before the switch, so the n>0-with-io.EOF data-plus-EOF form is tallied
	switch err {
	case nil:
		return n, nil
	case io.EOF:
		if b.complete() {
			return n, io.EOF
		}
		return n, b.torn(nil)
	default:
		return n, b.torn(err)
	}
}

// Close closes the underlying body, freeing its connection. It is safe at any point — after a full
// read, an early break, or never reading at all — so a caller's deferred Close always releases the
// connection. Completion is judged only when a caller actually reaches the end of the stream; an
// intentional partial read is the caller's choice, not a truncation the client invents.
func (b *body) Close() error { return b.resp.Body.Close() }

// complete applies the single completion rule. A non-negative ContentLength means the server
// declared an exact length, and completion is the delivered count reaching it — confirmed here, not
// inferred from the io.EOF alone. Against a conforming net/http this confirmation is always already
// true at io.EOF: a short body surfaces as io.ErrUnexpectedEOF and never reaches this branch, an
// over-long one is clamped to the length, an empty one is 0 == 0. So it costs nothing on the happy
// path. Its purpose is the path that is not conforming — an h2 reframing, a RoundTripper wrapper,
// a proxy that re-frames a live stream as a fixed length and then hangs up short — where the body
// can return a clean io.EOF before the declared bytes arrive; there the count makes the finalized
// arm refuse the truncation as positively as the live arm below refuses a missing trailer, rather
// than trusting a transport invariant to hold. The live arm carries no such byte backstop, and by
// necessity: a live clip has no advance length to cross-check a count against, and a final-size
// trailer would add none — an intermediary that drops the completion trailer drops a size trailer
// with it. So the live arm's only completion signal is the trailer, which net/http populates once
// the body is fully read — which is exactly now, at io.EOF — so reading it here, and only here, is
// both correct and the sole correct moment.
func (b *body) complete() bool {
	if b.resp.ContentLength >= 0 {
		return b.count == b.resp.ContentLength
	}
	return b.resp.Trailer.Get(wire.HeaderStatus) == wire.StatusComplete
}

// torn wraps the single truncation error. clip.ErrAborted is reused for every torn terminus — a
// short finalized body, an aborted live follow, a missing trailer, a dropped connection — because
// the client cannot, and must not, tell them apart: all are a stream that never completed, and
// clip.ErrAborted stays the one matched truncation identity. The underlying cause, when there
// is one, is wrapped alongside it rather than only printed, so a caller may still inspect it —
// a context cancellation, a reset, an io.ErrUnexpectedEOF — the way the request path keeps its
// transport cause inspectable under ErrUnreachable.
func (b *body) torn(cause error) error {
	if cause != nil {
		return fmt.Errorf("incomplete read of %q (%w): %w", b.name, cause, clip.ErrAborted)
	}
	return fmt.Errorf("incomplete read of %q (no completion signal): %w", b.name, clip.ErrAborted)
}
