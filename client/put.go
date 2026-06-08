package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/srevn/buff/clip"
)

// Put streams r to a clip and returns the finalized clip on success. The body is handed straight
// to the request so it streams to the server and never accumulates in memory — the client buffers
// no clip bytes. A 200 means stored and durable: the server sends it only after its durable commit
// returns, so a nil error here is a real acknowledgement, not an optimistic one.
//
// A non-2xx response is authoritative even if the body did not finish streaming. When the server
// enforces a cap mid-upload it replies and stops reading; in the common case net/http prefers that
// already-arrived response over the resulting body-write failure, so the status is what the caller
// honours — a real ErrTooLarge or ErrNoSpace, not a bare connection reset. That preference is a
// race in the transport's round-trip, though: in the narrow window where the body-write error wins
// instead, the cap trip surfaces here as ErrUnreachable. It is the accepted cost of the never-drain
// cap policy — reading an oversized body to its end just to guarantee a clean status is the exact
// resource abuse the cap exists to stop, so it cannot be closed server-side.
//
// A failed round-trip with no response is not always the network's fault: the body the caller
// handed in may have faulted under the transport's own read. The client tells the two apart
// by watching that body (recordingReader) — a source-read failure becomes ErrSource, a genuine
// connection failure stays ErrUnreachable — so a file or standard input that dies mid-upload is
// reported as the local fault it is, not a phantom unreachable server. The watch is on the read
// side only, which is exactly why it leaves the cap race above untouched: a cap rejection fails
// the connection write while the source read keeps succeeding, so the recorder stays empty and the
// status (or, in the narrow window, ErrUnreachable) still wins.
//
// On success the returned clip reports the server's stored truth, read back from the response
// headers exactly as Get and Head build theirs through parseClip: the generation and size the
// server assigned, and the consume-once and absolute expiry it actually applied — not the request
// options dressed up as confirmation. A requested consume-once the 200 does not echo is the one
// mismatch Put acts on rather than reports: the write already succeeded, so a persistent clip
// now holds what was meant to self-destruct after one read — a stripping intermediary dropped
// the header inbound, or the server does not implement consume-once — so Put best-effort deletes
// it and returns ErrConsumeUnconfirmed. That fails closed: if the server did honour consume-once
// but a response-only strip hid the echo, the delete spends the clip's single delivery early, the
// availability cost of preferring no-secret-left for a confidentiality primitive. The delete runs
// on the caller's ctx and is best-effort — a canceled caller skips it — so the error, not the
// cleanup, is the signal a caller reasons about.
func (c *Client) Put(ctx context.Context, name string, r io.Reader, m clip.Meta, o PutOpts) (clip.Clip, error) {
	if m.Kind == "" {
		// Default an absent kind here, at the wire boundary, exactly as the server's parse does — the
		// domain Kind validates strictly and never defaults itself, so interpreting an empty wire value
		// is this layer's job. Doing it before the encode is what makes the wire carry the concrete kind
		// the clip is stored under, so the response echoes that same kind back and the returned clip,
		// read from that response, agrees with the server's state rather than an empty one.
		m.Kind = clip.KindBytes
	}
	// Normalize before the encode below so the wire carries the coherent shape the server will keep:
	// the server normalizes again at admission with this same domain method, and the returned clip is
	// read back from the response, so a caller-built Meta with a file-scoped field on a non-file kind
	// is cleaned on the way out and reported clean on the way back rather than echoed as passed.
	m = m.Normalized()
	// Watch the body as the transport reads it, so a body-read failure can be told from a connection
	// failure: do collapses both into one ErrUnreachable, but only the former means the caller's
	// source faulted.
	rec := &recordingReader{r: r}
	resp, err := c.do(ctx, http.MethodPut, c.clipURL(name), rec, encodeHeaders(m, o))
	if err != nil {
		// do reports every failed round-trip as ErrUnreachable, but if the body itself faulted it was the
		// source, not the server. Prefer the recorded read error, which becomes ErrSource and supersedes
		// do's ErrUnreachable entirely so the message names the real cause — except a cancellation, which
		// is the operation being stopped rather than the source failing and stays the transport's report,
		// leaving the existing cancel→ErrUnreachable path (normalised to 130 at the process boundary)
		// unchanged.
		if rec.err != nil && !errors.Is(rec.err, context.Canceled) && !errors.Is(rec.err, context.DeadlineExceeded) {
			return clip.Clip{}, fmt.Errorf("%w: %w", ErrSource, rec.err)
		}
		return clip.Clip{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return clip.Clip{}, responseError(resp)
	}
	defer drain(resp.Body)
	cl := parseClip(name, resp.Header) // the server's stored truth, exactly as Get and Head build it
	if o.ConsumeOnce && !cl.ConsumeOnce {
		// The PUT already succeeded, so a durable clip now holds what the caller meant to self-destruct
		// after one read. Best-effort delete it so an unhonoured consume-once leaves no persistent
		// secret, then report the failure. The delete's own outcome is dropped on purpose: the caller's
		// actionable fact is that consume-once was not confirmed; the cleanup is a courtesy on top of
		// that, not a second guarantee to weigh.
		_ = c.Delete(ctx, name)
		return clip.Clip{}, fmt.Errorf("%w (%q)", ErrConsumeUnconfirmed, name)
	}
	return cl, nil
}

// recordingReader remembers the first non-EOF error the request body's Read returns, so a failed
// round-trip can be attributed to the source rather than the network. net/http reads this body
// on its own write-loop goroutine, so err is written there and read in Put — yet without a lock,
// because the read happens only after do returns an error, and net/http guarantees the body is
// finished by then: its transport waits for the write loop to terminate before returning any round-
// trip error (mapRoundTripError's "wait ... to avoid data races on callers who mutate the request
// on failure"), so the goroutine's last write to err happens-before do returns happens-before
// the read. The early-response paths — a 2xx, or a cap's 4xx/5xx — return a nil error, and there
// the field is never read. One read, strictly after the last write: race-free by net/http's own
// contract, not by chance. The EOF skip mirrors io.Copy's, the loop that drives this Read: a clean
// end is not a fault.
type recordingReader struct {
	r   io.Reader
	err error
}

func (rr *recordingReader) Read(p []byte) (int, error) {
	n, err := rr.r.Read(p)
	if err != nil && err != io.EOF && rr.err == nil {
		rr.err = err
	}
	return n, err
}
