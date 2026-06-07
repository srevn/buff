package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// paste reads a clip and writes it to the chosen sink. It opens the clip, warns once on standard
// error if reading it spent a consume-once delivery, picks the sink from the clip and the output
// streams, and streams the body through. The body is wrapped so that a truncation seen during the
// read is remembered even if the sink relabels it: a torn read is the user-visible fact, so it
// is returned in preference to whatever error the sink reported, which keeps a truncated paste a
// truncation regardless of how the sink failed.
//
// The one place a sink's error is not final is the consume-once salvage. That delivery is spent at
// the server before any byte ships, so a sink that refuses a landing with the body still whole — a
// no-clobber collision at a terminal — would lose the only copy. The flow detects that exact shape
// (consume-once, a refusal, a pristine body) and hands it to divertConsumeOnce, which lands the
// body on a free sibling beside the colliding name and clears the error, turning a lost delivery
// into a narrated beside-save. Keeping the decision here, off the sink, is what stops a future
// terminal sink from silently re-forgetting the rescue.
//
// The two sink families judge completion by two different signals, and both are correct. A byte
// sink — stdout, a file, a raw tar to a pipe — copies the body to its end, so the completion rule
// fires at the stream's terminus and any abort before it surfaces as a truncation (exit 7). An
// archive extract sink instead reads only as far as the tar's own end: a complete tar carries a
// trailer, and reaching it means every entry's declared bytes arrived, so a successful extraction
// is by construction a complete transfer, while an incomplete tar fails extraction and rolls  back
// (exit 7). The trailer is the archive's completion signal — the structural equivalent of the
// byte terminus the raw sinks use.  The one case the two would diverge, a structurally complete
// tar whose generation was aborted only after the trailer, cannot arise from buff's own producer
// — Stream writes the trailer solely on the clean finish that also finalizes — and would still
// deliver complete data, so the extract sink's exit 0 there is honest rather than a missed
// truncation.
func paste(ctx context.Context, c *client.Client, inv invocation, std IO) error {
	rc, cl, err := c.Get(ctx, inv.slot, client.GetOpts{})
	if err != nil {
		return err
	}
	defer rc.Close() // frees the connection on every exit, including a torn read or a sink failure
	if cl.ConsumeOnce {
		fmt.Fprintf(std.Err, "buff: %q was consume-once; this read spent it\n", inv.slot)
	}
	body := &tornReader{r: rc}
	sink := chooseSink(cl, inv, std)
	werr := sink.Write(ctx, body, cl.Meta)
	if werr != nil && cl.ConsumeOnce && body.pristine() {
		// A consume-once delivery is spent at the server the moment it is opened, so a sink that refused
		// its landing with the body still whole would lose the only copy. Hand it to the flow's rescue,
		// which lands it on a free sibling and clears the error, or — if it cannot — returns a refusal
		// that explains why; the loss itself is named once below.
		werr = divertConsumeOnce(ctx, sink, body, cl, werr)
	}
	if body.err != nil {
		werr = body.err // a torn read (primary OR salvage) outranks any error the sink derived from it
	}
	// One place names the loss. A consume-once delivery is spent at the server the instant it is
	// opened, so any consume-once paste that still ends in error — a collision the divert could not
	// rescue, an -o sink it never salvages, a drained body, a torn read — has lost the only copy. Wrap
	// the standing error so the final line distinguishes a spent secret from a replaceable collision,
	// not only the upfront "spent it" notice. %w keeps the cause's identity, so the exit code stays
	// the cause's; a cancellation still renders as the bare "canceled" (diagnostic short- circuits on
	// it) with the tail simply not shown — the user chose to stop. A clean paste, salvage included,
	// leaves werr nil and carries no tail.
	if cl.ConsumeOnce && werr != nil {
		werr = fmt.Errorf("%w; consume-once delivery lost", werr)
	}
	return werr
}

// tornReader passes a reader through while remembering two facts about the bytes that crossed it:
// whether a read ever ended in a truncation, and how many bytes were read at all. The completion-
// checked GET body returns clip.ErrAborted at a torn terminus; a sink that reads the body through
// a tar parser may turn that into the parser's own unexpected-EOF, which would lose the truncation
// identity the exit code depends on — so recording it as it passes lets the paste flow restore
// that identity afterwards, and a torn archive paste exits as a truncation, not a generic failure.
// The byte tally serves the consume-once salvage: a sink refusal with zero bytes read is a whole,
// still-rescuable delivery (see pristine).
type tornReader struct {
	r   io.Reader
	n   int64 // bytes read so far; zero is an untouched body, the consume-once salvage's precondition
	err error // the first truncation seen, which wraps clip.ErrAborted and its cause
}

func (t *tornReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.n += int64(n)
	if err != nil && t.err == nil && errors.Is(err, clip.ErrAborted) {
		t.err = err
	}
	return n, err
}

// pristine reports that the body is whole and untouched — no byte read and no truncation seen.
// It is the consume-once salvage's gate: a sink that refused its landing before reading a byte (a
// no-clobber collision raised at openInDir, an early ExtractNew name collision) leaves the spent
// delivery entirely in the body, so it can still be rescued; a sink that failed after consuming
// bytes (a late ExtractNew race that drained the tar into a temp it then discarded, or a torn read)
// has no whole body left to land, so the salvage must not fire. The byte tally is the only signal
// that tells the two apart — the refusal error is identical across ExtractNew's early and late
// collision — which is why the flow observes it here rather than trusting a sink's control flow.
// The err half is redundant with isCollision for today's two sinks (each refuses before reading,
// so n == 0 already implies no tear), but states the precondition sink-agnostically: whole and
// untouched.
func (t *tornReader) pristine() bool { return t.n == 0 && t.err == nil }
