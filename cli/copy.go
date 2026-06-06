package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/srevn/buff/client"
)

// joiner is a source body backed by a background producer. Its join reports that producer's outcome
// once the body has been streamed, so the copy flow can prefer a genuine source failure over the
// transport symptom it caused. Only the tar pipe runs a producer; a body without one — standard
// input, a single file — does not implement this, because there is no production step that can fail
// apart from the bytes it yields. The interface is unexported: it is a contract between the copy
// flow and its own sources, not part of the public seam.
type joiner interface {
	join() error
}

// copyFlow is the one shape every copy takes, whatever its source: open the source, stream it to
// the server, then reconcile the two outcomes. The flow owns the source's lifecycle. It lends the
// bytes to Put behind a no-op closer, because the transport closes whatever request body it is
// handed and the flow must remain the single owner of the real Close — where a file descriptor is
// released — rather than racing the transport to close it. It then closes the source itself once
// Put has returned; a read-source's close error is not a copy outcome, since the bytes are either
// delivered or not by then, so that error is dropped here.
//
// The outcome that does matter for a producer-backed source — the tar pipe's archiving result —
// is collected separately through join, kept apart from closing so that releasing the body never
// blocks on the producer and the producer is joined exactly once. A source with no producer has no
// outcome of its own, so the transport error stands. A failure opening the source (a vanished file)
// returns before any Put.
func copyFlow(ctx context.Context, c *client.Client, name string, src Source, o client.PutOpts) error {
	rc, meta, err := src.Open(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, putErr := c.Put(ctx, name, io.NopCloser(rc), meta, o)
	var srcErr error
	if j, ok := rc.(joiner); ok {
		srcErr = j.join()
	}
	return resolveCopyError(srcErr, putErr)
}

// resolveCopyError reconciles a source error with a transport error, giving the underlying cause
// priority over the symptom it caused. When a source's producer fails mid-stream — a file read
// error, a tar of a shrinking file, a missing root — the server and client only ever saw a torn
// request body, so the transport error is the symptom and the source error is the cause: the cause
// wins, and the exit code reflects what actually went wrong.
//
// Two source errors are themselves symptoms and yield to the transport error. io.ErrClosedPipe
// means the join closed the pipe's read end because Put had already failed (a cap rejection),
// so the producer's write failure is downstream of the real status the server returned.
// context.Canceled means the run was canceled, where the transport's own report of the cancellation
// is the one to surface. A nil source error leaves the Put error, which is the common case for the
// non-producer sources that have no separate outcome to weigh.
//
// This determinism is the reason the join is a plain goroutine and channel rather than a first-
// error group: a group surfaces whichever error registered first, which in the cap case races the
// pipe-closed symptom against the real status, while this always picks the cause.
func resolveCopyError(srcErr, putErr error) error {
	if srcErr != nil &&
		!errors.Is(srcErr, io.ErrClosedPipe) &&
		!errors.Is(srcErr, context.Canceled) {
		// The producer's own cause wins. It is an archiver or os error that carries no buff: marker, and
		// cli originates the line that prints it, so mark it here — the one place this cause is chosen
		// over the transport symptom. The putErr branch needs no marking: a client error already carries
		// buff: within its sentinel, and a simple source's fault arrives as client.ErrSource, which
		// already leads with it.
		return fmt.Errorf("buff: %w", srcErr)
	}
	return putErr
}
