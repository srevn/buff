package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// paste reads a clip and writes it to the chosen sink. It opens the clip, warns once on
// standard error if reading it spent a consume-once delivery, picks the sink from the clip's
// kind and the output streams, and streams the body through. The body is wrapped so that a
// truncation seen during the read is remembered even if the sink relabels it: a torn read is
// the user-visible fact, so it is returned in preference to whatever error the sink reported,
// which keeps a truncated paste a truncation regardless of how the sink failed.
//
// The two sink families judge completion by two different signals, and both are correct. A
// byte sink — stdout, a file, a raw tar to a pipe — copies the body to its end, so the
// completion rule fires at the stream's terminus and any abort before it surfaces as a
// truncation (exit 7). An archive extract sink instead reads only as far as the tar's own
// end: a complete tar carries a trailer, and reaching it means every entry's declared bytes
// arrived, so a successful extraction is by construction a complete transfer, while an
// incomplete tar fails extraction and rolls back (exit 7). The trailer is the archive's
// completion signal — the structural equivalent of the byte terminus the raw sinks use. The
// one case the two would diverge, a structurally complete tar whose generation was aborted
// only after the trailer, cannot arise from buff's own producer — Stream writes the trailer
// solely on the clean finish that also finalizes — and would still deliver complete data, so
// the extract sink's exit 0 there is honest rather than a missed truncation.
func paste(ctx context.Context, c *client.Client, inv invocation, std IO) error {
	rc, cl, err := c.Get(ctx, inv.slot)
	if err != nil {
		return err
	}
	defer rc.Close() // frees the connection on every exit, including a torn read or a sink failure
	if cl.ConsumeOnce {
		fmt.Fprintf(std.Err, "buff: %q was consume-once; this read spent it\n", inv.slot)
	}
	body := &tornReader{r: rc}
	sink := chooseSink(cl.Meta.Kind, inv, std)
	werr := sink.Write(ctx, body, cl.Meta)
	if body.err != nil {
		return body.err // truncation outranks any error the sink derived from it
	}
	return werr
}

// tornReader passes a reader through while remembering whether a read ever ended in a
// truncation. The completion-checked GET body returns clip.ErrAborted at a torn terminus;
// a sink that reads the body through a tar parser may turn that into the parser's own
// unexpected-EOF, which would lose the truncation identity the exit code depends on.
// Recording it as it passes lets the paste flow restore that identity afterwards, so a torn
// archive paste exits as a truncation and not as a generic failure.
type tornReader struct {
	r   io.Reader
	err error // the first truncation seen, which wraps clip.ErrAborted and its cause
}

func (t *tornReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && t.err == nil && errors.Is(err, clip.ErrAborted) {
		t.err = err
	}
	return n, err
}
