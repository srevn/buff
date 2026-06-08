package cli

import (
	"context"
	"strings"

	"github.com/srevn/buff/client"
)

// requireCaps refuses an operation before it runs when the server lacks a capability the chosen
// options need. It probes /health only when something is required — an ordinary copy or paste
// names no gated option, so req is empty and it pays no extra round-trip. A miss is reported
// as the operational mismatch it is: a well-formed command the target is too old to honour, a
// capabilityError distinct from a usageError. It names no protocol string of its own — req is the
// opaque set the option's Requires reported, forwarded straight to Health.Missing, so the wire
// vocabulary stays on the client side of the seam. An old server cannot be made to refuse a header
// it is built to ignore, so a pre-flight is the only safe check; it is best-effort against the one
// backend it probes (the cross-fleet residual is on the PutOpts.IfMatch doc). A probe error fails
// closed: the operation aborts rather than proceeding past an unanswered gate.
func requireCaps(ctx context.Context, c *client.Client, req []string) error {
	if len(req) == 0 {
		return nil
	}
	h, err := c.Health(ctx)
	if err != nil {
		return err
	}
	if miss := h.Missing(req); len(miss) > 0 {
		return &capabilityError{caps: miss}
	}
	return nil
}

// capabilityError is a well-formed command the target server cannot honour: the chosen options need
// a capability the server does not advertise. It is its own type rather than a usageError — whose
// meaning is a malformed command line — so the code never miscategorises "this server is too old"
// as "you typed it wrong," even though the exit map buckets both at the generic 1. The diagnostic
// names the missing capabilities in the server's own vocabulary, the same strings /health
// advertises; the cli prints them as data from Missing and spells no feature literal itself.
type capabilityError struct{ caps []string }

func (e *capabilityError) Error() string {
	return "buff: server does not support " + strings.Join(e.caps, ", ")
}
