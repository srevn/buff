package cli

import (
	"errors"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// exitCode maps the error a command returns to buff's process exit code. It is the single
// place that mapping lives: the client decodes the wire into typed clip errors and its own
// transport sentinel, and this turns those identities into the numbers a script reads.
//
// The cases are checked in order, but the sentinels are disjoint, so order matters only for
// an error that wraps a cause — and there the outer identity is the user-visible fact. A torn
// read that wraps a cancellation is still a truncation (7); an unreachable server that wraps
// one is still unreachable (8).
//
// Exit 6 is the conflict bucket on both sides of the wire: a server-side write conflict
// (clip.ErrBusy, clip.ErrClosed) and a client-side archive no-clobber refusal — archive.
// ErrDestExists, a paste into an existing directory name, and archive.ErrExists, a merge-mode
// entry collision. All four are "something is already there," which a script distinguishes
// from the generic usage 1.
//
// Everything unmatched is the generic 1: a usage mistake, a server error with no clip
// counterpart (an *client.HTTPError, e.g. a generic 400 or a 500), an invalid name the server
// rejected (clip.ErrNameInvalid is usage-class and has no code of its own), a local file error
// that is not one of the no-clobber conflicts above. A context cancellation reaches here as 1
// only when it is not already wrapped by a truncation or transport error — a copy aborted by a
// signal surfaces as 8 (the transport error wraps the cancel) and a paste as 7 (the torn-read
// error wraps it), because each is reported through the operation it interrupted. Translating a
// signal to the conventional 130 is the job of the process boundary that installs the signal
// handler and so knows a signal fired; this map sees only the resulting error.
func exitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, clip.ErrNotFound):
		return 3
	case errors.Is(err, clip.ErrConsumed):
		return 4
	case errors.Is(err, clip.ErrTooLarge), errors.Is(err, clip.ErrNoSpace):
		return 5
	case errors.Is(err, clip.ErrBusy), errors.Is(err, clip.ErrClosed),
		errors.Is(err, archive.ErrDestExists), errors.Is(err, archive.ErrExists):
		return 6
	case errors.Is(err, clip.ErrAborted):
		return 7
	case errors.Is(err, client.ErrUnreachable):
		return 8
	default:
		return 1
	}
}
