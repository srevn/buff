package cli

import (
	"context"
	"errors"
	"os"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// exitCode maps the error a command returns to buff's process exit code. It is the single place
// that mapping lives: the client decodes the wire into typed clip errors and its own transport
// sentinel, and this turns those identities into the numbers a script reads.
//
// The cases are checked in order, but the sentinels are disjoint, so order matters only for an
// error that wraps a cause — and there the outer identity is the user-visible fact. A torn read
// that wraps a cancellation is still a truncation (7); an unreachable server that wraps one is
// still unreachable (8).
//
// Exit 6 is the conflict bucket on both sides of the wire: a server-side write conflict
// (clip.ErrBusy, clip.ErrClosed); a client-side archive no-clobber refusal — archive.
// ErrDestExists, a paste into an existing directory name, and archive.ErrExists, a merge-mode entry
// collision; and os.ErrExist, a file clip's no-clobber save colliding with an existing file when
// saved at a terminal. All are "something is already there," which a script distinguishes from the
// generic usage 1. A consume-once landing that collides at a terminal is normally diverted before
// it reaches this bucket: the flow lands its spent delivery on a free sibling beside the colliding
// name (a narrated beside-save), so the collision costs nothing. The collision stands as a 6 only
// when the divert cannot rescue it — a buff-named sink whose peer sent no generation id, or one
// whose name and generation cannot splice into a valid filename, or an -o sink buff never salvages
// because the user named that target — and then stderr names the delivery lost rather than hiding
// it.
//
// Everything unmatched is the generic 1: a usage mistake, a server error with no clip counterpart
// (an *client.HTTPError, e.g. a generic 400 or a 500), an invalid name the server rejected
// (clip.ErrNameInvalid is usage-class and has no code of its own), a source that faulted mid-upload
// (client.ErrSource — a local read failure, deliberately not the network's 8), or a local file
// error that is not one of the no-clobber conflicts above. A context cancellation reaches here as
// 1 only when it is not already wrapped by a truncation or transport error — a copy aborted by a
// signal surfaces as 8 (the transport error wraps the cancel) and a paste mid-body as 7 (the torn-
// read error wraps it), while an archive paste canceled between entries, with no read in flight to
// tear, returns a bare cancellation that lands in this generic class. Translating a signal to the
// conventional 130 is the job of the process boundary that installs the signal handler and so knows
// a signal fired — it normalises all of these cancellation cases alike; this map sees only the
// resulting error.
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
		errors.Is(err, archive.ErrDestExists), errors.Is(err, archive.ErrExists),
		errors.Is(err, os.ErrExist):
		return 6
	case errors.Is(err, clip.ErrAborted):
		return 7
	case errors.Is(err, client.ErrUnreachable):
		return 8
	default:
		return 1
	}
}

// diagnostic renders the one stderr line a failed run prints, the message-side twin of exitCode.
// Almost every error prints as itself: it already leads with "buff:" and names its own cause, so
// the line is just err.Error(). The lone exception is a cancellation, where the error in hand is
// the symptom the stop produced — a copy's transport ErrUnreachable, a paste's torn-read ErrAborted
// — not the user-visible fact, which is only that the run was stopped. Printing the symptom
// misreports a perfectly reachable server as unreachable (the Ctrl-C-mid-copy "server unreachable:
// ... context canceled") or a deliberate stop as a stream truncation, so a canceled run gets one
// honest line instead. This is exactly where exitCode normalises every cancellation case alike to
// one code at the process boundary, mirrored: their lines normalise alike to one too.
//
// The match is on the wrapped cause; context.Canceled rides under ErrUnreachable, under ErrAborted,
// or bare — so this stays a pure function of the typed error, never reading the context the way the
// boundary's code does. context.DeadlineExceeded is deliberately excluded: a dial that timed out
// is a genuine unreachable, not a stop, and keeps its transport line; and a server- aborted follow
// tears with a non-cancel cause, so it too keeps its faithful "incomplete read" line.
func diagnostic(err error) string {
	if errors.Is(err, context.Canceled) {
		return "buff: canceled"
	}
	return err.Error()
}
