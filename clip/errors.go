package clip

import "errors"

// The lifecycle sentinel errors. Every layer compares against these with errors.Is, and the HTTP
// layer maps each to a status code and a machine-readable sentinel string. They are made with
// errors.New — no wrapping, no formatting — so the offending input is never echoed back inside
// the error: a rejected name or filename may be hostile, and the sentinel identity is all a caller
// needs to match on.
var (
	// ErrNotFound means the name has no finalized generation to read or delete.
	ErrNotFound = errors.New("buff: clip not found")

	// ErrConsumed means a consume-once clip has already been claimed by a reader; its bytes are gone
	// and will not be delivered again.
	ErrConsumed = errors.New("buff: clip already consumed")

	// ErrBusy means a generation is already being written to this name. At most one live generation
	// may exist per name, so a second concurrent write is refused rather than queued behind the first.
	ErrBusy = errors.New("buff: clip is being written")

	// ErrClosed means a write arrived after its generation was finalized; the writer is no longer
	// accepting bytes.
	ErrClosed = errors.New("buff: clip closed")

	// ErrTooLarge means a write would exceed the per-clip size cap. The offending chunk is rejected
	// whole and the generation discarded — never truncated to fit.
	ErrTooLarge = errors.New("buff: clip exceeds size limit")

	// ErrNoSpace means a write or create would exceed the store's total-byte or clip-count cap.
	ErrNoSpace = errors.New("buff: store is full")

	// ErrNameInvalid means a clip name failed ValidName.
	ErrNameInvalid = errors.New("buff: invalid clip name")

	// ErrAborted is handed to a follower of a live generation whose write was aborted — or whose
	// server crashed — before it finalized. It marks a torn, never- completed stream and has no HTTP
	// status of its own: the connection is reset instead, so a truncated read can never be mistaken
	// for a complete one.
	ErrAborted = errors.New("buff: write aborted")

	// ErrFilenameInvalid means a decoded filename failed ValidFilename. It is a malformed-request
	// condition distinct from ErrNameInvalid: the HTTP layer maps it to a generic bad request, not to
	// the name-invalid sentinel.
	ErrFilenameInvalid = errors.New("buff: invalid filename")
)

// Sentinels enumerates every lifecycle sentinel above, in declaration order, so the layers that
// translate a clip identity can be tested for completeness instead of hand-audited. Go cannot
// enumerate a package's vars at runtime, so without this nothing could range "all clip sentinels":
// api/ ranges it to prove every sentinel is forward-mapped to an HTTP status — or is ErrAborted,
// which resets the connection rather than producing one — and cli/ to prove every sentinel is exit-
// coded, or is deliberately the generic usage code. It names the vars rather than re-spelling them,
// and a test parses this file to prove it lists exactly the sentinels declared with errors.New:
// that keying is exact, not a heuristic, because the package cannot import fmt, so errors.New is
// the only form a sentinel can take. So "add a sentinel, forget a hop" is a build failure rather
// than a silent gap left to discipline.
var Sentinels = []error{
	ErrNotFound, ErrConsumed, ErrBusy, ErrClosed, ErrTooLarge,
	ErrNoSpace, ErrNameInvalid, ErrAborted, ErrFilenameInvalid,
}
