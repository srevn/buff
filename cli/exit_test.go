package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// TestExitCode pins the error-to-exit-code map: one assertion per code, the multi-sentinel rows
// (5, and 6 with its server-side, archive, and file no-clobber conflicts), and — the part order
// protects — that an error wrapping a cause maps by its outer identity. A torn read wrapping a
// cancellation is still 7; an unreachable server wrapping one is still 8; a bare cancellation, a
// generic *HTTPError, an invalid name, and a usage mistake are all the generic 1.
func TestExitCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{name: "nil is success", err: nil, want: 0},
		{name: "not found", err: clip.ErrNotFound, want: 3},
		{name: "consumed", err: clip.ErrConsumed, want: 4},
		{name: "too large", err: clip.ErrTooLarge, want: 5},
		{name: "no space", err: clip.ErrNoSpace, want: 5},
		{name: "busy", err: clip.ErrBusy, want: 6},
		{name: "closed", err: clip.ErrClosed, want: 6},
		{name: "precondition failed", err: clip.ErrPreconditionFailed, want: 6},
		{name: "dest exists is a conflict", err: archive.ErrDestExists, want: 6},
		{name: "merge entry collision is a conflict", err: archive.ErrExists, want: 6},
		{name: "file no-clobber collision is a conflict", err: os.ErrExist, want: 6},
		{name: "aborted", err: clip.ErrAborted, want: 7},
		{name: "unreachable", err: client.ErrUnreachable, want: 8},

		{name: "wrapped not found", err: fmt.Errorf("get %q: %w", "x", clip.ErrNotFound), want: 3},
		{name: "torn wrapping cancel is truncation", err: fmt.Errorf("incomplete (%w): %w", context.Canceled, clip.ErrAborted), want: 7},
		{name: "unreachable wrapping cancel is transport", err: fmt.Errorf("%w: %w", client.ErrUnreachable, context.Canceled), want: 8},

		{name: "generic http error", err: &client.HTTPError{Status: 400, Sentinel: "bad_request"}, want: 1},
		{name: "invalid name is usage class", err: clip.ErrNameInvalid, want: 1},
		{name: "usage error", err: usagef("nope"), want: 1},
		{name: "bare cancellation", err: context.Canceled, want: 1},
		{name: "unknown error", err: errors.New("something local"), want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitCode(tc.err); got != tc.want {
				t.Errorf("exitCode(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// TestSentinelExitCompleteness proves every clip sentinel is exit-coded, the clip-keyed
// completeness twin of the per-case TestExitCode above. Ranging clip.Sentinels, each sentinel
// either maps to a code of its own or is one of the two deliberately generic-1 sentinels:
// ErrNameInvalid is usage-class, and ErrFilenameInvalid never reaches the CLI as itself (it
// collapses to bad_request on the wire) but would be 1 if it did. A sentinel that silently fell
// to the generic 1 — the exact drift the wire-side tests cannot reach, since they force a new row
// through wire/api/client but never into exitCode — fails here instead.
//
// Scope: exitCode also codes non-clip inputs — client.ErrUnreachable, archive.ErrDestExists and
// ErrExists, os.ErrExist, a *client.HTTPError, a context cancellation. clip.Sentinels deliberately
// does not range those: the stdlib inputs cannot be enumerated, and client/ and archive/ are small,
// stable sets whose own manifests would be low value. This covers the largest, most-churned domain.
func TestSentinelExitCompleteness(t *testing.T) {
	// The two sentinels deliberately left at the generic usage code 1, for the different reasons
	// stated above. Every other sentinel must have a code of its own.
	knownGeneric := map[error]bool{
		clip.ErrNameInvalid:     true,
		clip.ErrFilenameInvalid: true,
	}
	for _, e := range clip.Sentinels {
		code := exitCode(e)
		if knownGeneric[e] {
			if code != 1 {
				t.Errorf("clip sentinel %v is knownGeneric but exitCode = %d, want the generic 1", e, code)
			}
			continue
		}
		if code == 1 {
			t.Errorf("clip sentinel %v falls to the generic exit 1; give it a code in exitCode or add it to knownGeneric", e)
		}
	}
}
