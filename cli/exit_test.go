package cli

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// TestExitCode pins the error-to-exit-code map: one assertion per code, the multi-sentinel rows
// (5, and 6 with its server-side and archive no-clobber conflicts), and — the part order
// protects — that an error wrapping a cause maps by its outer identity.
// A torn read wrapping a cancellation is still 7; an unreachable server wrapping one is still
// 8; a bare cancellation, a generic *HTTPError, an invalid name, and a usage mistake are all
// the generic 1.
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
		{name: "dest exists is a conflict", err: archive.ErrDestExists, want: 6},
		{name: "merge entry collision is a conflict", err: archive.ErrExists, want: 6},
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
