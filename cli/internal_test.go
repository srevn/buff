package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/srevn/buff/clip"
)

// archiveReader must satisfy joiner so the copy flow collects its producer's outcome through
// join rather than through Close — the split that keeps the transport's body-close from either
// blocking on the producer or consuming the outcome.
var _ joiner = (*archiveReader)(nil)

// TestArchiveReaderCloseJoinSplit pins that an archiveReader's two jobs stay apart, the load-
// bearing property behind the copy flow's robustness. Close is the io.Closer contract net/http
// exercises on the request body: it must be idempotent, never block on the producer, and never
// consume the producer's result — so it is called twice here and must stay clean. join is the
// copy flow's single reader of the outcome and must deliver the producer's error. Were the two
// ever merged again, a second Close would drain or block on the one result the flow needs, the
// regression this guards.
func TestArchiveReaderCloseJoinSplit(t *testing.T) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	want := errors.New("archiver failed mid-tar")
	go func() {
		pw.CloseWithError(want) // a mid-tar failure ends the reader with that error
		done <- want            // and reports it on the buffered channel join drains
	}()
	a := &archiveReader{pr: pr, done: done}

	if err := a.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("repeat Close (the transport's close of the same body): %v", err)
	}
	if err := a.join(); err != want {
		t.Errorf("join() = %v, want the producer's %v", err, want)
	}
}

// TestResolveCopyError pins the causal-priority join white-box, since the function is
// unexported. A genuine source error wins over the transport error it caused; the two
// symptom errors — the pipe this flow closed after a failed Put, and a cancellation — yield
// to the transport error so the real status or the transport's own cancellation report
// surfaces; and both-nil is success. This determinism is what a first-error group cannot
// guarantee and the reason the join is hand-rolled.
func TestResolveCopyError(t *testing.T) {
	srcFail := errors.New("read /root/file: input/output error")
	cases := []struct {
		name   string
		srcErr error
		putErr error
		want   error
	}{
		{name: "source error wins over transport symptom", srcErr: srcFail, putErr: clip.ErrTooLarge, want: srcFail},
		{name: "source error with no transport error still surfaces", srcErr: srcFail, putErr: nil, want: srcFail},
		{name: "closed pipe yields to put", srcErr: io.ErrClosedPipe, putErr: clip.ErrTooLarge, want: clip.ErrTooLarge},
		{name: "wrapped closed pipe yields to put", srcErr: fmt.Errorf("stream: %w", io.ErrClosedPipe), putErr: clip.ErrNoSpace, want: clip.ErrNoSpace},
		{name: "cancellation yields to put", srcErr: context.Canceled, putErr: clip.ErrAborted, want: clip.ErrAborted},
		{name: "nil source leaves the put error", srcErr: nil, putErr: clip.ErrTooLarge, want: clip.ErrTooLarge},
		{name: "both nil is success", srcErr: nil, putErr: nil, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveCopyError(tc.srcErr, tc.putErr); got != tc.want {
				t.Errorf("resolveCopyError(%v, %v) = %v, want %v", tc.srcErr, tc.putErr, got, tc.want)
			}
		})
	}
}
