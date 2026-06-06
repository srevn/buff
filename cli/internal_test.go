package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
// unexported. A genuine source error wins over the transport error it caused and is re-attributed
// to cli with the buff: marker (it carries none of its own); the two symptom errors — the pipe
// this flow closed after a failed Put, and a cancellation — yield to the transport error so the
// real status or the transport's own cancellation report surfaces verbatim; and both-nil is
// success. This determinism is what a first-error group cannot guarantee and the reason the join
// is hand-rolled.
func TestResolveCopyError(t *testing.T) {
	srcFail := errors.New("read /root/file: input/output error")
	cases := []struct {
		name   string
		srcErr error
		putErr error
		want   error // the error the result must errors.Is-match
		wrap   bool  // source wins: result is marked buff: and is no longer the identical error
	}{
		{name: "source error wins over transport symptom", srcErr: srcFail, putErr: clip.ErrTooLarge, want: srcFail, wrap: true},
		{name: "source error with no transport error still surfaces", srcErr: srcFail, putErr: nil, want: srcFail, wrap: true},
		{name: "closed pipe yields to put", srcErr: io.ErrClosedPipe, putErr: clip.ErrTooLarge, want: clip.ErrTooLarge},
		{name: "wrapped closed pipe yields to put", srcErr: fmt.Errorf("stream: %w", io.ErrClosedPipe), putErr: clip.ErrNoSpace, want: clip.ErrNoSpace},
		{name: "cancellation yields to put", srcErr: context.Canceled, putErr: clip.ErrAborted, want: clip.ErrAborted},
		{name: "nil source leaves the put error", srcErr: nil, putErr: clip.ErrTooLarge, want: clip.ErrTooLarge},
		{name: "both nil is success", srcErr: nil, putErr: nil, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveCopyError(tc.srcErr, tc.putErr)
			if !errors.Is(got, tc.want) {
				t.Fatalf("resolveCopyError(%v, %v) = %v, want errors.Is %v", tc.srcErr, tc.putErr, got, tc.want)
			}
			if tc.wrap {
				if got == tc.want {
					t.Errorf("source cause should be re-wrapped, got the bare error %v", got)
				}
				if !strings.HasPrefix(got.Error(), "buff:") {
					t.Errorf("source cause = %q, want it to lead with buff:", got.Error())
				}
			} else if got != tc.want {
				t.Errorf("resolveCopyError(%v, %v) = %v, want the verbatim %v", tc.srcErr, tc.putErr, got, tc.want)
			}
		})
	}
}

// TestChooseSinkNewDirLastComponent pins that the terminal archive sink names its new directory
// from the slot's last path component, not the whole slot. It is a no-op while names are single-
// component, but it pins the reduction before ValidName widens to the hierarchical namespace it
// reserves: a slot like "team/work" must extract into "work", a single component ExtractNew
// accepts, rather than tripping its single-component guard. The chooser is driven directly with an
// archive kind and a terminal output, the cell that selects newDirSink.
func TestChooseSinkNewDirLastComponent(t *testing.T) {
	cases := []struct{ slot, want string }{
		{"proj", "proj"},      // single component: unchanged
		{"team/work", "work"}, // hierarchical (forward-compat): reduced to the leaf
		{"a/b/c", "c"},        // deeper still
	}
	for _, tc := range cases {
		t.Run(tc.slot, func(t *testing.T) {
			s := chooseSink(clip.Clip{Meta: clip.Meta{Kind: clip.KindArchive}}, invocation{slot: tc.slot}, IO{OutIsTTY: true})
			nd, ok := s.(newDirSink)
			if !ok {
				t.Fatalf("chooseSink(archive, tty) = %T, want newDirSink", s)
			}
			if nd.name != tc.want {
				t.Errorf("newDirSink.name = %q, want %q (slot's last component)", nd.name, tc.want)
			}
		})
	}
}

// TestChooseSourceRejectsSpecialFile pins the copy-side early-exit: a single path that is
// neither a regular file nor a directory has nothing to archive, so it is refused before any
// transfer opens rather than streamed as an empty clip. /dev/null is the portable special file
// (a character device); the archive.Stream ErrEmptyArchive backstop covers the multi-path lists
// this single-path check cannot reach.
func TestChooseSourceRejectsSpecialFile(t *testing.T) {
	const special = "/dev/null"
	fi, err := os.Stat(special)
	if err != nil || fi.Mode().IsRegular() || fi.IsDir() {
		t.Skipf("%s is not an available special file here (err=%v)", special, err)
	}
	_, err = chooseSource(invocation{paths: []string{special}}, IO{Out: io.Discard, Err: io.Discard})
	if err == nil {
		t.Fatalf("chooseSource(%s) = nil error, want a refusal of a non-regular, non-directory source", special)
	}
	if !strings.Contains(err.Error(), "not a regular file or directory") {
		t.Errorf("error = %q, want it to explain the source is not a regular file or directory", err)
	}
}

// TestDivertConsumeOnceEmptyGeneration pins the empty-generation guard. The salvage names its sibling
// with the delivery's generation id — a wire value a foreign or buggy peer controls — so an absent
// one leaves no way to form a distinct name. Rather than mint a degenerate, non-unique sibling
// (./secret.bin. with a trailing dot, which a second such salvage would then collide with), the
// divert refuses: the body is never touched, the collision identity is kept (so it still scores exit
// 6), and the error names both the missing generation and the lost delivery. A real api server always
// sends a generation, so this floor guards only the foreign peer.
func TestDivertConsumeOnceEmptyGeneration(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	const body = "the one copy"
	r := strings.NewReader(body)
	cl := clip.Clip{ConsumeOnce: true, Meta: clip.Meta{Kind: clip.KindFile, Filename: "secret.bin"}} // Generation: ""
	err := divertConsumeOnce(context.Background(), saveSink{errw: io.Discard, slot: "s"}, r, cl, buffErr(os.ErrExist))
	if err == nil {
		t.Fatal("divert with no generation id returned nil, want a reported loss")
	}
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("err=%v, want it to keep the collision identity (exit 6)", err)
	}
	if e := err.Error(); !strings.Contains(e, "generation") || !strings.Contains(e, "lost") {
		t.Errorf("err=%q, want it to name the missing generation and the lost delivery", e)
	}
	if r.Len() != len(body) {
		t.Errorf("the guard consumed %d bytes from the body, want it untouched", len(body)-r.Len())
	}
	if ents, _ := os.ReadDir(work); len(ents) != 0 {
		t.Errorf("the refused salvage created %d entries, want none (no degenerate sibling)", len(ents))
	}
}
