package cli

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/clip"
)

// Source is one end of the copy seam: something that opens as a byte stream plus the
// metadata describing how to remember it. It is the only forward-compatibility hook the
// copy side exposes — a future clipboard bridge (a desktop selection, an OSC 52 escape)
// is a new Source with no change here — which is why it is exported even though the v1
// implementations are unexported and chosen by the grammar rather than injected.
//
// Open returns a reader the caller streams to the server and then closes; Close is plain
// resource release. A source backed by a background producer reports that producer's outcome
// separately, through the internal joiner contract the copy flow recognises — kept apart from
// Close so that releasing the body never blocks on the producer.
type Source interface {
	Open(ctx context.Context) (io.ReadCloser, clip.Meta, error)
}

// chooseSource selects the copy source from the resolved invocation. The sigil grammar
// already settled that these arguments are paths, not slots, so inspecting a path here to
// decide its kind is not the filesystem probing the grammar forbids — that probing is
// about classifying an argument, which is already done; this only reads a path we are
// committed to copying. Statting and opening go through the ordinary os calls rather than
// an *os.Root: taring your own local tree is not a security boundary, the same stance the
// archiver takes on its source side. Confinement begins on the paste side, where the bytes
// come from elsewhere.
//
// No path is stdin (text). Exactly one regular file is that file (its basename remembered).
// Anything else — one directory, or two or more paths — is an archive of them all.
func chooseSource(inv invocation, std IO) (Source, error) {
	if len(inv.paths) == 0 {
		return stdinSource{r: std.In}, nil
	}
	if len(inv.paths) == 1 {
		fi, err := os.Stat(inv.paths[0])
		if err != nil {
			// A named path that cannot be statted is a clean failure, not an archive. The os
			// error already names the path and operation; the prefix only marks it as buff's,
			// so every diagnostic the run prints reads consistently.
			return nil, fmt.Errorf("buff: %w", err)
		}
		if fi.Mode().IsRegular() {
			base := filepath.Base(inv.paths[0])
			if err := clip.ValidFilename(base); err != nil {
				return nil, fmt.Errorf("buff: cannot copy %q: %w", inv.paths[0], err)
			}
			return fileSource{path: inv.paths[0], name: base}, nil
		}
		// A single directory — or a single special file the archiver will report and skip,
		// yielding an empty archive — falls through to the archive source.
	}
	return archiveSource{roots: inv.paths, onSkip: warnSkip(std.Err)}, nil
}

// stdinSource streams standard input as an opaque text clip. A byte stream has no name, so
// the metadata carries only the kind.
type stdinSource struct{ r io.Reader }

// Open wraps the input in a no-op closer: the process owns standard input, so the copy
// flow's Close must not close it. There is nothing to fail, so Open never errors.
func (s stdinSource) Open(ctx context.Context) (io.ReadCloser, clip.Meta, error) {
	return io.NopCloser(s.r), clip.Meta{Kind: clip.KindText}, nil
}

// fileSource streams one regular file as a file clip, remembering its basename so a paste
// can restore the name. The basename was validated when the source was chosen.
type fileSource struct {
	path string
	name string // the validated basename to remember
}

// Open opens the file for streaming. A file that vanished between the stat that chose this
// source and now surfaces here as a clean open error rather than a corrupt transfer.
func (s fileSource) Open(ctx context.Context) (io.ReadCloser, clip.Meta, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, clip.Meta{}, fmt.Errorf("buff: %w", err)
	}
	return f, clip.Meta{Kind: clip.KindFile, Filename: s.name}, nil
}

// archiveSource streams a deterministic tar of its roots as an archive clip. The tar is
// produced on the fly by a background goroutine writing into a pipe, so no archive is ever
// held whole in memory; the reader Open returns is the pipe's read end, and its Close joins
// the producer. v1 sets no filename — an archive paste names its output from the slot, not
// from a remembered basename — so the metadata carries only the kind.
type archiveSource struct {
	roots  []string
	onSkip func(rel string, mode fs.FileMode)
}

// Open starts the archiving goroutine and returns the pipe's read end. The goroutine runs
// the archiver into the pipe writer and closes that writer with the archiver's result, so
// a clean archive ends the reader at EOF and a mid-tar failure ends it with that error —
// which makes the server abort the generation rather than commit a truncated one. The
// producer's error is also sent on a buffered channel the reader's join drains, so the
// true source outcome is recoverable even after the transport has reduced it to a torn
// body. Open itself cannot fail: every error the archiver finds surfaces through the
// reader or its join.
func (s archiveSource) Open(ctx context.Context) (io.ReadCloser, clip.Meta, error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1) // buffered so the producer can send its result and exit, always
	go func() {
		err := archive.Stream(ctx, s.roots, pw, archive.StreamOpts{OnSkip: s.onSkip})
		pw.CloseWithError(err) // nil ends the reader at EOF; an error ends it with that error
		done <- err
	}()
	return &archiveReader{pr: pr, done: done}, clip.Meta{Kind: clip.KindArchive}, nil
}

// archiveReader is the read end of the tar pipe plus the handle that joins its producer. Its
// two jobs are deliberately split across two methods: Close releases the pipe, join reports the
// producer's outcome. Keeping them apart is what lets the transport close the request body — as
// net/http always does — without that close either blocking on the producer or consuming the
// outcome the copy flow needs.
type archiveReader struct {
	pr   *io.PipeReader
	done chan error // the producer's archiving result, delivered once and read by join
}

// Read yields tar bytes from the pipe.
func (a *archiveReader) Read(p []byte) (int, error) { return a.pr.Read(p) }

// Close releases the read end of the tar pipe. It is the resource-release half of the body and
// nothing more: the pipe reader's own close is idempotent, so the transport closing the request
// body and the copy flow closing the source cannot collide. It deliberately does not wait for
// the producer — that is join's job — so a plain Close, the io.Closer contract net/http
// exercises, never blocks on the archiving goroutine.
func (a *archiveReader) Close() error { return a.pr.Close() }

// join waits for the archiving goroutine and returns its outcome. It is the copy flow's, and
// only the copy flow's, to call, exactly once and after Put has returned, so the single buffered
// result is read by a single receiver. It closes the read end first, which unblocks a producer
// still trying to write — the case where the transport stopped early at a cap and left the
// producer mid-archive — then receives the result; closing before receiving is what prevents a
// deadlock. On the clean path the producer has already finished and the receive returns at once.
// The result is nil for a clean archive, the archiver's error for a genuine source failure that
// the copy flow lets win over the transport, or io.ErrClosedPipe when this join stopped a
// running producer, which the copy flow treats as the symptom it is.
func (a *archiveReader) join() error {
	a.pr.Close()
	return <-a.done
}

// warnSkip builds the archiver's skip hook: it writes one stderr line per source entry the
// archiver declines to transfer — a symlink, device, FIFO, or socket it will not follow.
// The relative name comes from the user's own tree, so it is trusted and safe to print. The
// warning goes to Err, never Out, so it cannot contaminate a piped tar.
func warnSkip(w io.Writer) func(rel string, mode fs.FileMode) {
	return func(rel string, mode fs.FileMode) {
		fmt.Fprintf(w, "buff: skipping %s (%s)\n", rel, mode)
	}
}
