package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/clip"
)

// Sink is the paste counterpart of Source: somewhere a clip's bytes go. Like Source it is
// the seam a future output bridge would implement, so it is exported even though the v1
// sinks are unexported and selected by chooseSink. Write streams the reader to the
// destination and returns any error; cancellation reaches a byte-copying sink through the
// reader, which is the completion-checked GET body, while the archive sinks also honour ctx
// between entries.
type Sink interface {
	Write(ctx context.Context, r io.Reader, m clip.Meta) error
}

// chooseSink resolves where a paste's bytes go, from the clip and the output streams. It draws
// only on what is known before the body is read — the kind, whether the clip is finalized, whether
// output is a terminal, and whether -o was given — so it stays pure routing; the one disposition
// that needs the bytes themselves, showing text versus saving binary, is made later inside
// terminalSink, the sole place a clip's content is ever inspected.
//
// -o is the explicit target and wins. -o - is the Unix spelling of "raw bytes to stdout" for any
// kind, which also keeps -o - from being taken as a file literally named "-". Without -o: an
// archive at a terminal extracts into a slot-named directory; a finalized file or blob at a
// terminal is peeked and shown-or-saved; everything else — any pipe or redirect, and a still-live
// file or blob at a terminal — is written raw, so piping a tar to tar and redirecting to a file
// behave the way the shell leads one to expect. A live clip is never peeked: its bytes may not have
// arrived, so a peek could block on the producer and tax the very live-follow a terminal exists to
// watch; the rare live-binary-at-a-terminal garble is the accepted price of never stalling.
func chooseSink(cl clip.Clip, inv invocation, std IO) Sink {
	if inv.outputSet {
		if inv.output == "-" {
			return stdoutSink{w: std.Out} // explicit raw stdout, any kind
		}
		if cl.Meta.Kind == clip.KindArchive {
			return extractSink{target: inv.output}
		}
		return fileSink{target: inv.output}
	}
	if cl.Meta.Kind == clip.KindArchive && std.OutIsTTY {
		// ExtractNew publishes one new directory and requires its name to be a single path
		// component, so reduce the slot to its last component. A no-op while names are single-
		// component; what keeps a future hierarchical slot extracting into its leaf rather than
		// tripping ExtractNew's single-component guard. path (not filepath) because a slot is the
		// logical "/"-namespace, not an OS path.
		return newDirSink{name: path.Base(inv.slot)}
	}
	if std.OutIsTTY && cl.Finalized {
		return terminalSink{out: std.Out, errw: std.Err, slot: inv.slot, consumeOnce: cl.ConsumeOnce}
	}
	return stdoutSink{w: std.Out}
}

// stdoutSink writes the clip's bytes straight to standard output: the raw bytes of a text or
// file clip, or the raw tar of an archive bound for a pipe or redirect. A truncated read
// surfaces as the copy error from the completion-checked body; some bytes may already have
// reached output before the truncation is known, which is inherent to streaming to a stream
// that cannot be unwound.
type stdoutSink struct{ w io.Writer }

func (s stdoutSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	_, err := io.Copy(s.w, r)
	return buffErr(err)
}

// fileSink writes a text or file clip's bytes to a -o target. If the target is an existing
// directory the clip is saved inside it under its remembered filename; otherwise the target
// names the file to write, clobbering an existing one the way a shell redirect does.
type fileSink struct{ target string }

// Write resolves the target and writes the bytes. The directory case needs a remembered
// filename and saves under it through openInDir, the shared filename boundary that re-validates
// the peer-supplied name and confines the write to the directory. The plain-file case writes the
// user's own literal path directly, which is their choice to make, exactly as a redirect is.
func (s fileSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	if fi, err := os.Stat(s.target); err == nil && fi.IsDir() {
		if m.Filename == "" {
			return fmt.Errorf("buff: clip has no filename; specify -o <path>")
		}
		f, err := openInDir(s.target, m.Filename, false) // clobber, like a shell redirect
		if err != nil {
			return buffErr(err)
		}
		return copyClose(f, r)
	}
	f, err := os.Create(s.target)
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	return copyClose(f, r)
}

// newDirSink extracts an archive into a fresh directory named for the slot, in the working
// directory — the behaviour for an archive pasted at a terminal. It is always the atomic
// whole-archive form, so a pre-existing directory of that name is refused rather than merged
// into: the terminal default did not ask to merge, and refusing avoids silently mixing a new
// tree into an old one.
type newDirSink struct{ name string }

func (s newDirSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	root, err := os.OpenRoot(".")
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	defer root.Close()
	return buffErr(archive.ExtractNew(ctx, root, s.name, r, archive.ExtractOpts{}))
}

// extractSink extracts an archive into an explicit -o target. An existing directory is
// merged into, with the archiver's per-entry no-clobber as the safety net; an absent target
// is published atomically as a new directory, which requires its parent to exist; a target
// that is an existing file is an error, since an archive needs a directory. Merging into an
// existing directory cannot be atomic, so a failed merge may leave some entries behind — the
// one weaker guarantee, accepted because the user named that directory explicitly.
type extractSink struct{ target string }

func (s extractSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	target := filepath.Clean(s.target)
	fi, err := os.Stat(target)
	switch {
	case err == nil && fi.IsDir():
		root, err := os.OpenRoot(target)
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer root.Close()
		return buffErr(archive.Extract(ctx, root, r, archive.ExtractOpts{}))
	case err == nil:
		return fmt.Errorf("buff: -o %s is a file; an archive extracts into a directory", s.target)
	case errors.Is(err, os.ErrNotExist):
		parent, err := os.OpenRoot(filepath.Dir(target))
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer parent.Close()
		return buffErr(archive.ExtractNew(ctx, parent, filepath.Base(target), r, archive.ExtractOpts{}))
	default:
		return fmt.Errorf("buff: %w", err) // a real stat failure (permission) is neither "directory" nor "free"
	}
}

// terminalSink is the disposition for a finalized file or blob pasted at a terminal with no -o:
// the one sink that reads its bytes to route them. It peeks the leading bytes and shows text on
// the terminal or saves binary to a file in the working directory, so a human never has an
// unreadable clip dumped at them — the same symmetry the archive case already keeps, where a tree
// auto-extracts rather than spilling raw tar. chooseSink reaches it in exactly one cell and only
// for a finalized clip, so the peeked bytes are all present and the peek can never stall on a
// writer; a still-live clip streams raw instead and is never peeked.
//
// It alone among the sinks carries errw, because it alone narrates its choice: saving binary, or
// diverting a salvage, moves the bytes off the user's terminal onto disk, which they must be told.
// consumeOnce is the salvage hinge — the server spends a consume-once delivery at open, before any
// byte ships, so once a save fails the delivery is already gone; rather than lose it, the bytes go
// raw to stdout.
type terminalSink struct {
	out, errw   io.Writer
	slot        string
	consumeOnce bool
}

func (s terminalSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	br := bufio.NewReaderSize(r, peekWindow)
	// Peek's error is deliberately dropped. A clip shorter than the window yields io.EOF with the
	// whole clip as the prefix, which classifies fine; a torn or failing read resurfaces on the
	// copy below, where bufio replays the buffered bytes and then re-returns the error — there the
	// tornReader beneath records the truncation and the paste flow turns it into exit 7.
	prefix, _ := br.Peek(peekWindow)
	if isText(prefix) {
		_, err := io.Copy(s.out, br) // show, including an empty clip, which shows nothing
		return buffErr(err)
	}
	name := m.Filename
	if name == "" {
		name = path.Base(s.slot) // a blob has no remembered name; its slot is the best buff has
	}
	f, err := openInDir(".", name, true) // save, no-clobber
	if err != nil {
		if s.consumeOnce {
			// The delivery is already spent server-side; never lose it to a setup failure.
			fmt.Fprintf(s.errw, "buff: could not save %q (%v); writing raw to stdout to keep the consume-once delivery\n", name, err)
			_, cerr := io.Copy(s.out, br)
			return buffErr(cerr)
		}
		return buffErr(err) // replaceable: surface it — a collision is exit 6 via os.ErrExist
	}
	fmt.Fprintf(s.errw, "buff: clip is binary; saved to ./%s\n", name)
	return copyClose(f, br) // a mid-copy failure leaves a partial file and the error, with no rewind
}

// openInDir opens name for writing inside dir, confined to an os.Root and re-validating name —
// the receiving half of the filename boundary. The name we are about to write to a consumer's
// disk arrived over the wire from a peer that may be hostile, foreign, or buggy, so it is never
// trusted on the peer's word: "../escape" is rejected here, and the os.Root makes it unwriteable
// outside dir regardless. excl chooses no-clobber — O_EXCL refuses an existing file; without it
// the open truncates one, the way a shell redirect does.
//
// It is the setup half of a directory save, deliberately apart from the byte copy (copyClose),
// so a caller can tell a save that never began — a rejected name, an unwritable dir, a no-clobber
// collision — from one that failed mid-write. That line is the pre-commit boundary a consume-once
// paste needs in order to know it can still salvage a delivery the server has already spent.
//
// The root is closed before the file is returned: an os.Root confines name resolution, which is
// finished once the file is open, and an open fd is an independent kernel object that outlives the
// directory handle it was opened through. The error is returned unwrapped — the caller owns the
// user-facing line and adds the buff: marker where it surfaces, so a cause folded into a larger
// diagnostic carries no stray prefix of its own.
func openInDir(dir, name string, excl bool) (*os.File, error) {
	if err := clip.ValidFilename(name); err != nil {
		return nil, fmt.Errorf("refusing unsafe filename %q: %w", name, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	flag := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if excl {
		flag = os.O_RDWR | os.O_CREATE | os.O_EXCL
	}
	f, err := root.OpenFile(name, flag, 0o644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// copyClose streams r into f and then closes f, returning the first error. The copy error
// wins when there is one — it is the truncation or read failure the caller reports — and a
// close error surfaces only on an otherwise-clean write, where it means the bytes may not
// have reached the disk.
func copyClose(f *os.File, r io.Reader) error {
	_, cerr := io.Copy(f, r)
	if err := f.Close(); cerr == nil {
		cerr = err
	}
	return buffErr(cerr)
}
