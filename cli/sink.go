package cli

import (
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

// chooseSink resolves where a paste's bytes go, drawing only on the clip's kind, whether output is a
// terminal, and the -o flag — never on the bytes themselves. The kind is the gesture that made the
// clip: a piped stream is text, a single file is file, a tree is archive. Routing by kind restores
// that gesture without inspecting content — at a terminal buff saves a file, extracts an archive, and
// shows text; to a pipe it is cat; -o forces the destination. This is the last place the client ever
// branched on content — the peek that once sniffed text from binary is gone — so the whole relay is
// now content-blind, the same stance the server holds.
//
// -o is the explicit target and wins. -o - is the Unix spelling of "raw bytes to stdout" for any
// kind, which also keeps -o - from being read as a file literally named "-". Without -o, a terminal
// restores the gesture by kind and a pipe or redirect always gets raw bytes, so piping a tar to tar
// and redirecting to a file behave the way the shell leads one to expect. Routing never reads
// cl.Finalized: a still-being-written file saves exactly as a finalized one (a live archive already
// extracts ungated), so a clip's disposition cannot change as it finalizes. The trade the gesture
// model makes is that the producer chooses the gesture and the consumer bears it — a binary stream
// piped in as a text clip will garble a terminal on paste, recovered with -o - or a pipe.
func chooseSink(cl clip.Clip, inv invocation, std IO) Sink {
	if inv.outputSet {
		if inv.output == "-" {
			return stdoutSink{w: std.Out} // explicit raw stdout, any kind
		}
		if cl.Meta.Kind == clip.KindArchive {
			return extractSink{target: inv.output, errw: std.Err}
		}
		return fileSink{target: inv.output, errw: std.Err}
	}
	if std.OutIsTTY {
		switch cl.Meta.Kind {
		case clip.KindArchive:
			// ExtractNew publishes one new directory and requires its name to be a single path
			// component, so reduce the slot to its last component. A no-op while names are single-
			// component; what keeps a future hierarchical slot extracting into its leaf rather than
			// tripping ExtractNew's single-component guard. path (not filepath) because a slot is the
			// logical "/"-namespace, not an OS path.
			return newDirSink{name: path.Base(inv.slot), errw: std.Err}
		case clip.KindFile:
			return saveSink{out: std.Out, errw: std.Err, slot: inv.slot, consumeOnce: cl.ConsumeOnce}
		}
		// A text clip — and any kind a foreign peer left unset or unknown — falls through: its bytes
		// are meant for eyes, shown raw on the terminal rather than guessed at or written to a file.
	}
	return stdoutSink{w: std.Out}
}

// stdoutSink writes the clip's bytes straight to standard output: the raw bytes of a text or file
// clip, or the raw tar of an archive bound for a pipe or redirect, and a text clip shown at a
// terminal. A truncated read surfaces as the copy error from the completion-checked body; some bytes
// may already have reached output before the truncation is known, which is inherent to streaming to a
// stream that cannot be unwound.
type stdoutSink struct{ w io.Writer }

func (s stdoutSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	_, err := io.Copy(s.w, r)
	return buffErr(err)
}

// fileSink writes a text or file clip's bytes to a -o target. If the target is an existing
// directory the clip is saved inside it under its remembered filename — a name drawn from clip
// metadata, not from the command, so the landing path is narrated (through narrateSave) exactly as a
// terminal save is; otherwise the target names the file to write, clobbering an existing one the way
// a shell redirect does, and staying silent because the user spelled that whole path out. errw
// carries the directory arm's one notice and is unused by the file arm.
type fileSink struct {
	target string
	errw   io.Writer
}

// Write resolves the target and writes the bytes. The directory case needs a remembered
// filename and saves under it through openInDir, the shared filename boundary that re-validates
// the peer-supplied name and confines the write to the directory. The plain-file case writes the
// user's own literal path directly, clobbering like a redirect — their choice to make. Both cases
// restore the clip's executable bit, so a copied binary lands runnable rather than inert.
func (s fileSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	if fi, err := os.Stat(s.target); err == nil && fi.IsDir() {
		if m.Filename == "" {
			return fmt.Errorf("buff: clip has no filename; specify -o <path>")
		}
		f, err := openInDir(s.target, m.Filename, false, m.Executable) // clobber, like a shell redirect
		if err != nil {
			return buffErr(err)
		}
		// openInDir succeeded, so m.Filename was a valid single component: the joined path is the real
		// destination, and naming it now — before the copy — mirrors saveSink, the one other sink that
		// lands a metadata-chosen filename the user never typed.
		narrateSave(s.errw, filepath.Join(s.target, m.Filename))
		return copyClose(f, r)
	}
	f, err := os.Create(s.target)
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	if m.Executable {
		// The user named this literal path, so restoring the clip's run bit is the cp-like behaviour
		// they asked for. This arm is unconfined by design — the user's own path, not a peer-supplied
		// name — so it applies the bit straight to the os.Create fd rather than through openInDir.
		if err := makeExecutable(f); err != nil {
			f.Close()
			return buffErr(err)
		}
	}
	return copyClose(f, r)
}

// newDirSink extracts an archive into a fresh directory named for the slot, in the working
// directory — the behaviour for an archive pasted at a terminal. It is always the atomic
// whole-archive form, so a pre-existing directory of that name is refused rather than merged
// into: the terminal default did not ask to merge, and refusing avoids silently mixing a new
// tree into an old one. On success it narrates the landing directory, the archive counterpart
// of saveSink's file note: a paste that lands a whole tree on disk is exactly the disk landing
// worth confirming, and errw carries that one line.
type newDirSink struct {
	name string
	errw io.Writer
}

func (s newDirSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	root, err := os.OpenRoot(".")
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	defer root.Close()
	if err := archive.ExtractNew(ctx, root, s.name, r, archive.ExtractOpts{}); err != nil {
		return buffErr(err) // a torn or colliding extract rolls back and is never narrated
	}
	narrateExtract(s.errw, "./"+s.name)
	return nil
}

// extractSink extracts an archive into an explicit -o target. An existing directory is
// merged into, with the archiver's per-entry no-clobber as the safety net; an absent target
// is published atomically as a new directory, which requires its parent to exist; a target
// that is an existing file is an error, since an archive needs a directory. Merging into an
// existing directory cannot be atomic, so a failed merge may leave some entries behind — the
// one weaker guarantee, accepted because the user named that directory explicitly. Either way
// a clean extraction narrates the landing directory through errw, the disk-landing confirmation
// the file sinks already give; only success reaches that line, so a failed merge's partial
// state is reported as the error it is, never as a landing.
type extractSink struct {
	target string
	errw   io.Writer
}

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
		if err := archive.Extract(ctx, root, r, archive.ExtractOpts{}); err != nil {
			return buffErr(err)
		}
	case err == nil:
		return fmt.Errorf("buff: -o %s is a file; an archive extracts into a directory", s.target)
	case errors.Is(err, os.ErrNotExist):
		parent, err := os.OpenRoot(filepath.Dir(target))
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer parent.Close()
		if err := archive.ExtractNew(ctx, parent, filepath.Base(target), r, archive.ExtractOpts{}); err != nil {
			return buffErr(err)
		}
	default:
		return fmt.Errorf("buff: %w", err) // a real stat failure (permission) is neither "directory" nor "free"
	}
	narrateExtract(s.errw, target)
	return nil
}

// saveSink is the disposition for a file clip pasted at a terminal with no -o: it saves the clip into
// the working directory under its remembered filename, refusing to clobber an existing file. It
// touches the terminal only to narrate the save (through narrateSave), or — for a consume-once clip
// whose save cannot begin — to salvage the delivery raw to stdout.
//
// It narrates because the bytes land on disk where the terminal cannot show them: buff @doc saves
// ./report.pdf rather than printing it, so confirming the landing — and the metadata-drawn name the
// user never typed — is worth a line. Every disk-landing sink does the same now (fileSink's directory
// arm for a file, newDirSink and extractSink for a tree); only an -o <file> the user spelled out in
// full, and bytes the terminal or a pipe already shows, stay silent. The salvage, though, is
// saveSink's alone: it is the only such save that refuses to clobber, so the only one whose open can
// fail with a consume-once delivery already spent — the server claims that delivery at open, before
// any byte ships, so once a no-clobber save collides the bytes are gone, and rather than lose them the
// untouched stream goes raw to stdout. fileSink's directory arm clobbers, so it never needs to salvage.
type saveSink struct {
	out, errw   io.Writer
	slot        string
	consumeOnce bool
}

// Write saves the clip's bytes to a no-clobber file in the working directory. The name is the
// remembered filename, falling back to the slot's last component for a clip that carries none — now
// only a malformed peer, since buff's own file copies always remember a basename and a text clip
// never reaches this sink. A failed open before any byte is read is where a consume-once delivery is
// salvaged: nothing has been consumed from r, so streaming it raw to stdout is a complete delivery
// that keeps a spent delivery from being lost; a replaceable clip instead surfaces the collision as
// os.ErrExist, which scores exit 6.
func (s saveSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	name := m.Filename
	if name == "" {
		name = path.Base(s.slot)
	}
	f, err := openInDir(".", name, true, m.Executable) // save, no-clobber
	if err != nil {
		if s.consumeOnce {
			// The delivery is already spent server-side; never lose it to a setup failure.
			fmt.Fprintf(s.errw, "buff: could not save %q (%v); writing raw to stdout to keep the consume-once delivery\n", name, err)
			_, cerr := io.Copy(s.out, r)
			return buffErr(cerr)
		}
		return buffErr(err) // replaceable: surface it — a collision is exit 6 via os.ErrExist
	}
	narrateSave(s.errw, "./"+name)
	return copyClose(f, r) // a mid-copy failure leaves a partial file and the error, with no rewind
}

// narrateSave and narrateExtract are the disk-landing notices: a paste that writes bytes somewhere the
// terminal cannot show them says where they went. The rule across every sink is one line — confirm a
// landing unless the user spelled the exact final path (-o <file>) or the bytes are already visible
// (a terminal show, a pipe, -o -). Both write to errw, never stdout, so a paste whose stdout is
// redirected keeps the notice out of its data, and each caller passes its own honest path so the
// shared piece is the wording, not the path.
//
// narrateSave reports a file landing under a name buff resolved from clip metadata — saveSink's cwd
// ./name or fileSink's -o <dir> joined target, a name the user never typed. Its tense is deliberate:
// a streamed, possibly still-live, non-atomic write can tear after this line, so it promises the
// attempt, not a finished file.
func narrateSave(errw io.Writer, path string) {
	fmt.Fprintf(errw, "buff: saving to %s\n", path)
}

// narrateExtract reports a tree landing in a directory buff placed on disk — newDirSink's ./slot or
// extractSink's -o target. Its tense is the opposite of narrateSave's and just as deliberate: an
// extraction is atomic (it builds a temp tree and renames it into place, or rolls back leaving
// nothing), so the caller reaches this line only once the directory is wholly there, and a past-tense
// line can promise the result rather than the attempt. The trailing slash marks the landing a directory.
func narrateExtract(errw io.Writer, dir string) {
	fmt.Fprintf(errw, "buff: extracted to %s/\n", dir)
}

// openInDir opens name for writing inside dir, confined to an os.Root and re-validating name —
// the receiving half of the filename boundary. The name we are about to write to a consumer's
// disk arrived over the wire from a peer that may be hostile, foreign, or buggy, so it is never
// trusted on the peer's word: "../escape" is rejected here, and the os.Root makes it unwriteable
// outside dir regardless. excl chooses no-clobber — O_EXCL refuses an existing file; without it
// the open truncates one, the way a shell redirect does. executable layers the clip's run bits
// onto the freshly-opened file with fchmod (see makeExecutable), so a restored binary stays runnable.
//
// It is the setup half of a directory save, deliberately apart from the byte copy (copyClose),
// so a caller can tell a save that never began — a rejected name, an unwritable dir, a no-clobber
// collision, a failed chmod — from one that failed mid-write. Folding the chmod in here, before any
// byte is copied, keeps every such case on the pre-commit boundary a consume-once paste needs in
// order to know it can still salvage a delivery the server has already spent.
//
// The root is closed before the file is returned: an os.Root confines name resolution, which is
// finished once the file is open, and an open fd is an independent kernel object that outlives the
// directory handle it was opened through. The error is returned unwrapped — the caller owns the
// user-facing line and adds the buff: marker where it surfaces, so a cause folded into a larger
// diagnostic carries no stray prefix of its own.
func openInDir(dir, name string, excl, executable bool) (*os.File, error) {
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
	// The 0o644 base above stays the right non-executable default; an executable clip layers the run
	// bits on with fchmod rather than a wider create perm. fchmod, not the create mode, is what closes
	// the clobber gap: O_TRUNC over an existing file keeps that file's old mode and ignores the create
	// perm, so a create-perm approach would silently leave a clobbered file non-runnable.
	if executable {
		if err := makeExecutable(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

// makeExecutable mirrors `chmod +x` on an already-open file: it guarantees owner-exec and grants
// group/other exec only where the matching read bit is already present. Deriving the exec bits from
// the file's on-disk mode — which the kernel already filtered through the consumer's umask when the
// file was created — is what honours a tight umask without buff ever reading umask itself: no
// syscall, portable, a clean no-op of effect where a platform has no such bits. It runs fchmod on
// the fd, so it stays confined to the file already opened and needs no second path lookup, and works
// on the unconfined os.Create fd of a plain -o path exactly as on an os.Root fd.
func makeExecutable(f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	m := fi.Mode().Perm()
	return f.Chmod(m | 0o100 | ((m & 0o444) >> 2))
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
