package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

// chooseSink resolves where a paste's bytes go, from the clip's kind, whether output is a
// terminal, and whether -o was given. -o is the explicit rendered-output target and wins
// over the terminal heuristic. Without it, an archive at a terminal is extracted into a new
// directory named for the slot, while an archive to a pipe or redirect — and any text or
// file clip — is written raw, so that piping a tar to tar and redirecting it to a file both
// behave the way the shell leads one to expect.
func chooseSink(k clip.Kind, inv invocation, std IO) Sink {
	if inv.outputSet {
		if k == clip.KindArchive {
			return extractSink{target: inv.output}
		}
		return fileSink{target: inv.output}
	}
	if k == clip.KindArchive && std.OutIsTTY {
		return newDirSink{name: inv.slot}
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
	return err
}

// fileSink writes a text or file clip's bytes to a -o target. If the target is an existing
// directory the clip is saved inside it under its remembered filename; otherwise the target
// names the file to write, clobbering an existing one the way a shell redirect does.
type fileSink struct{ target string }

// Write resolves the target and writes the bytes. The directory case needs a remembered
// filename and re-validates it: the name came from the server, which decoded but did not
// vet it, so a hostile filename is rejected here and the write is confined through a root
// rooted at the directory — the receiving half of the filename boundary, so a name like
// "../escape" cannot write outside the chosen directory. The plain-file case writes the
// user's own literal path directly, which is their choice to make, exactly as a redirect is.
func (s fileSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	if fi, err := os.Stat(s.target); err == nil && fi.IsDir() {
		if m.Filename == "" {
			return fmt.Errorf("buff: clip has no filename; specify -o <path>")
		}
		if err := clip.ValidFilename(m.Filename); err != nil {
			return fmt.Errorf("buff: server sent an unsafe filename %q: %w", m.Filename, err)
		}
		root, err := os.OpenRoot(s.target)
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer root.Close()
		f, err := root.Create(m.Filename) // clobbers like a redirect; --no-clobber would be additive
		if err != nil {
			return fmt.Errorf("buff: %w", err)
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
	return archive.ExtractNew(ctx, root, s.name, r, archive.ExtractOpts{})
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
		return archive.Extract(ctx, root, r, archive.ExtractOpts{})
	case err == nil:
		return fmt.Errorf("buff: -o %s is a file; an archive extracts into a directory", s.target)
	case errors.Is(err, os.ErrNotExist):
		parent, err := os.OpenRoot(filepath.Dir(target))
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer parent.Close()
		return archive.ExtractNew(ctx, parent, filepath.Base(target), r, archive.ExtractOpts{})
	default:
		return fmt.Errorf("buff: %w", err) // a real stat failure (permission) is neither "directory" nor "free"
	}
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
	return cerr
}
