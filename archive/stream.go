package archive

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"
)

// Stream writes a deterministic tar of roots into w and returns the first error it meets.
// It archives regular files and directories only; a symlink, hardlink, device, FIFO or
// socket is skipped and reported through opts.OnSkip rather than followed, so the stream
// can never escape the roots or loop. Each entry's name is its root's basename followed by
// the entry's path within that root; ownership and access/change times are dropped, the
// mode is reduced to its permission bits, the modification time is kept, and the PAX format
// is fixed — so the same tree yields the same bytes, on any machine and regardless of the
// order the roots are given.
//
// Stream writes the tar trailer but never closes w: the caller owns w. The CLI drives
// Stream from a goroutine that does pw.CloseWithError(Stream(...)) on an io.Pipe, so a
// mid-tar read error closes the pipe with that error and the upload aborts. On any error
// Stream returns immediately WITHOUT writing the trailer, leaving a valid tar prefix that
// ends abruptly — the truncation signal the consumer needs.
//
// An archive that resolves to zero entries — every root a skipped special file — is refused
// with ErrEmptyArchive rather than emitted as a bare trailer, since a tar of nothing is not a
// meaningful clip. A real empty directory is one entry, not zero, so it is unaffected.
func Stream(ctx context.Context, roots []string, w io.Writer, opts StreamOpts) error {
	rs, err := normRoots(roots)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(w)
	n := 0
	for _, r := range rs {
		written, err := streamRoot(ctx, tw, r.path, r.base, opts)
		if err != nil {
			return err // do not Close tw: the caller owns w and will CloseWithError(err)
		}
		n += written
	}
	if n == 0 {
		// Every root resolved to a skipped special file. Refuse it like the error paths above —
		// return WITHOUT tw.Close, so no trailer is written and the pipe closes with this error
		// rather than ending cleanly and committing an empty clip.
		return ErrEmptyArchive
	}
	return tw.Close() // flush the trailer into w; never close w itself
}

// root pairs a cleaned source path with the basename its entries are named under.
type root struct {
	path string
	base string
}

// normRoots cleans, de-duplicates and orders the source roots so the byte stream never
// depends on the order they were given. Each root is path-cleaned, exact duplicates are
// dropped, and the remainder are sorted by basename. A root whose basename cannot name its
// entries — ".", "..", or a filesystem root — is refused with ErrUnusableRoot (resolving
// such a root to a concrete directory is a CLI concern); two distinct roots that share a
// basename, which would collide under one name in the tar, with ErrDuplicateRoot; and an
// empty list with ErrNoRoots.
func normRoots(roots []string) ([]root, error) {
	if len(roots) == 0 {
		return nil, ErrNoRoots
	}
	seen := make(map[string]bool, len(roots))     // cleaned path already included
	byBase := make(map[string]string, len(roots)) // basename -> the cleaned path that owns it
	out := make([]root, 0, len(roots))
	for _, r := range roots {
		clean := filepath.Clean(r)
		if seen[clean] {
			continue
		}
		base := filepath.Base(clean)
		if base == "." || base == ".." || base == string(os.PathSeparator) {
			return nil, ErrUnusableRoot
		}
		if prev, ok := byBase[base]; ok && prev != clean {
			return nil, ErrDuplicateRoot
		}
		seen[clean] = true
		byBase[base] = clean
		out = append(out, root{path: clean, base: base})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].base < out[j].base })
	return out, nil
}

// streamRoot walks one root and writes its entries to tw, returning the number of entries
// written. It descends real directories only — WalkDir never follows a symlink — and reports
// anything that is neither a regular file nor a directory through opts.OnSkip without following
// or descending it. A walk error (a missing or unreadable root or entry) is returned as a hard
// error, distinct from a skipped type: a path you named that cannot be read is a failure, not
// something to quietly omit. ctx is checked once per entry. The count excludes skipped entries,
// so Stream can sum it across roots and refuse an archive that came to nothing.
func streamRoot(ctx context.Context, tw *tar.Writer, rootPath, base string, opts StreamOpts) (int, error) {
	n := 0
	err := filepath.WalkDir(rootPath, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		name, err := entryName(base, rootPath, p)
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			if err := streamDir(tw, name, d); err != nil {
				return err
			}
			n++
		case nonRegular(d.Type()):
			// A symlink (even to a directory), device, FIFO or socket: reported and skipped,
			// crucially never followed or descended, and not counted as an archived entry.
			if opts.OnSkip != nil {
				opts.OnSkip(name, d.Type())
			}
		default:
			if err := streamReg(tw, name, p, d); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// entryName builds the slash-separated tar name for the file at OS path p within root,
// whose entries are named under base: base for the root itself, and base joined with the
// file's path within the root otherwise. Tar names are always slash-separated regardless of
// the host OS, so the within-root path is converted with ToSlash before joining.
func entryName(base, root, p string) (string, error) {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return "", err
	}
	return path.Join(base, filepath.ToSlash(rel)), nil
}

// nonRegular reports whether a directory entry of mode m is neither a regular file nor a
// directory — a symlink, device, named pipe, socket, or other special file. Stream skips
// such entries. It is written against the mode bits so a test can exercise it with synthetic
// modes: creating a real device or socket needs privileges, but the classification does not.
func nonRegular(m fs.FileMode) bool {
	return !m.IsRegular() && !m.IsDir()
}

// streamDir writes a directory entry: a sanitized header with a trailing slash and no body,
// so the directory — including an empty one — and its permissions survive the round trip.
func streamDir(tw *tar.Writer, name string, d fs.DirEntry) error {
	h, err := header(name, d)
	if err != nil {
		return err
	}
	return tw.WriteHeader(h)
}

// streamReg writes a regular-file entry: its sanitized header followed by exactly header.Size
// bytes of content. The body is copied with copyEntry, bounded to the size in the header, so a
// file that grows between the stat and the read yields a consistent prefix snapshot rather than
// a corrupt entry, and one that shrinks fails with io.ErrUnexpectedEOF rather than under-filling
// the entry. The file is opened by ordinary path: Stream reads the caller's own tree, which is
// not a security boundary.
func streamReg(tw *tar.Writer, name, p string, d fs.DirEntry) error {
	h, err := header(name, d)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return copyEntry(tw, f, h.Size)
}

// copyEntry copies exactly size bytes from src into tw — the body of one regular-file entry,
// whose header already declared size bytes will follow. io.CopyN reports a source that ends
// short (the file shrank between the stat that set size and this read) as a plain io.EOF, which
// is benign to CopyN but dangerous here: Stream's only error channel is the value it returns,
// which the caller feeds to pipeWriter.CloseWithError — and CloseWithError(io.EOF) is a CLEAN
// close, indistinguishable from a finished archive. The server would then read the truncated
// tar to a normal end and commit it as complete instead of aborting. Translating the short read
// to io.ErrUnexpectedEOF keeps it a real error, so the pipe closes with an error and the upload
// aborts. A source longer than size is not an error: CopyN stops at size, a consistent prefix.
func copyEntry(tw io.Writer, src io.Reader, size int64) error {
	if _, err := io.CopyN(tw, src, size); err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

// header builds the sanitized tar header for the entry at name from its directory entry.
// tar.FileInfoHeader, on a Unix file, copies the owner ids, owner names and access and
// change times out of the OS stat — all of which buff drops: ownership, because the archive
// is restored as whoever extracts it; and the access and change times, because they are not
// stable across reads (a read updates atime) and would otherwise make the byte stream
// non-deterministic. The modification time is kept, the mode is reduced to its permission
// bits (masking setuid/setgid/sticky), and the PAX format is fixed so long or Unicode names
// and large files encode reproducibly.
func header(name string, d fs.DirEntry) (*tar.Header, error) {
	fi, err := d.Info()
	if err != nil {
		return nil, err
	}
	h, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return nil, err
	}
	h.Name = name
	if d.IsDir() {
		h.Name += "/"
	}
	h.Mode &= 0o777
	h.Uid, h.Gid = 0, 0
	h.Uname, h.Gname = "", ""
	h.AccessTime, h.ChangeTime = time.Time{}, time.Time{}
	h.Format = tar.FormatPAX
	return h, nil
}
