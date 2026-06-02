package archive

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

// Extract untars r into dst, confined to dst by construction: every file and directory it
// creates is made through dst, an *os.Root the caller owns, so nothing can be written
// outside it even if an entry tries. It materializes regular files and directories only,
// and refuses every other entry type, any absolute or non-local path, and a name that
// already exists (no-clobber). The number of entries is capped (opts.MaxEntries, or a
// default) as a tar-bomb backstop, and ctx is checked between entries.
//
// Extract is the merge form, used for the "-o DIR" output mode: it writes straight into
// dst, so entries that succeed before a later one fails remain on disk. Surfacing that
// weaker, per-entry guarantee is the caller's job; ExtractNew is the whole-archive atomic
// form.
func Extract(ctx context.Context, dst *os.Root, r io.Reader, opts ExtractOpts) error {
	return extract(ctx, dst, r, norm(opts))
}

// ExtractNew atomically publishes the archive in r as a fresh directory named name under
// parent. It untars into a temporary sibling directory and, only on full success, renames
// that sibling onto name — so a reader of parent never sees a half-extracted tree, and any
// failure (a rejected entry, a write error, a cancelled ctx) leaves name absent and the
// temporary tree removed. name must be a single path component that does not already exist.
//
// This is the whole-archive atomic form, for pasting into a new directory. The rename is a
// same-directory move within parent, which the filesystem performs atomically. There is no
// fsync: this publishes to the caller's own filesystem, where atomicity means visibility,
// not crash durability — a crash mid-extraction may leave a tempPrefix sibling the user can
// delete.
func ExtractNew(ctx context.Context, parent *os.Root, name string, r io.Reader, opts ExtractOpts) error {
	if !singleComponent(name) {
		return ErrUnsafePath
	}
	switch _, err := parent.Lstat(name); {
	case err == nil:
		return ErrDestExists
	case !errors.Is(err, os.ErrNotExist):
		return err // a real stat failure (e.g. permission) is not "the name is free"
	}

	tmp, err := tempName()
	if err != nil {
		return err
	}
	if err := parent.Mkdir(tmp, 0o700); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			parent.RemoveAll(tmp) // discard a half-extraction on any failure; best effort
		}
	}()

	troot, err := parent.OpenRoot(tmp)
	if err != nil {
		return err
	}
	err = extract(ctx, troot, r, norm(opts))
	troot.Close() // release the fd before the rename
	if err != nil {
		return err
	}
	if err := parent.Rename(tmp, name); err != nil {
		return err
	}
	ok = true
	return nil
}

// extract is the confined untar shared by Extract and ExtractNew; o is assumed already
// normalized. It returns the first error it meets — a rejected entry, a malformed tar, or a
// write failure — having created whatever earlier entries succeeded. ExtractNew is what
// turns that into all-or-nothing.
func extract(ctx context.Context, root *os.Root, r io.Reader, o ExtractOpts) error {
	tr := tar.NewReader(r)
	for n := 0; ; n++ {
		if err := ctx.Err(); err != nil {
			return err // cancel between entries
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err // a malformed tar surfaces here; the fuzz target drives this path
		}
		if n >= o.MaxEntries {
			return ErrTooManyEntries
		}
		rel, err := safeName(hdr.Name)
		if err != nil {
			return entryErr(n, err) // absolute / ".." / NUL / "." — rejected before any write
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(rel, clampDir(hdr.Mode)); err != nil {
				return entryErr(n, err)
			}
		case tar.TypeReg:
			if err := extractReg(root, rel, hdr, tr); err != nil {
				return entryErr(n, err)
			}
		default:
			// symlink, hardlink, char/block device, FIFO, socket, sparse — the whole
			// traversal and abuse surface, rejected. (The reader folds the old TypeRegA
			// into TypeReg, so a regular file from any tar lands in the case above.)
			return entryErr(n, ErrUnsupportedEntry)
		}
	}
}

// extractReg extracts one regular-file entry. The file is created with O_EXCL, so an entry
// whose name already exists is an error, never an overwrite: O_EXCL — not root.Create,
// which carries O_TRUNC and would silently clobber — is the whole no-clobber guarantee, and
// the trap that, if missed, turns "name collision rejected" into "name collision
// overwritten." Missing parent directories are created first, since a tar need not list
// them. The body streams straight to the file, so no entry is ever held whole in memory.
func extractReg(root *os.Root, rel string, hdr *tar.Header, body io.Reader) error {
	if dir := filepath.Dir(rel); dir != "." {
		if err := root.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	f, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, clampFile(hdr.Mode))
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrExists
		}
		return err
	}
	if _, err := io.Copy(f, body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// safeName turns an untrusted tar entry name — always slash-separated — into a clean,
// local, OS-separated relative path, or returns ErrUnsafePath. It cleans with path.Clean
// first so the forms standard tar tools emit are accepted (a directory's trailing slash, a
// leading "./", a doubled "//", a safe interior "a/../b"), then localizes, which rejects an
// absolute path, a name that escapes with "..", and an embedded NUL.
//
// Cleaning before localizing is safe: path.Clean never removes a leading or escaping ".."
// from a relative path, so every escaping name still fails Localize. The one gap Clean
// leaves is "." and the empty name, which it reduces to "." — a name Localize would accept
// as the destination root itself — so those are rejected explicitly here.
func safeName(name string) (string, error) {
	c := path.Clean(name)
	if c == "." || c == "/" {
		return "", ErrUnsafePath
	}
	rel, err := filepath.Localize(c)
	if err != nil {
		return "", ErrUnsafePath
	}
	return rel, nil
}

// clampFile and clampDir reduce a tar entry's mode to the permission bits buff will
// restore. The setuid, setgid and sticky bits are masked off (& 0o777), so a hostile
// archive cannot plant a setuid binary, and the owner bits are forced on so the extracting
// user can always read a file it wrote (0o600) and read, write and descend a directory it
// created (0o700). uid and gid are never restored: extraction runs as the current user.
func clampFile(mode int64) os.FileMode { return os.FileMode(mode&0o777) | 0o600 }
func clampDir(mode int64) os.FileMode  { return os.FileMode(mode&0o777) | 0o700 }

// singleComponent reports whether name is a single, safe path component: non-empty, not
// "." or "..", and free of any path separator. ExtractNew publishes into exactly one fresh
// directory under parent and renames a sibling onto it, so a name with a separator (which
// would not be a sibling of the temp directory) or "." / ".." (which would not be a fresh
// child) is refused. parent is an *os.Root and would itself block an escape; this is the
// clear early check in front of that boundary, not the boundary itself.
func singleComponent(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for i := 0; i < len(name); i++ {
		if name[i] == '/' || name[i] == os.PathSeparator {
			return false
		}
	}
	return true
}

// tempName returns a fresh temporary-directory name for ExtractNew: the tempPrefix followed
// by 16 random bytes in hex, drawn from crypto/rand so two concurrent pastes into the same
// parent cannot collide. It errors only if the system random source fails.
func tempName() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return tempPrefix + hex.EncodeToString(b[:]), nil
}

// entryErr wraps err with the 1-based ordinal of the entry it concerns, so a diagnostic can
// point at "entry 7" without echoing the entry's name — which, in a hostile archive, could
// carry terminal escape sequences. The package's own rejections are bare sentinels with no
// name, but an incidental filesystem failure (an *os.PathError from a failed MkdirAll/OpenFile,
// or a write error) embeds the offending path, and Localize admits control characters into it —
// so a path-bearing error is reduced to its bare cause before wrapping, dropping the name while
// keeping the errno-level reason. The %w wrap keeps errors.Is working against the sentinels
// (which, not being PathErrors, pass through untouched).
func entryErr(n int, err error) error {
	if pe, ok := errors.AsType[*os.PathError](err); ok {
		err = pe.Err
	}
	return fmt.Errorf("archive: entry %d: %w", n+1, err)
}
