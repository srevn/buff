// Package internal pins, as compiled code, the Go-toolchain features buff's
// confined-filesystem core cannot build without: the *os.Root method surface, the
// (*os.File).ReadDir that startup recovery enumerates with, and the os.OpenRoot
// constructor the whole confinement model starts from. Encoding each assumption here
// means a future toolchain that drops or changes one of them fails this package's
// build loudly and immediately — with a precise diagnostic naming the offending
// method — instead of surfacing as a baffling error deep in the storage core.
//
// The package has zero runtime: nothing imports it, and Go's internal-package rule
// makes it unimportable from outside this module. It exists only to be compiled, as
// `go build ./...` does.
//
// (The disk store's durable commit needs no constant pinned here: its fsync is
// (*os.File).Sync(), which on darwin issues F_FULLFSYNC inside the Go runtime —
// no syscall and no platform-specific code for buff to guard.)
package internal

import "os"

// rootAPI is the subset of *os.Root that buff's storage core and its confined
// archive extraction are unbuildable without. Every file the server touches is
// reached through an *os.Root anchored at the data directory — that confinement
// makes path traversal impossible by construction — so the availability of each
// method below is load-bearing.
//
// Stating the requirement as an interface, rather than calling the methods loosely,
// makes *os.Root prove at compile time that it satisfies the whole surface, and
// yields a precise diagnostic — the exact method whose name or signature changed —
// if a future toolchain regresses one. It pins the return types too, not just names.
//
// Only methods the design actually uses are listed. *os.Root also offers Link,
// ReadFile and WriteFile, but buff deliberately uses none of them: it rejects
// hardlinks, and writes a clip's metadata record by streaming to a temp file,
// fsyncing, then renaming — never in a single WriteFile — so pinning them would
// constrain the toolchain for features that do not exist.
type rootAPI interface {
	// A clip's byte log: the append fd it is written through, the read fds that
	// servers and followers read it through, and the directory fds opened only to
	// fsync a rename so the publish is durable.
	Open(string) (*os.File, error)
	OpenFile(string, int, os.FileMode) (*os.File, error)
	Create(string) (*os.File, error)

	// Mkdir creates a generation's directory directly under clips/, named by its id;
	// MkdirAll is the confined archive extractor materialising an entry's missing
	// parent directories during an untar.
	Mkdir(string, os.FileMode) error
	MkdirAll(string, os.FileMode) error

	// Rename is the single atomic publish that makes a finished clip durable and
	// visible, and the move that quarantines a startup leftover we cannot interpret.
	// RemoveAll is every recursive reclamation path: aborting a half-written clip,
	// reclaiming a superseded one, deleting on request, and dropping leftovers found
	// at startup — each takes a generation's whole directory, since a generation is one
	// directory named by its id with nothing else to prune around it.
	Rename(string, string) error
	RemoveAll(string) error

	// Startup recovery and quota accounting. Stat re-derives a clip's size to catch
	// a write truncated by a crash and to recompute disk usage; Lstat and Readlink
	// inspect leftover entries without following links; Symlink builds the hostile
	// inputs the archive extractor is tested against (and backs a possible future
	// symlink opt-in).
	Stat(string) (os.FileInfo, error)
	Lstat(string) (os.FileInfo, error)
	Symlink(string, string) error
	Readlink(string) (string, error)

	// A nested, separately confined root per archive extraction, so untarring can
	// never write outside the directory it was handed, even if an entry tries to.
	OpenRoot(string) (*os.Root, error)
}

// dirReader is the directory enumeration startup recovery walks clips/ with. os.Root has
// no ReadDir of its own, so recovery opens a directory through the root and reads its
// entries as a file — (*os.File).ReadDir — separating the per-generation <genid>/
// directories from any stray file beside them. Pinning it here means a toolchain that
// drops or changes ReadDir fails this build, not the recovery walk at runtime.
type dirReader interface {
	ReadDir(int) ([]os.DirEntry, error)
}

var (
	// *os.Root must satisfy the entire surface the core depends on.
	_ rootAPI = (*os.Root)(nil)
	// The directory handle os.Root hands back must support the recovery enumeration.
	_ dirReader = (*os.File)(nil)
	// The server opens its data directory through this package-level constructor;
	// pin it too, since the whole confinement model starts there.
	_ func(string) (*os.Root, error) = os.OpenRoot
)
