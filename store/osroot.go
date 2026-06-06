// This file is buff's build-time guard over the Go toolchain's confined-filesystem API — the slice
// of *os.Root, of the root-derived *os.File, and of the os.OpenRoot constructor that the module
// cannot build without. Each requirement is stated below as an interface *os.Root or *os.File
// must satisfy, or as a typed var, so a toolchain that drops a method or changes a signature fails
// THIS package's compile with a diagnostic naming the offending method — not a baffling error deep
// in the store, the archive extractor, or the client sink. The guard has zero runtime: its only
// product is a `go build` outcome and the accuracy of the contract it states — and gating the build
// on that check is the deliberate point, not an accident of where the file sits.
//
// Why it lives in store/ rather than a package of its own. The contract is module-wide — store
// is the heaviest consumer, but the archive extractor and the client output sink are first-class
// users too, as the citations below show — yet store has the one property the guard's whole worth
// depends on: it is in the import graph of every build that produces a binary (cmd/buff imports
// it transitively), so the assertion is compiled by `go build ./cmd/buff`, `go install`, and `make
// dist` alike. The guard's former home was a package imported by nobody, reached only by `go build
// ./...`; every actual binary was built without ever checking it — the one thing a build guard must
// not permit. store also already hosts buff's var-underscore conformance family (the medium and
// Reaper assertions), so the idiom is at home here. None of this makes the contract store-owned; it
// is the module's, parked where the binary's compiler is guaranteed to see it.
//
// One closed rule decides every line: pin a confined-filesystem method — on *os.Root, or on
// a *os.File obtained through it — iff buff calls it AND no io.* interface already covers
// its signature. A method io.* covers (Close→io.Closer, Read→io.Reader, Write→io.Writer,
// ReadAt→io.ReaderAt) is already constrained wherever buff passes the value as that interface,
// and is called directly besides, so a regression surfaces without help from here; re-pinning it
// would be noise. That rule — not the looser "every method we call" — is why (*os.Root).Close and
// *os.File's read and write methods are deliberately absent though buff calls them constantly, and
// it is what makes the pinned set provably complete rather than a list that merely happens to look
// right.
//
// What it does NOT guard: existence and signatures, never semantics. A toolchain that kept every
// signature here but broke os.Root's traversal confinement, the durable-commit atomicity, or the
// F_FULLFSYNC that Sync issues inside the runtime would pass this build untouched. Those properties
// are owned where they can actually be exercised — the archive fuzz target and the store's kill-
// between-every-IO recovery harness — so this guard stays a signature check and says so, lest a
// green build here be mistaken for a proof of confinement.

package store

import "os"

// rootAPI is the slice of *os.Root the module is unbuildable without. Every byte the server
// persists, every entry the archive extractor writes, and the file the client's directory output
// sink creates all reach the filesystem through an *os.Root anchored at a known directory — that
// confinement is what makes path traversal impossible by construction — so each method below is
// load-bearing. Stating the dependency as an interface makes *os.Root prove at compile time that
// it still satisfies the whole surface, and names the exact method, return types included, if a
// toolchain ever regresses one.
//
// Membership follows the file-level rule: used, and not covered by an io.* interface.
// (*os.Root).Close is therefore absent though buff calls it at every sink and at server teardown —
// it is io.Closer, and a signature regression would break each direct root.Close() besides. Create
// is absent though os.Root offers it and it is the obvious convenience: every confined create
// routes through OpenFile instead, for the flag and mode control noted there, while the unconfined
// plain -o path calls the package-level os.Create — a different symbol, outside this surface. Link,
// ReadFile and WriteFile are absent because buff uses none: it rejects hardlinks, and writes a
// record by streaming to a temp file then renaming, never in one WriteFile. Symlink and Readlink
// are likewise unused today; a future symlink-preservation opt-in would reach for root.Symlink and
// re-pin it here in one line.
type rootAPI interface {
	// The O_RDONLY opens: every follower's and recovery's read of a clip's data file, plus the
	// directory handles opened only to fsync a publish durable. (store: openGen's reader opener,
	// syncDir, checksumData; recovery: listDir, readMeta, and toRecovered's recovered-generation
	// opener.)
	Open(string) (*os.File, error)
	// Every confined create buff makes: a clip's append fd and its meta.json.tmp (store: openGen,
	// writeMeta), the output sink's directory save (cli: openInDir), and the extractor's regular-file
	// create (archive: extractReg). The flags and mode are the caller's, which is the point — O_EXCL
	// is the whole no-clobber guarantee a fresh save and the extractor rest on, and an explicit 0o644
	// base is what lets openInDir layer an exec bit on cleanly. That control is why every one uses
	// OpenFile and none uses Create, whose fixed O_TRUNC|0o666 can express neither.
	OpenFile(string, int, os.FileMode) (*os.File, error)

	// Mkdir makes a generation's own clips/<genid> directory and the shared clips//quarantine parents
	// (store: makeGenDir, mk), and the extractor's temporary directory (archive: ExtractNew). MkdirAll
	// is the extractor alone, materialising a directory entry and an entry's missing parents (archive:
	// extract, extractReg); the store never needs it, since a generation is one flat directory.
	Mkdir(string, os.FileMode) error
	MkdirAll(string, os.FileMode) error

	// Rename is every atomic move: the publish and the consume-once claim (store: commit), the
	// quarantine of an uninterpretable leftover (recovery: quarantine), and the extractor's temp→final
	// whole-archive publish (archive: ExtractNew). RemoveAll is every recursive reclamation: undoing
	// a half-made generation and dropping a superseded or deleted one (store: create, remove),
	// reclaiming crash garbage at boot (recovery: removeGenDir), and discarding a failed extraction
	// (archive: ExtractNew) — each takes a whole directory, the unit both a generation and a temp
	// tree are.
	Rename(string, string) error
	RemoveAll(string) error

	// Stat re-derives a finalized clip's size against its record and probes for a quarantine-name
	// collision (recovery: classify, quarantine); it follows links, which is correct for a data file
	// that is always a regular file. Lstat is the extractor's no-clobber check, before and after the
	// publish rename (archive: ExtractNew) — Lstat, not Stat, so a symlink planted at the destination
	// cannot disguise an occupied name as free. The two never swap: recovery stats its own files, the
	// extractor must not trust the bytes it was handed.
	Stat(string) (os.FileInfo, error)
	Lstat(string) (os.FileInfo, error)

	// A nested, separately confined root per extraction, so an untar cannot escape the directory it
	// was handed even if an entry tries (archive: ExtractNew). The package-level os.OpenRoot that
	// opens the first root is a different symbol, pinned separately below.
	OpenRoot(string) (*os.Root, error)
}

// fileAPI is the slice of root-derived *os.File methods buff invokes on the open fd itself — fstat,
// fchmod, fsync, and the directory read — never through a path-based os.Root method. The fd form
// is the one buff wants: it re-resolves no name, so a path swap cannot race it and it behaves the
// same on the unconfined fds buff also holds — a producer's source file read for its exec bit, a
// -o path's os.Create handle; and for Sync and ReadDir os.Root has no path form at all. The closed
// rule is rootAPI's — used, and not covered by an io.* interface — plus one qualifier: the handle
// must be root-derived, which is what puts the method on the confined surface this guard covers.
// One root-derived caller is enough to pin; that buff also fstats an unconfined source file or a
// -o fd does not exempt Stat. Read, Write, ReadAt and Close stay absent as io.Reader, io.Writer,
// io.ReaderAt and io.Closer.
type fileAPI interface {
	// Recovery enumerates clips/ by opening it through the root and reading it as a file, since
	// os.Root has no ReadDir of its own (recovery: listDir).
	ReadDir(int) ([]os.DirEntry, error)
	// The durable-commit primitive: the data-file flush (buffer: diskBacking.sync), the meta.json.tmp
	// flush, and the directory-entry flush that makes a publish survive a crash (store: writeMeta,
	// syncDir). It is the most load-bearing toolchain method buff has — crash-correctness and the
	// consume-once security guarantee both rest on it — yet os.Root exposes no Sync, so it can only
	// be pinned here. Existence and signature only: the F_FULLFSYNC the runtime issues inside Sync on
	// darwin is a semantic this guard does not, and cannot, assert.
	Sync() error
	// The executable-bit restore on a consumer's disk: makeExecutable fstats the freshly-opened file
	// to read the mode the umask just allowed, then fchmods the run bits onto it (cli: makeExecutable,
	// reached through openInDir on a confined directory save). Both act on the open fd, not a name —
	// so the bit lands on the exact file just opened with no second lookup to race, and the one helper
	// also serves the unconfined os.Create fd of a plain -o path. os.Root has path-based Stat and
	// Chmod, but the fd forms are what this needs: (*os.Root).Chmod buff never calls, and this fd Stat
	// is a distinct method from the (*os.Root).Stat rootAPI pins for recovery — same name, different
	// receiver.
	Stat() (os.FileInfo, error)
	Chmod(os.FileMode) error
}

// The three pins, kept together — they share one purpose and one failure mode. os.OpenRoot is
// pinned twice on purpose: the method (rootAPI, the extractor's nested per-extraction root) and the
// package-level function (here, the server's data-root bootstrap and the client's output sinks) are
// different symbols, both real.
var (
	_ rootAPI                        = (*os.Root)(nil) // the confined-fs surface buff persists through
	_ fileAPI                        = (*os.File)(nil) // the recovery read, the durable fsync, the exec-bit restore
	_ func(string) (*os.Root, error) = os.OpenRoot     // the constructor the whole confinement model starts from
)
