package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/srevn/buff/clip"

	"github.com/srevn/buff/store/internal/buffer"
)

// These are the disk store's focused white-box tests. The interchangeability of disk and memory is
// proven by the disk row of the contract suite; here we pin what is specific to disk and invisible
// from the Store interface: the real fsync path, the on-disk layout a later recovery pass will
// read, the durable consume-once claim marker, and the destroy-in-place branch a failed read on
// disk reaches but the infallible memory medium never can.

// quietLogger discards recovery's log output, so a test that deliberately drives a quarantine or a
// reclaim does not spray warnings across the test run. The tests assert on-disk and in-store state,
// not log lines, so swallowing the logs costs no coverage.
func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// captureLogger returns a logger that records into the returned buffer, for the one kind of test
// that must assert on a log line rather than on-disk or in-store state: a reclamation failure is
// observable only as the warning the medium emits, so the warning is the property under test.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// newDiskMedium builds a disk medium over an already-open root and prepares its shared directories,
// defaulting to the quiet logger when none is given. It is the construction half newDiskStore and
// recoverDiskStore share.
func newDiskMedium(t *testing.T, root *os.Root, opts DiskOpts) *diskMedium {
	t.Helper()
	log := opts.Logger
	if log == nil {
		log = quietLogger()
	}
	m := &diskMedium{root: root, fsync: opts.Fsync, checksum: opts.Checksum, log: log}
	if err := m.ensureDirs(); err != nil {
		t.Fatal(err)
	}
	return m
}

// newDiskStore builds a first-boot disk store over a fresh temp root and returns the concrete store
// and its medium, so a test can inspect the bytes the store laid down. The root is empty, so there
// is nothing to recover; recoverDiskStore is the helper that replays an existing root. The root is
// closed when the test ends.
func newDiskStore(t *testing.T, c Config, opts DiskOpts) (*store, *diskMedium) {
	t.Helper()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	m := newDiskMedium(t, root, opts)
	return newStore(m, time.Now, c), m
}

// recoverDiskStore is the white-box twin of NewDisk: over an already-populated root it runs the
// scan and restore a real restart runs, and hands back the concrete store and medium. It takes an
// injectable clock so a recovery test can drive the monotonic-id reseed under a backward clock,
// and an existing root so a test can simulate a crash simply by abandoning the first store and
// recovering over the same bytes.
func recoverDiskStore(t *testing.T, root *os.Root, now func() time.Time, c Config, opts DiskOpts) (*store, *diskMedium) {
	t.Helper()
	m := newDiskMedium(t, root, opts)
	rec, err := m.scan(opts.Checksum)
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(m, now, c)
	s.restore(rec)
	return s, m
}

// readRoot reads a file within the root, failing the test if it cannot.
func readRoot(t *testing.T, root *os.Root, path string) []byte {
	t.Helper()
	f, err := root.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// assertAbsent fails the test unless path does not exist within the root.
func assertAbsent(t *testing.T, root *os.Root, path string) {
	t.Helper()
	if _, err := root.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s: Stat = %v, want a not-exist error", path, err)
	}
}

// TestDiskRoundTripFsync drives a create, write, finalize, and read with durability on, so every
// real Sync runs: the data file, the metadata temp file, and the directory entries — the root, the
// per-name levels, and the gen dir on commit. On darwin those are full-device flushes. The test
// asserts the bytes round-trip; that they do, with fsync on, is the proof the Sync path is sound on
// this platform.
func TestDiskRoundTripFsync(t *testing.T) {
	s, _ := newDiskStore(t, Config{}, DiskOpts{Fsync: true})
	const body = "durable bytes"
	c := finalize(t, s, "doc", PutOpts{}, []byte(body))
	if c.Size != int64(len(body)) {
		t.Errorf("finalized size = %d, want %d", c.Size, len(body))
	}

	rc, gc, err := s.Open(context.Background(), "doc", GetOpts{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	data, err := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != body {
		t.Errorf("read %q, want %q", data, body)
	}
	if gc.Generation != c.Generation {
		t.Errorf("opened generation %q != written %q", gc.Generation, c.Generation)
	}
}

// TestDiskLayout pins the on-disk shape a finalized clip leaves behind — the exact contract the
// recovery pass will read. After a clean finalize the data file holds the bytes, meta.json parses
// and describes the generation, the transient temp and consumed markers are gone, and the data and
// directory are owner-only — plaintext at rest is not group- or world-readable, a free defense in
// depth that does not replace the trust model.
func TestDiskLayout(t *testing.T) {
	s, m := newDiskStore(t, Config{}, DiskOpts{Fsync: true})
	const body = "the bytes"
	c := finalize(t, s, "report", PutOpts{}, []byte(body))
	genDir := dirClips + "/" + c.Generation

	if got := readRoot(t, m.root, genDir+"/"+fileData); string(got) != body {
		t.Errorf("data = %q, want %q", got, body)
	}

	var mf metaFile
	if err := json.Unmarshal(readRoot(t, m.root, genDir+"/"+fileMeta), &mf); err != nil {
		t.Fatalf("unmarshal meta.json: %v", err)
	}
	if mf.Version != metaVersion || mf.Name != "report" || mf.Generation != c.Generation ||
		mf.Kind != clip.KindText || mf.Size != int64(len(body)) {
		t.Errorf("meta.json = %+v; want version %d, name report, gen %s, kind text, size %d",
			mf, metaVersion, c.Generation, len(body))
	}

	assertAbsent(t, m.root, genDir+"/"+fileMetaTmp)
	assertAbsent(t, m.root, genDir+"/"+fileConsumed)

	// Owner-only: umask can only clear more bits, so a 0o600 file and 0o700 dir never carry group or
	// other access whatever the environment's umask.
	for _, p := range []string{genDir, genDir + "/" + fileData} {
		fi, err := m.root.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if fi.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s perms = %#o, want no group or other access", p, fi.Mode().Perm())
		}
	}
}

// TestDiskDurableConsumeClaim proves the consume-once claim is the durable rename it must be: an
// Open of a finalized consume-once clip turns meta.json into meta.consumed on disk — the marker a
// crash would leave for recovery to reclaim — and once the claiming reader drains and closes, the
// whole generation directory is gone.
func TestDiskDurableConsumeClaim(t *testing.T) {
	s, m := newDiskStore(t, Config{}, DiskOpts{Fsync: true})
	c := finalize(t, s, "secret", PutOpts{ConsumeOnce: true}, []byte("payload"))
	genDir := dirClips + "/" + c.Generation

	rc, _, err := s.Open(context.Background(), "secret", GetOpts{})
	if err != nil {
		t.Fatalf("Open (the claim): %v", err)
	}
	assertAbsent(t, m.root, genDir+"/"+fileMeta)
	if _, err := m.root.Stat(genDir + "/" + fileConsumed); err != nil {
		t.Errorf("meta.consumed after claim: Stat = %v, want present", err)
	}

	data, err := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("read the claimed clip: %v", err)
	}
	if string(data) != "payload" {
		t.Errorf("claimed read = %q, want payload", data)
	}
	assertAbsent(t, m.root, genDir)
}

// readFailMedium serves real disk-backed buffers whose append side works but whose read side
// never opens, and whose lifecycle steps are all no-ops. It is the only way to reach the store's
// openRead-after-claim branch: the memory medium's reads cannot fail, but a disk read can (the data
// file's open can error), and when it does on an already-claimed consume-once generation the store
// must destroy that generation in place rather than un-claim it.
type readFailMedium struct{ dir string }

func (m *readFailMedium) create(id genID) (*buffer.Buffer, error) {
	f, err := os.OpenFile(filepath.Join(m.dir, id.String()), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	failOpen := func() (*os.File, error) { return nil, errMedium }
	return buffer.NewDisk(f, failOpen, false), nil
}
func (m *readFailMedium) finalize(*generation) error      { return nil }
func (m *readFailMedium) claim(*generation) (bool, error) { return true, nil }
func (m *readFailMedium) remove(*generation)              {}

// TestOpenReadFailDestroysClaimed proves the destroy-in-place path: a consume-once Open claims its
// one delivery, then fails to open the reader. The claim cannot be taken back — un-claiming would
// risk a second delivery — so the claimed generation is destroyed where it stands: Open returns
// the wrapped error, the quota is fully released, the handle is evicted, and a later Open finds
// nothing. At-most-once holds with zero delivery.
func TestOpenReadFailDestroysClaimed(t *testing.T) {
	s := newStore(&readFailMedium{dir: t.TempDir()}, time.Now, Config{})
	ctx := context.Background()

	w, err := s.Create(ctx, "secret", clip.Meta{Kind: clip.KindText}, PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := s.Open(ctx, "secret", GetOpts{}); !errors.Is(err, errMedium) {
		t.Fatalf("Open with a failing reader = %v, want wrap of %v", err, errMedium)
	}

	if hasHandle(s.reg, "secret") {
		t.Error("destroy-in-place left the handle behind")
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
		t.Errorf("quota after destroy-in-place: bytes=%d clips=%d, want 0/0", b, c)
	}
	if _, _, err := s.Open(ctx, "secret", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after destroy = %v, want ErrNotFound", err)
	}
}

// TestRemoveFailureLogs proves the one signal a failed runtime reclamation leaves: when RemoveAll
// cannot delete a generation's home, diskMedium.remove records a warning instead of swallowing the
// error, and a consume-once clip earns a distinct, greppable line so a deliver-once secret kept
// past its one delivery is alertable. The forcing function is closing the os.Root: every operation
// through it then fails deterministically with "file already closed" while the directory provably
// survives — portable, and not bypassed the way a permission bit would be under a privileged test
// runner. Accounting must still balance: a home the disk could not delete never strands the quota,
// because releaseGen reads the in-memory Size regardless of whether the directory is gone.
func TestRemoveFailureLogs(t *testing.T) {
	ctx := context.Background()

	t.Run("a generic clip names its bytes", func(t *testing.T) {
		log, buf := captureLogger()
		s, m := newDiskStore(t, Config{}, DiskOpts{Logger: log})
		finalize(t, s, "doc", PutOpts{}, []byte("bytes")) // finalized on disk before the root closes

		if err := m.root.Close(); err != nil { // every later RemoveAll through the root now fails
			t.Fatalf("close root: %v", err)
		}
		if err := s.Delete(ctx, "doc"); err != nil {
			t.Fatalf("Delete returned %v; a failed reclaim must not fail the operation", err)
		}

		out := buf.String()
		if !strings.Contains(out, "its bytes remain on disk") {
			t.Errorf("missing the generic reclaim warning; log = %q", out)
		}
		if strings.Contains(out, "plaintext") {
			t.Errorf("a non-consume clip logged the consume-once wording; log = %q", out)
		}
		if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
			t.Errorf("quota after a failed reclaim: bytes=%d clips=%d, want 0/0", b, c)
		}
	})

	t.Run("a consume-once secret names its plaintext", func(t *testing.T) {
		log, buf := captureLogger()
		s, m := newDiskStore(t, Config{}, DiskOpts{Logger: log})
		finalize(t, s, "secret", PutOpts{ConsumeOnce: true}, []byte("payload"))

		// Deliver the secret once, then fail its post-delivery cleanup: the exact lifecycle in which a
		// consume-once clip's plaintext would be silently retained on disk.
		rc, _, err := s.Open(ctx, "secret", GetOpts{})
		if err != nil {
			t.Fatalf("Open (claim): %v", err)
		}
		if data, err := io.ReadAll(rc); err != nil || string(data) != "payload" {
			t.Fatalf("drained = %q (err %v), want payload", data, err)
		}
		if err := m.root.Close(); err != nil {
			t.Fatalf("close root: %v", err)
		}
		_ = rc.Close() // leasedReader.Close → cleanupConsumed → reclaim → remove fails

		out := buf.String()
		if !strings.Contains(out, "plaintext remains on disk") {
			t.Errorf("missing the consume-once reclaim warning; log = %q", out)
		}
		if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
			t.Errorf("quota after a failed consume reclaim: bytes=%d clips=%d, want 0/0", b, c)
		}
	})
}
