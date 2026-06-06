package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/clip"
)

// archiveReader must satisfy joiner so the copy flow collects its producer's outcome through join
// rather than through Close — the split that keeps the transport's body-close from either blocking
// on the producer or consuming the outcome.
var _ joiner = (*archiveReader)(nil)

// saveSink and newDirSink are the two terminal sinks that can rescue a spent consume-once delivery
// whose landing collided; the flow dispatches the rescue through a runtime sink.(salvager)
// assertion, so a drift in either salvager method's signature would silently yield ok==false,
// disable the rescue, and still compile — losing the only copy of a spent delivery. These anchors
// turn that drift into a build error, mirroring var _ joiner above for the package's other optional
// capability.
var (
	_ salvager = saveSink{}
	_ salvager = newDirSink{}
)

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

// TestResolveCopyError pins the causal-priority join white-box, since the function is unexported.
// A genuine source error wins over the transport error it caused and is re-attributed to cli with
// the buff: marker (it carries none of its own); the two symptom errors — the pipe this flow closed
// after a failed Put, and a cancellation — yield to the transport error so the real status or the
// transport's own cancellation report surfaces verbatim; and both-nil is success. This determinism
// is what a first-error group cannot guarantee and the reason the join is hand-rolled.
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
// component, but it pins the reduction before ValidName widens to the hierarchical namespace
// it reserves: a slot like "team/work" must extract into "work", a single component ExtractNew
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

// TestChooseSourceRejectsSpecialFile pins the copy-side early-exit: a single path that is neither
// a regular file nor a directory has nothing to archive, so it is refused before any transfer
// opens rather than streamed as an empty clip. /dev/null is the portable special file (a character
// device); the archive.Stream ErrEmptyArchive backstop covers the multi-path lists this single-path
// check cannot reach.
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

// TestDivertConsumeOnceEmptyGeneration pins the empty-generation guard. The salvage names its
// sibling with the delivery's generation id — a wire value a foreign or buggy peer controls — so
// an absent one leaves no way to form a distinct name. Rather than mint a degenerate, non-unique
// sibling (./secret.bin. with a trailing dot, which a second such salvage would then collide with),
// the divert refuses: the body is never touched, the collision identity is kept (so it still scores
// exit 6), and the error names the missing generation. Naming the loss itself is the flow's job,
// uniform across every path, so this white-box check (which bypasses paste) sees only the why. A
// real api server always sends a generation, so this floor guards only the foreign peer.
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
	if e := err.Error(); !strings.Contains(e, "generation") {
		t.Errorf("err=%q, want it to name the missing generation", e)
	}
	if r.Len() != len(body) {
		t.Errorf("the guard consumed %d bytes from the body, want it untouched", len(body)-r.Len())
	}
	if ents, _ := os.ReadDir(work); len(ents) != 0 {
		t.Errorf("the refused salvage created %d entries, want none (no degenerate sibling)", len(ents))
	}
}

// TestDivertConsumeOnceUnusableSibling pins the flow's ValidFilename gate directly, for both
// salvagers. A present-but-hostile generation id (here one carrying a path separator, a wire value
// a foreign peer controls) splices into a name that is not a valid basename, so neither sink can
// form a usable sibling. The flow catches that uniformly, before any byte is read: it wraps the
// original collision — keeping its identity, so a script still reads exit 6 — names the unusable
// sibling, leaves the body untouched, and writes nothing to disk. The loss itself is named once
// by paste, not here. This single gate is what makes "no usable sibling" the one signal both sinks
// share; before it, each sink's own open scored its own low-level error (a generic 1), diverging
// from the absent-generation floor's 6.
func TestDivertConsumeOnceUnusableSibling(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	const body = "the one copy"
	const hostileGen = "abcd/ef" // a separator no basename may carry; ValidName admits no '/' either
	cases := []struct {
		name    string
		sink    Sink
		meta    clip.Meta
		refusal error
		ident   error // the collision identity the wrap must preserve, so exitCode stays 6
	}{
		{"file", saveSink{errw: io.Discard, slot: "s"}, clip.Meta{Kind: clip.KindFile, Filename: "secret.bin"}, buffErr(os.ErrExist), os.ErrExist},
		{"dir", newDirSink{name: "proj", errw: io.Discard}, clip.Meta{Kind: clip.KindArchive, Filename: "proj"}, buffErr(archive.ErrDestExists), archive.ErrDestExists},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(body)
			cl := clip.Clip{ConsumeOnce: true, Generation: hostileGen, Meta: tc.meta}
			err := divertConsumeOnce(context.Background(), tc.sink, r, cl, tc.refusal)
			if err == nil {
				t.Fatal("divert with a hostile generation returned nil, want a reported loss")
			}
			if !errors.Is(err, tc.ident) {
				t.Errorf("err=%v, want it to keep the collision identity (exit 6)", err)
			}
			if e := err.Error(); !strings.Contains(e, "usable sibling") {
				t.Errorf("err=%q, want it to name the unusable sibling", e)
			}
			if r.Len() != len(body) {
				t.Errorf("the gate consumed %d bytes from the body, want it untouched", len(body)-r.Len())
			}
			if ents, _ := os.ReadDir(work); len(ents) != 0 {
				t.Errorf("the refused salvage created %d entries, want none", len(ents))
			}
		})
	}
}

// TestBesideNameDotfile pins the file salvage's sibling formatter across the cases that matter:
// the one that changed and the ones that must not. A pure dotfile (.bashrc) is all "extension"
// to path.Ext, so the naive splice would put the gen before the leading dot; besideName treats an
// empty stem as extension-less and appends the gen at the end, exactly as for a name with no dot at
// all (Makefile). Every other shape is unchanged: an interior extension keeps the gen in front of
// it (report.pdf, archive.tar.gz), and a leading dot with a later dot has a real stem, so it is not
// the dotfile case (.bash.rc). Only a consume-once file clip literally named like a dotfile reaches
// this, but the formatter is pure, so the contract is pinned directly.
func TestBesideNameDotfile(t *testing.T) {
	const gen = "0123456789abcdef0123456789abcdef" // a 32-hex stand-in for a real genid
	cases := []struct{ name, want string }{
		{".bashrc", ".bashrc." + gen},                    // pure dotfile: gen at the end (the fix)
		{".gitignore", ".gitignore." + gen},              // likewise
		{"Makefile", "Makefile." + gen},                  // no extension: unchanged, gen at the end
		{"report.pdf", "report." + gen + ".pdf"},         // interior extension: gen kept in front of it
		{"archive.tar.gz", "archive.tar." + gen + ".gz"}, // only the final extension is kept last
		{".bash.rc", ".bash." + gen + ".rc"},             // leading dot but a real stem: not the dotfile case
	}
	for _, tc := range cases {
		if got := besideName(tc.name, gen); got != tc.want {
			t.Errorf("besideName(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestCreatedText pins the CREATED rendering: a span back to the creation instant, the "how fresh
// is this" a listing of ephemeral clips asks. It covers the just-now floor — a sub-second span, and
// the slightly negative one a client clock running ahead of the server's yields, both read "just
// now" rather than "0s ago" or a negative span — the one-second boundary into a counted span, and
// the defensive dash for a zero instant a finalized clip never carries.
func TestCreatedText(t *testing.T) {
	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		t    time.Time
		want string
	}{
		{"zero instant is a dash", time.Time{}, "-"},
		{"created at the listing instant", now, "just now"},
		{"created within the last second", now.Add(-500 * time.Millisecond), "just now"},
		{"clock skew: created just ahead of now", now.Add(time.Second), "just now"},
		{"one second ago", now.Add(-time.Second), "1s ago"},
		{"two minutes ago", now.Add(-2 * time.Minute), "2m ago"},
		{"three hours ago", now.Add(-3 * time.Hour), "3h ago"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := createdText(now, tc.t); got != tc.want {
				t.Errorf("createdText = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestExpiresText pins the EXPIRES rendering and, since the renderers are humanDuration's only
// callers, the magnitude formatter's contract through it. The framing: a zero instant is the kept-
// forever "never", an instant already past is "expired" — which a finalized clip briefly is between
// its deadline and the reaper's next sweep — and otherwise the time left. The magnitude: unit
// selection down to the second a wall-clock "15:04" would have hidden, truncation to the whole unit
// so a span never claims more time than is left, the sub-second sliver floored to "in 0s", and a
// ceiling at hours since a TTL is a Go duration with no day unit.
func TestExpiresText(t *testing.T) {
	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		t    time.Time
		want string
	}{
		{"zero instant is never", time.Time{}, "never"},
		{"hours left", now.Add(24 * time.Hour), "in 24h"},
		{"minutes left", now.Add(5 * time.Minute), "in 5m"},
		{"seconds left, visible at last", now.Add(9 * time.Second), "in 9s"},
		{"truncates to the whole unit left", now.Add(90 * time.Second), "in 1m"}, // 90s floors to 1m, never 2m
		{"a multi-day span stays in hours", now.Add(240 * time.Hour), "in 240h"}, // no day unit
		{"a sliver under a second still has time", now.Add(500 * time.Millisecond), "in 0s"},
		{"at the deadline", now, "expired"},
		{"past the deadline, not yet reaped", now.Add(-5 * time.Second), "expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := expiresText(now, tc.t); got != tc.want {
				t.Errorf("expiresText = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSafeField pins the metadata-probe render guard: a clean field is returned untouched, and a
// field carrying a terminal-driving byte is rendered inert — the result differs from the input and
// no control rune or invalid byte survives to reach the terminal raw. ESC and a raw C1 byte are the
// active escape-sequence introducers; a tab or newline would break the listing's column alignment.
func TestSafeField(t *testing.T) {
	for _, s := range []string{"clip", "café.pdf", "a-b_c.tar.gz", "bytes", ""} {
		if got := safeField(s); got != s {
			t.Errorf("safeField(%q) = %q, want it unchanged", s, got)
		}
	}
	for _, s := range []string{"a\x1bb", "tab\there", "line\nbreak", "bell\x07", "del\x7f", "\x9b]0;x"} {
		got := safeField(s)
		if got == s {
			t.Errorf("safeField(%q) left the value unchanged; a control byte would reach the terminal", s)
		}
		for _, r := range got {
			if r == utf8.RuneError || unicode.IsControl(r) {
				t.Errorf("safeField(%q) = %q still carries a control rune or invalid byte", s, got)
			}
		}
	}
}
