package cli

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/srevn/buff/client"
)

// resolve runs the front end the way Run does, via parseArgs (scan then grammar). An error
// from either pass is what a real invocation would see.
func resolve(args []string, inIsTTY bool) (invocation, error) {
	return parseArgs(args, inIsTTY)
}

// TestScan pins the lexical pass: how raw arguments become slots, paths, and flag values,
// purely by syntax. It covers the sigil/path/flag classification, the two escapes the grammar
// imposes (./serve and ./@foo are paths, not the reserved word or a slot), value consumption
// in both the spaced and =attached forms, the -- end-of-flags marker, and the loud failures
// (unknown flag, empty @, missing value).
func TestScan(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantSlots []string
		wantPaths []string
		check     func(flags) bool // optional extra flag assertions
		wantErr   string           // non-empty ⇒ expect an error containing this
	}{
		{name: "empty", args: nil},
		{name: "one slot", args: []string{"@work"}, wantSlots: []string{"work"}},
		{name: "one path", args: []string{"report.pdf"}, wantPaths: []string{"report.pdf"}},
		{name: "slot and path interleave one way", args: []string{"@work", "a", "b"}, wantSlots: []string{"work"}, wantPaths: []string{"a", "b"}},
		{name: "slot and path interleave other way", args: []string{"a", "@work", "b"}, wantSlots: []string{"work"}, wantPaths: []string{"a", "b"}},
		{name: "two slots are kept (parse rejects)", args: []string{"@a", "@b"}, wantSlots: []string{"a", "b"}},
		{name: "serve escape is a path", args: []string{"./serve"}, wantPaths: []string{"./serve"}},
		{name: "at-file escape is a path", args: []string{"./@foo"}, wantPaths: []string{"./@foo"}},
		{name: "lone dash is a path", args: []string{"-"}, wantPaths: []string{"-"}},
		{name: "double dash ends flags", args: []string{"--", "-rf"}, wantPaths: []string{"-rf"}},
		{name: "double dash keeps sigil classification", args: []string{"--", "@work"}, wantSlots: []string{"work"}},
		{name: "output spaced", args: []string{"-o", "out"}, check: func(f flags) bool { return f.outputSet && f.output == "out" }},
		{name: "output attached", args: []string{"-o=out"}, check: func(f flags) bool { return f.outputSet && f.output == "out" }},
		{name: "long output spaced", args: []string{"--output", "out"}, check: func(f flags) bool { return f.outputSet && f.output == "out" }},
		{name: "ttl attached", args: []string{"--ttl=1h30m"}, check: func(f flags) bool { return f.ttlSet && f.ttl == 90*time.Minute }},
		{name: "server spaced", args: []string{"--server", "http://h:8080"}, check: func(f flags) bool { return f.serverSet && f.server == "http://h:8080" }},
		{name: "bool flags", args: []string{"-c", "--keep", "--consume"}, check: func(f flags) bool { return f.copy && f.keep && f.consume }},
		{name: "output value may look like a flag", args: []string{"-o", "-weird"}, check: func(f flags) bool { return f.output == "-weird" }},

		{name: "unknown flag", args: []string{"-z"}, wantErr: "unknown flag"},
		{name: "empty slot", args: []string{"@"}, wantErr: "needs a name"},
		{name: "missing value", args: []string{"-o"}, wantErr: "requires a value"},
		{name: "value on bool flag", args: []string{"--keep=1"}, wantErr: "takes no value"},
		{name: "negative ttl", args: []string{"--ttl=-1h"}, wantErr: "must not be negative"},
		{name: "malformed ttl", args: []string{"--ttl=nope"}, wantErr: "invalid --ttl"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scan(tc.args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("scan(%q) error = %v, want containing %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("scan(%q) unexpected error: %v", tc.args, err)
			}
			if !slices.Equal(got.slots, tc.wantSlots) {
				t.Errorf("slots = %q, want %q", got.slots, tc.wantSlots)
			}
			if !slices.Equal(got.paths, tc.wantPaths) {
				t.Errorf("paths = %q, want %q", got.paths, tc.wantPaths)
			}
			if tc.check != nil && !tc.check(got.flags) {
				t.Errorf("flags = %+v failed the case check", got.flags)
			}
		})
	}
}

// TestMode pins the whole copy-vs-paste table: stream type and the -c/-p forces with no
// residual guess. Every ambiguous or sourceless case is a loud error, never a silent
// mis-action.
func TestMode(t *testing.T) {
	cases := []struct {
		name                                string
		hasPaths, stdinPiped, force, fpaste bool
		want                                action
		wantErr                             string
	}{
		{name: "tty no args pastes", want: actionPaste},
		{name: "piped stdin copies", stdinPiped: true, want: actionCopy},
		{name: "paths copy", hasPaths: true, want: actionCopy},
		{name: "paths and pipe is ambiguous", hasPaths: true, stdinPiped: true, wantErr: "both stdin and path"},
		{name: "force copy with pipe", force: true, stdinPiped: true, want: actionCopy},
		{name: "force copy with no source", force: true, wantErr: "nothing to copy"},
		{name: "force paste", fpaste: true, want: actionPaste},
		{name: "force paste ignores incidental pipe", fpaste: true, stdinPiped: true, want: actionPaste},
		{name: "force paste rejects paths", fpaste: true, hasPaths: true, wantErr: "paste takes no path"},
		{name: "conflicting forces", force: true, fpaste: true, wantErr: "conflict"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mode(tc.hasPaths, tc.stdinPiped, tc.force, tc.fpaste)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("mode error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("mode = %v, want %v", got, tc.want)
			}
		})
	}
}

// want is the resolved-invocation shape a parse case asserts; its zero value is the common
// case (no paths, no -o, no write-options, no server).
type want struct {
	act    action
	slot   string
	paths  []string
	out    string
	outSet bool
	put    client.PutOpts
	server string
}

func eqInv(got invocation, w want) bool {
	return got.act == w.act && got.slot == w.slot &&
		slices.Equal(got.paths, w.paths) &&
		got.output == w.out && got.outputSet == w.outSet &&
		got.put == w.put && got.server == w.server
}

// TestParse is the grammar matrix — the named P8 proof. It drives scan+parse across slots,
// paths, the mode forces, the management flags, and the write/output options, asserting the
// resolved (action, slot, paths, options) for the valid cases and a clear usage error for
// every malformed one. The serve/@-escapes and the both-stdin-and-paths error are included.
func TestParse(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		tty     bool // stdin is a terminal
		want    want
		wantErr string
	}{
		// position independence and slot defaulting
		{name: "slot then path", args: []string{"@a", "b"}, tty: true, want: want{act: actionCopy, slot: "a", paths: []string{"b"}}},
		{name: "path then slot", args: []string{"b", "@a"}, tty: true, want: want{act: actionCopy, slot: "a", paths: []string{"b"}}},
		{name: "two slots", args: []string{"@a", "@b"}, tty: true, wantErr: "too many slots"},
		{name: "no args at tty pastes default", args: nil, tty: true, want: want{act: actionPaste, slot: "default"}},
		{name: "piped stdin copies default", args: nil, tty: false, want: want{act: actionCopy, slot: "default"}},
		{name: "piped stdin to a slot", args: []string{"@work"}, tty: false, want: want{act: actionCopy, slot: "work"}},
		{name: "pipe plus path is ambiguous", args: []string{"file"}, tty: false, wantErr: "both stdin and path"},
		{name: "two paths copy", args: []string{"a", "b", "@p"}, tty: true, want: want{act: actionCopy, slot: "p", paths: []string{"a", "b"}}},

		// escapes
		{name: "serve escape copies the file", args: []string{"./serve"}, tty: true, want: want{act: actionCopy, slot: "default", paths: []string{"./serve"}}},
		{name: "at-file escape copies the file", args: []string{"./@foo"}, tty: true, want: want{act: actionCopy, slot: "default", paths: []string{"./@foo"}}},

		// mode forces
		{name: "conflicting forces", args: []string{"-c", "-p"}, tty: true, wantErr: "conflict"},
		{name: "paste rejects paths", args: []string{"-p", "file"}, tty: true, wantErr: "paste takes no path"},
		{name: "force copy no source", args: []string{"-c"}, tty: true, wantErr: "nothing to copy"},
		{name: "force copy with pipe", args: []string{"-c"}, tty: false, want: want{act: actionCopy, slot: "default"}},
		{name: "paste a slot", args: []string{"@work"}, tty: true, want: want{act: actionPaste, slot: "work"}},

		// write-options (copy) and output (paste) placement
		{name: "copy with consume", args: []string{"--consume", "file", "@x"}, tty: true, want: want{act: actionCopy, slot: "x", paths: []string{"file"}, put: client.PutOpts{ConsumeOnce: true}}},
		{name: "copy with ttl", args: []string{"--ttl", "1h", "file", "@x"}, tty: true, want: want{act: actionCopy, slot: "x", paths: []string{"file"}, put: client.PutOpts{TTL: time.Hour}}},
		{name: "paste with output", args: []string{"@x", "-o", "out"}, tty: true, want: want{act: actionPaste, slot: "x", out: "out", outSet: true}},
		{name: "copy rejects output", args: []string{"file", "@x", "-o", "out"}, tty: true, wantErr: "applies only when pasting"},
		{name: "paste rejects write opts", args: []string{"@x", "--ttl", "1h"}, tty: true, wantErr: "apply only when copying"},
		{name: "keep and ttl conflict", args: []string{"--keep", "--ttl", "1h", "file", "@x"}, tty: true, wantErr: "--keep and --ttl conflict"},
		{name: "server override on paste", args: []string{"--server", "http://h:9", "@x"}, tty: true, want: want{act: actionPaste, slot: "x", server: "http://h:9"}},

		// management
		{name: "list", args: []string{"-l"}, tty: true, want: want{act: actionList}},
		{name: "list rejects a slot", args: []string{"-l", "@x"}, tty: true, wantErr: "takes no slot"},
		{name: "delete a slot", args: []string{"-d", "@x"}, tty: true, want: want{act: actionDelete, slot: "x"}},
		{name: "delete needs a slot not a path", args: []string{"-d", "x"}, tty: true, wantErr: "needs a slot"},
		{name: "delete with no slot", args: []string{"-d"}, tty: true, wantErr: "needs exactly one slot"},
		{name: "stat a slot", args: []string{"-s", "@x"}, tty: true, want: want{act: actionStat, slot: "x"}},
		{name: "version", args: []string{"--version"}, tty: true, want: want{act: actionVersion}},
		{name: "version takes nothing", args: []string{"--version", "@x"}, tty: true, wantErr: "takes no slot"},
		{name: "two management flags conflict", args: []string{"-l", "-d", "@x"}, tty: true, wantErr: "conflicting actions"},

		// help short-circuits the grammar: it wins over a management conflict and stray args alike,
		// but a scan error (an unknown flag earlier on the line) still fails before parse sees help.
		{name: "help short", args: []string{"-h"}, tty: true, want: want{act: actionHelp}},
		{name: "help long", args: []string{"--help"}, tty: true, want: want{act: actionHelp}},
		{name: "help wins over a management conflict", args: []string{"-l", "-h"}, tty: true, want: want{act: actionHelp}},
		{name: "help wins over too many slots", args: []string{"-h", "@a", "@b"}, tty: true, want: want{act: actionHelp}},
		{name: "scan error beats help", args: []string{"--bogus", "-h"}, tty: true, wantErr: "unknown flag"},
		{name: "management rejects mode force", args: []string{"-l", "-c"}, tty: true, wantErr: "apply only when copying or pasting"},
		{name: "management rejects write opts", args: []string{"-d", "@x", "--keep"}, tty: true, wantErr: "apply only when copying"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolve(tc.args, tc.tty)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("resolve(%q) error = %v, want containing %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve(%q) unexpected error: %v", tc.args, err)
			}
			if !eqInv(got, tc.want) {
				t.Errorf("resolve(%q) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

// TestUsageErrorType confirms a malformed invocation is a *usageError, the identity the exit
// map turns into the generic usage exit and a caller uses to tell a grammar mistake from a
// network or domain failure.
func TestUsageErrorType(t *testing.T) {
	_, err := resolve([]string{"-z"}, true)
	var ue *usageError
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v (%T), want *usageError", err, err)
	}
}
