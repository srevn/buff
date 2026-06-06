package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestResolveServerURL drives the client's pre-flag precedence — BUFF_URL over a baked default
// over the built-in fallback — as a pure table, the mirror of TestResolveVersion. Both inputs are
// parameters, so every combination is proven without touching the real environment or depending on
// how the test binary was stamped. The one thing it cannot reach — that the linker actually writes
// main.bakedURL — is TestBakedServerURLStamp's job.
func TestResolveServerURL(t *testing.T) {
	for _, c := range []struct {
		name   string
		envURL string
		baked  string
		want   string
	}{
		// The empty/empty row is also the empty-stamp guard: an unset SERVER_URL that still reaches the
		// linker leaves bakedURL "", and that must degrade to the fallback, never an empty URL.
		{"both unset falls back", "", "", fallbackServerURL},
		{"env only", "https://env.example", "", "https://env.example"},
		{"baked only, no env", "", "https://baked.example", "https://baked.example"},
		{"env wins over baked", "https://env.example", "https://baked.example", "https://env.example"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveServerURL(c.envURL, c.baked); got != c.want {
				t.Errorf("resolveServerURL(%q, %q) = %q, want %q", c.envURL, c.baked, got, c.want)
			}
		})
	}
}

// TestBakedServerURLStamp proves the whole bake chain end to end in a real binary — the -X stamp
// reaches main.bakedURL, resolveServerURL folds it in, and the help renders it — which no pure
// test can show. The linker silently ignores an unknown -X target, so a drift between the var
// name here and the Makefile's -X main.bakedURL would otherwise ship a binary that quietly ignores
// SERVER_URL; this is the automated guard against that. It builds this package with a stamp, runs
// the offline -h (no server, no network — help short-circuits before any client), and asserts
// the baked URL shows and the localhost fallback does not. Guarded so it never breaks a -short or
// toolchain-less run.
func TestBakedServerURLStamp(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a stamped binary")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go toolchain on PATH")
	}
	const baked = "https://baked.example:9999"
	bin := filepath.Join(t.TempDir(), "buff")
	// "." is this package (cmd/buff); go test runs with the package directory as the working dir.
	if out, err := exec.Command(goBin, "build", "-ldflags", "-X main.bakedURL="+baked, "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build stamped binary: %v\n%s", err, out)
	}
	help := runHelp(t, bin)
	if !strings.Contains(help, baked) {
		t.Errorf("help does not show the baked server %q:\n%s", baked, help)
	}
	if strings.Contains(help, "localhost") {
		t.Errorf("help still shows the localhost fallback despite the bake:\n%s", help)
	}
}

// runHelp runs the built binary's offline -h with BUFF_URL stripped from its environment. The strip
// is load-bearing for the assertion: an ambient BUFF_URL (a developer or CI host that points at a
// real relay) would correctly outrank the bake and mask the very value the test checks, so the test
// would fail not on a regression but on the environment it happens to run in.
func runHelp(t *testing.T, bin string) string {
	t.Helper()
	cmd := exec.Command(bin, "-h")
	cmd.Env = withoutBuffURL()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run -h on the stamped binary: %v\n%s", err, out)
	}
	return string(out)
}

// withoutBuffURL returns the process environment with any BUFF_URL entry removed. cmd.Env replaces
// the child's whole environment, so the rest is carried through verbatim and only BUFF_URL is
// dropped.
func withoutBuffURL() []string {
	var out []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "BUFF_URL=") {
			out = append(out, kv)
		}
	}
	return out
}
