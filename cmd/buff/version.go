package main

import (
	"runtime/debug"
	"sync"
)

// version is the release stamp a build may set with -ldflags "-X main.version=vX.Y.Z". It reads
// "dev" when unset, which buildVersion takes as "resolve me from the embedded build metadata
// instead." A single package-level var is the whole stamping seam.
var version = "dev"

// buildVersion resolves the human-facing version once, lazily, the first time it is needed. The
// resolution itself is the pure resolveVersion below; this wrapper supplies the one input that
// cannot be a test parameter — the build metadata the toolchain embeds — and memoizes the result,
// since both consumption sites (the client's --version and the server's health string) ask the same
// question and the answer never changes within a process.
var buildVersion = sync.OnceValue(func() string {
	info, ok := debug.ReadBuildInfo()
	return resolveVersion(version, info, ok)
})

// resolveVersion turns the -ldflags stamp and the embedded build metadata into the version string,
// correct across every way the binary is built. The order encodes the precedence:
//
// - An explicit release stamp (anything other than the "dev" placeholder, and non-empty) always
// wins: a `make dist` build says exactly what it stamped. - Otherwise the module version the
// toolchain records: `go install …@vX.Y.Z` embeds "vX.Y.Z" and `go install …@latest` a pseudo-
// version, neither of which -ldflags can reach — this is the fallback that lets a `go install`-ed
// binary self-identify at all. "(devel)" is the toolchain's own placeholder for a local build and
// is treated as "not a real module version." - Otherwise the VCS revision the toolchain stamps into
// a local `go build` (short-hashed, marked -dirty for an uncommitted tree), surfaced as "dev+<rev>"
// so a desk build is still traceable. - Otherwise the placeholder stands: there is nothing more
// to say.
//
// It is a pure function of its inputs so every branch is table-tested without depending on how the
// test binary itself happens to be built.
func resolveVersion(stamp string, info *debug.BuildInfo, ok bool) string {
	if stamp != "dev" && stamp != "" {
		return stamp
	}
	if !ok {
		return stamp
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var rev string
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev != "" {
		if len(rev) > 12 {
			rev = rev[:12]
		}
		if dirty {
			rev += "-dirty"
		}
		return "dev+" + rev
	}
	return stamp
}
