package main

import (
	"runtime/debug"
	"testing"
)

// TestResolveVersion drives every branch of the release-critical resolver with synthetic build
// metadata, so the logic is proven without depending on how the test binary itself was built. The
// impure half — reading the real build info — is the one line buildVersion adds over this, exercised
// indirectly by TestBuffMain (the client prints whatever resolves) and the make dist smoke.
func TestResolveVersion(t *testing.T) {
	// rev is 20 hex chars (> 12) so the truncation branch is exercised; rev12 is its 12-char prefix.
	const rev = "0123456789abcdef0123"
	const rev12 = "0123456789ab"

	info := func(mainVer string, settings ...debug.BuildSetting) *debug.BuildInfo {
		return &debug.BuildInfo{Main: debug.Module{Version: mainVer}, Settings: settings}
	}
	vcs := func(revision, modified string) []debug.BuildSetting {
		return []debug.BuildSetting{
			{Key: "vcs.revision", Value: revision},
			{Key: "vcs.modified", Value: modified},
		}
	}

	cases := []struct {
		name  string
		stamp string
		info  *debug.BuildInfo
		ok    bool
		want  string
	}{
		// An explicit -ldflags stamp wins over anything the toolchain embedded.
		{"semver stamp wins", "v0.1.0", info("v9.9.9", vcs(rev, "true")...), true, "v0.1.0"},
		{"commit stamp wins", "a09c6eb", info("(devel)", vcs(rev, "false")...), true, "a09c6eb"},

		// No build info at all: only the placeholder is knowable.
		{"no build info", "dev", nil, false, "dev"},
		{"empty stamp, no info", "", nil, false, ""},

		// The go-install fallback: the toolchain-recorded module version.
		{"module version", "dev", info("v1.2.3"), true, "v1.2.3"},
		{"pseudo-version", "dev", info("v0.0.0-20260603000000-abcdef012345"), true, "v0.0.0-20260603000000-abcdef012345"},

		// The local-build fallback: the embedded VCS revision, short-hashed and dirty-marked.
		{"clean vcs rev truncates", "dev", info("(devel)", vcs(rev, "false")...), true, "dev+" + rev12},
		{"dirty vcs rev gets suffix", "dev", info("(devel)", vcs(rev, "true")...), true, "dev+" + rev12 + "-dirty"},
		{"short rev not truncated", "dev", info("(devel)", vcs("abc123", "false")...), true, "dev+abc123"},

		// Nothing usable embedded (no module version, no VCS): the placeholder stands.
		{"devel with no vcs", "dev", info("(devel)"), true, "dev"},
		{"empty main and no vcs", "dev", info(""), true, "dev"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveVersion(c.stamp, c.info, c.ok); got != c.want {
				t.Errorf("resolveVersion(%q, …, %v) = %q, want %q", c.stamp, c.ok, got, c.want)
			}
		})
	}
}
