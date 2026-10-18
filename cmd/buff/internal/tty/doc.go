// Package tty answers two questions for the binary's apex: is a given standard stream a terminal,
// and does this process hold that terminal's foreground? It is the single place that knows how
// to ask, isolated here because both answers are platform specific — a terminal ioctl on unix, a
// character-device heuristic elsewhere — and because isolating them is what lets cmd/buff stay
// wiring and cli stay a pure consumer of what they produce. The two answers keep differently. A
// stream's being a terminal is fixed while it is open, so one answer serves a whole run; the
// foreground is not, since a running job moves between the foreground and the background at the
// user's say, so a caller asks it afresh each time it matters. Nothing below the apex knows how a
// terminal is probed.
//
// The detection is the one libc's isatty(3) makes and golang.org/x/term wraps, done in the standard
// library alone so the module keeps its near-empty go.mod (an x/term would pull x/sys with it): the
// read-terminal-attributes ioctl succeeds only on a tty. The broader "is this a character device"
// test it supersedes counted /dev/null and every other char device as a terminal — the defect
// this package closes on the platforms buff ships, and the heuristic it keeps only as the portable
// fallback.
package tty
