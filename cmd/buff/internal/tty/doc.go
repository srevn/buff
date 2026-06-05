// Package tty answers one question for the binary's apex: is a given standard stream a terminal?
// It is the single place that knows how to ask, isolated here because the answer is platform
// specific — a terminal-attributes ioctl on unix, a character-device heuristic elsewhere — and
// because isolating it is what lets cmd/buff stay wiring and cli stay a pure consumer of the
// booleans it produces. The apex calls IsTerminal once per standard stream and hands cli plain bools;
// nothing below the apex ever probes a terminal.
//
// The detection is the one libc's isatty(3) makes and golang.org/x/term wraps, done in the standard
// library alone so the module keeps its near-empty go.mod (an x/term would pull x/sys with it): the
// read-terminal-attributes ioctl succeeds only on a tty. The broader "is this a character device"
// test it supersedes counted /dev/null and every other char device as a terminal — the defect this
// package closes on the platforms buff ships, and the heuristic it keeps only as the portable fallback.
package tty
