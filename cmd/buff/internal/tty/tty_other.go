//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package tty

import "os"

// IsTerminal is the portable fallback for platforms with no termios ioctl behind the syscall package
// — Windows above all, where the buff binary still builds and runs. It keeps the historical
// heuristic, a terminal is a character device, with the imprecision that carries: it also reports
// /dev/null and kin as terminals. That imprecision is corrected on the unix platforms buff ships and
// CI exercises, where the build selects the ioctl in tty_unix.go; here the heuristic is retained
// verbatim so no platform regresses from today's behaviour and the cross-compile stays green.
func IsTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
