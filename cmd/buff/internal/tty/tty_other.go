//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package tty

import "os"

// IsTerminal is the portable fallback for platforms with no termios ioctl behind the syscall
// package — Windows above all, where the buff binary still builds and runs. It keeps the historical
// heuristic, a terminal is a character device, with the imprecision that carries: it also reports
// /dev/null and kin as terminals. That imprecision is corrected on the unix platforms buff ships
// and CI exercises, where the build selects the ioctl in tty_unix.go; here the heuristic is
// retained verbatim so no platform regresses from today's behaviour and the cross-compile stays
// green.
func IsTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// IsForeground is the fallback's answer to the foreground question, which it cannot ask: Windows,
// plan9, js and wasip1 have no job control to move a process into a terminal's background, and aix,
// solaris and illumos, which do, are given TIOCGPGRP by the syscall package but neither an ioctl
// entry point to issue it through nor getpgrp to compare its answer with. So a terminal the process
// writes to is taken for its own to draw on, and the question reduces to IsTerminal — exact where
// there is no background, and on those three unixes the cost of a line drawn from one.
func IsForeground(f *os.File) bool { return IsTerminal(f) }
