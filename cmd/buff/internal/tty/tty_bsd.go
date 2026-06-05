//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package tty

import "syscall"

// ioctlReadTermios reads terminal attributes on the BSD lineage, darwin included: TIOCGETA.
// IsTerminal (tty_unix.go) issues it and reads only whether it succeeded. Only the request number
// differs between the unixes buff targets, so it is the sole thing split out per platform.
const ioctlReadTermios = syscall.TIOCGETA
