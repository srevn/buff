//go:build linux

package tty

import "syscall"

// ioctlReadTermios reads terminal attributes on linux: TCGETS, the same request glibc's tcgetattr —
// and thus isatty — issues. IsTerminal (tty_unix.go) issues it and reads only whether it succeeded.
const ioctlReadTermios = syscall.TCGETS
