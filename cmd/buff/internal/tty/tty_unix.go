//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package tty

import (
	"os"
	"syscall"
	"unsafe"
)

// IsTerminal reports whether f is a terminal by the test libc's isatty(3) makes: the read-terminal-
// attributes ioctl succeeds only on a tty and fails with ENOTTY on everything else. The request is
// named ioctlReadTermios per platform — TIOCGETA on darwin and the BSDs, TCGETS on linux — because
// the kernels number it differently; the body is otherwise identical, which is the whole reason it
// is one shared function over a per-platform constant.
//
// This is the question buff's dispositions actually want answered: copy-versus-paste on stdin, and
// show-versus-save and extract-versus-raw on stdout, all turn on "is a human interactively at this
// stream." The os.ModeCharDevice test in tty_other.go answers a strictly broader one — it holds
// for every character device — so it takes /dev/null, /dev/zero, and every other non-terminal char
// device for a terminal, turning the universal `>/dev/null` discard idiom into a file saved or
// a tree extracted into the working directory. Asking the kernel directly draws the line exactly
// where the feature needs it.
//
// One ioctl through the syscall package is the entire mechanism, and it adds no dependency: an
// x/term would issue exactly this underneath. The Termios value is written only by the kernel for
// the duration of the call and never read — the success-or-ENOTTY outcome is the whole signal — and
// passing its address as the syscall's uintptr argument, in the call expression itself, is the one
// unsafe.Pointer conversion go vet recognises as safe (and what keeps t live across the call).
func IsTerminal(f *os.File) bool {
	var t syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), ioctlReadTermios, uintptr(unsafe.Pointer(&t)))
	return errno == 0
}

// IsForeground reports whether f is this process's controlling terminal and the process is in that
// terminal's foreground — the one place a line it redraws in place can land without harm. A job the
// shell has put in the background shares the terminal with whatever the user does next, so a line
// drawn there lands on their prompt; and under `stty tostop` the write itself stops the job, the
// kernel's SIGTTOU suspending it until it is brought back, stalling the transfer the line reports.
//
// The test is tcgetpgrp(3)'s: the terminal's foreground process group, read with TIOCGPGRP, against
// this process's own. The request has one name on all the unixes buff targets, so unlike the
// termios read it needs no per-platform constant. The kernel answers only for the caller's
// controlling terminal and fails with ENOTTY on anything else, so a pipe, a file, or a terminal
// this process does not control all read as not foreground. The group is a pid_t, 32 bits on all of
// them, and the same in-expression unsafe.Pointer conversion as IsTerminal's keeps it live across
// the call.
func IsForeground(f *os.File) bool {
	var pgrp int32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), syscall.TIOCGPGRP, uintptr(unsafe.Pointer(&pgrp)))
	return errno == 0 && int(pgrp) == syscall.Getpgrp()
}
