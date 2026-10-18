package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// syncBuf is a buffer a status line writes while the test reads it, under one lock for both. A
// synctest.Wait orders the line's writes before the test's read, but nothing orders the read before
// a write the line makes once the clock moves on, so the lock, not the barrier, is what makes the
// buffer safe to share.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// statusTerm is a status line's fake terminal: the two streams it writes, the IO a transfer runs
// under while the line lives, and the settle that ends it. It runs inside a synctest bubble, whose
// fake clock drives the line's ticker, so a test steps through the line's life by sleeping, and
// reads the streams after synctest.Wait, the barrier past which every due draw has happened.
type statusTerm struct {
	err, out syncBuf
	fg       atomic.Bool // the process holds the terminal's foreground
	std      IO
	settle   func(error)
}

func newStatusTerm(p *progress) *statusTerm {
	term := &statusTerm{}
	term.fg.Store(true)
	term.std, term.settle = showProgress(p, IO{
		Out: &term.out, Err: &term.err, ErrIsTTY: true, OutIsTTY: true, Foreground: term.fg.Load,
	})
	return term
}

// erase is the bytes that blank a row painted with line.
func erase(line string) string { return "\r" + strings.Repeat(" ", len(line)) + "\r" }

// TestStatusLineRow drives a status line's whole life on a fake terminal: when it appears, how it
// gives way to a message and to content, how it settles, what it never does in the background or
// after the terminal refuses it, and what a Read blocked on its input reads as. The bytes it writes
// are pinned exactly, since they are all it does.
func TestStatusLineRow(t *testing.T) {
	upload := invocation{act: actionCopy, slot: "x"}
	connecting := func(secs int) string { return fmt.Sprintf("↑ connecting to h %ds", secs) }

	t.Run("a transfer settled within a second writes nothing of its own", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			term := newStatusTerm(newProgress(upload, "h"))
			time.Sleep(appearAfter - redrawEvery)
			fmt.Fprintln(term.std.Err, "buff: note") // no row to lift, so it passes straight through
			term.settle(errors.New("failed"))
			if got, want := term.err.String(), "buff: note\n"; got != want {
				t.Errorf("stderr = %q, want %q", got, want)
			}
		})
	})

	t.Run("past a second the row draws, a message lifts it, and success erases it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			term := newStatusTerm(newProgress(upload, "h"))
			time.Sleep(appearAfter)
			synctest.Wait()
			line := connecting(1)
			want := "\r\r" + line
			if got := term.err.String(); got != want {
				t.Fatalf("first draw = %q, want %q", got, want)
			}
			fmt.Fprintln(term.std.Err, "buff: note")
			want += erase(line) + "buff: note\n" + "\r\r" + line
			if got := term.err.String(); got != want {
				t.Fatalf("after a message = %q, want %q", got, want)
			}
			term.settle(nil)
			if got := term.err.String(); got != want+erase(line) {
				t.Errorf("after success = %q, want the row erased: %q", got, want+erase(line))
			}
		})
	})

	t.Run("a failure keeps the row and ends it with a newline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			term := newStatusTerm(newProgress(upload, "h"))
			time.Sleep(appearAfter)
			synctest.Wait()
			term.settle(errors.New("failed"))
			if got, want := term.err.String(), "\r\r"+connecting(1)+"\n"; got != want {
				t.Errorf("stderr = %q, want %q", got, want)
			}
		})
	})

	t.Run("the first content byte takes the row over for good", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			term := newStatusTerm(newProgress(upload, "h"))
			time.Sleep(appearAfter)
			synctest.Wait()
			io.WriteString(term.std.Out, "content")
			want := "\r\r" + connecting(1) + erase(connecting(1))
			time.Sleep(2 * time.Second) // ticks that must not draw across the content
			term.settle(errors.New("failed"))
			if got := term.err.String(); got != want {
				t.Errorf("stderr = %q, want the row erased and never redrawn: %q", got, want)
			}
			if got := term.out.String(); got != "content" {
				t.Errorf("stdout = %q, want the content untouched", got)
			}
		})
	})

	t.Run("a message that leaves the cursor mid-line retires the row", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			term := newStatusTerm(newProgress(upload, "h"))
			time.Sleep(appearAfter)
			synctest.Wait()
			fmt.Fprint(term.std.Err, "partial")
			want := "\r\r" + connecting(1) + erase(connecting(1)) + "partial"
			time.Sleep(2 * time.Second)
			term.settle(nil)
			if got := term.err.String(); got != want {
				t.Errorf("stderr = %q, want no row drawn onto the partial line: %q", got, want)
			}
		})
	})

	t.Run("in the background the row is never written", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			term := newStatusTerm(newProgress(upload, "h"))
			term.fg.Store(false)
			time.Sleep(2 * time.Second)
			synctest.Wait()
			if got := term.err.String(); got != "" {
				t.Fatalf("stderr = %q while in the background, want nothing", got)
			}
			term.fg.Store(true) // brought back: the next tick draws afresh
			time.Sleep(redrawEvery)
			synctest.Wait()
			want := "\r\r" + connecting(2)
			if got := term.err.String(); got != want {
				t.Fatalf("back in the foreground = %q, want %q", got, want)
			}
			term.fg.Store(false) // sent back before it ends: no newline, no erase on the shell's prompt
			term.settle(errors.New("failed"))
			if got := term.err.String(); got != want {
				t.Errorf("settled in the background = %q, want nothing more than %q", got, want)
			}
		})
	})

	t.Run("a terminal that refuses a write gets no more", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			writes := 0
			refusing := writerFunc(func([]byte) (int, error) { writes++; return 0, errors.New("input/output error") })
			_, settle := showProgress(newProgress(upload, "h"), IO{
				Out: io.Discard, Err: refusing, ErrIsTTY: true, OutIsTTY: true,
			})
			time.Sleep(3 * time.Second) // the line's first draw is refused, and nothing after it may try again
			settle(errors.New("failed"))
			if writes != 1 {
				t.Errorf("the line wrote %d times, want 1: nothing after the terminal refused its first draw", writes)
			}
		})
	})

	t.Run("a Read blocked on its input reads as waiting for input", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := newProgress(upload, "h")
			pr, pw := io.Pipe()
			body := p.attach(pr, 0)
			term := newStatusTerm(p)
			p.connected()
			go body.Read(make([]byte, 8)) // parks on the empty pipe, as a transport does on a quiet source
			time.Sleep(appearAfter)
			synctest.Wait()
			if got, want := term.err.String(), "\r\r↑ @x  waiting for input 1s"; got != want {
				t.Errorf("stderr = %q, want %q", got, want)
			}
			pw.Close()
			term.settle(nil)
		})
	})
}

// TestShowProgressGate pins when a transfer gets a status line: only with error output a terminal
// and standard output one no other process reads — the terminal itself, whose content then takes
// the row over, or a regular file. Off, the run's streams are returned untouched, which is what
// keeps its output byte for byte what it is without the feature.
func TestShowProgressGate(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		errTTY, outTTY, outFile bool
		wantLine, wantOutTapped bool
	}{
		{"error output not a terminal", false, true, false, false, false},
		{"standard output a pipe", true, false, false, false, false},
		{"standard output the terminal", true, true, false, true, true},
		{"standard output a regular file", true, false, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			std := IO{Out: io.Discard, Err: io.Discard, ErrIsTTY: tc.errTTY, OutIsTTY: tc.outTTY, OutIsFile: tc.outFile}
			got, settle := showProgress(newProgress(invocation{act: actionCopy}, "h"), std)
			defer settle(nil)
			if _, line := got.Err.(writerFunc); line != tc.wantLine {
				t.Errorf("status line on = %t, want %t", line, tc.wantLine)
			}
			if _, tapped := got.Out.(writerFunc); tapped != tc.wantOutTapped {
				t.Errorf("standard output routed through the line = %t, want %t", tapped, tc.wantOutTapped)
			}
		})
	}
}
