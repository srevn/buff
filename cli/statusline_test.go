package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/srevn/buff/client"
)

// TestStatusText pins the words of every phase, from snapshots built by hand at a fixed now. Each
// row is one milestone set: which of them a transfer has passed is the whole of its state.
func TestStatusText(t *testing.T) {
	const mib = 1 << 20
	t0 := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return t0.Add(d * time.Second) }
	now := at(10)
	const host = "buff.example:8080"
	cases := []struct {
		name string
		s    status
		rate int64
		want string
	}{
		{"an upload not yet connected names the server",
			status{act: actionCopy, slot: "x", host: host, start: t0, attached: t0},
			0, "↑ connecting to buff.example:8080 10s"},
		{"a phase under a second shows no count",
			status{act: actionPaste, slot: "x", host: host, start: now.Add(-500 * time.Millisecond)},
			0, "↓ connecting to buff.example:8080"},
		{"a plain read waits for the server's answer",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1)},
			0, "↓ @x  waiting for server 9s"},
		{"a waiting read is parked for a write",
			status{act: actionPaste, slot: "x", get: client.GetOpts{Wait: true}, start: t0, connected: at(1)},
			0, "↓ @x  waiting for a write 9s"},
		{"a follow-next read is parked for the next write",
			status{act: actionPaste, slot: "x", get: client.GetOpts{FollowNext: true}, start: t0, connected: at(1)},
			0, "↓ @x  waiting for the next write 9s"},
		{"a moving upload quotes its rate",
			status{act: actionCopy, slot: "x", start: t0, attached: t0, connected: at(1), n: 48 * mib, moved: at(9)},
			mib, "↑ @x  48.0MiB  1.0MiB/s"},
		{"an upload whose source Read is in flight waits for input",
			status{act: actionCopy, slot: "x", start: t0, attached: t0, connected: at(1), n: 48 * mib, moved: at(2), reading: at(2)},
			mib, "↑ @x  48.0MiB  waiting for input 8s"},
		{"an upload with no Read in flight is stalled on the network",
			status{act: actionCopy, slot: "x", start: t0, attached: t0, connected: at(1), n: 48 * mib, moved: at(2)},
			mib, "↑ @x  48.0MiB  stalled 8s"},
		{"a still state before the first byte counts from the connect",
			status{act: actionCopy, slot: "x", start: t0, attached: t0, connected: at(1), reading: at(1)},
			0, "↑ @x  waiting for input 9s"},
		{"a drained upload waits for the server's answer",
			status{act: actionCopy, slot: "x", start: t0, attached: t0, connected: at(1), n: 48 * mib, moved: at(7), drained: at(7)},
			mib, "↑ @x  48.0MiB  waiting for server 3s"},
		{"a moving download of a finalized clip quotes its share",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1), attached: at(2), size: 100 * mib, n: 48 * mib, moved: at(9)},
			mib, "↓ @x  48.0MiB  48%  1.0MiB/s"},
		{"the share is truncated, so it reads 100% only once the last byte is in",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1), attached: at(2), size: 100 * mib, n: 100*mib - 1, moved: at(9)},
			mib, "↓ @x  100.0MiB  99%  1.0MiB/s"},
		{"a finalized download waiting on the network is stalled",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1), attached: at(2), size: 100 * mib, n: 48 * mib, moved: at(2), reading: at(2)},
			0, "↓ @x  48.0MiB  48%  stalled 8s"},
		{"a download with no Read in flight is writing",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1), attached: at(2), size: 100 * mib, n: 48 * mib, moved: at(2)},
			0, "↓ @x  48.0MiB  48%  writing 8s"},
		{"a live download waiting on the network waits for its writer",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1), attached: at(2), n: 48 * mib, moved: at(2), reading: at(2)},
			0, "↓ @x  48.0MiB  waiting for writer 8s"},
		{"a live download before its first byte counts from the answer",
			status{act: actionPaste, slot: "x", start: t0, connected: at(1), attached: at(2), reading: at(2)},
			0, "↓ @x  waiting for writer 8s"},
		{"a slot carrying a control byte is shown quoted",
			status{act: actionPaste, slot: "a\x1bb", start: t0, connected: at(1)},
			0, `↓ @"a\x1bb"  waiting for server 9s`},
		{"a long slot is cut to its share of the line",
			status{act: actionPaste, slot: "abcdefghijklmnop", start: t0, connected: at(1)},
			0, "↓ @abcdefg…  waiting for server 9s"},
		{"a long host is cut to its share of the line",
			status{act: actionCopy, slot: "x", host: "a-very-long-host.example.internal:8080", start: t0, attached: t0},
			0, "↑ connecting to a-very-long-host.example.… 10s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.s.text(now, tc.rate); got != tc.want {
				t.Errorf("text = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestStatusBegun pins the instant a transfer's rate and its stillness count from: once its stream
// is attached and a connection carries it, whichever came last. An upload attaches its source
// before it connects, a download connects before its answer attaches the body, and neither has
// begun until both have happened, so the rate never averages in time spent connecting or parked at
// the server.
func TestStatusBegun(t *testing.T) {
	t0 := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	for _, tc := range []struct {
		name string
		s    status
		want time.Time
	}{
		{"an upload's source attached, not yet connected", status{attached: t0}, time.Time{}},
		{"a download connected, its answer not yet in", status{connected: t0}, time.Time{}},
		{"an upload begins at its connect", status{attached: t0, connected: t1}, t1},
		{"a download begins at its answer", status{connected: t0, attached: t1}, t1},
	} {
		if got := tc.s.begun(); !got.Equal(tc.want) {
			t.Errorf("%s: begun = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestStatusTextFits renders every phase at its widest — a slot and a host far past their caps,
// units' widest size and rate, a full share, an hour-scale count — and requires each to fit lineMax
// bytes, the bound that keeps the line, and the blank that erases it, on one row of a 50-column
// terminal. A phase added to text without a row here is the gap this cannot see.
func TestStatusTextFits(t *testing.T) {
	const widest = 1<<30 - 1 // units renders this 1024.0MiB, its widest
	t0 := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	now := t0.Add(59*time.Minute + 59*time.Second)
	slot, host := strings.Repeat("s", 128), strings.Repeat("h", 60)+":65535"
	up := status{act: actionCopy, slot: slot, host: host, start: t0, attached: t0, connected: t0}
	down := status{act: actionPaste, slot: slot, host: host, start: t0, connected: t0}
	moving := func(s status) status { s.n, s.moved = widest, now; return s }
	still := func(s status, inFlight bool) status {
		s.n, s.moved = widest, t0
		if inFlight {
			s.reading = t0
		}
		return s
	}
	answered := func(s status, size int64) status { s.attached, s.size = t0, size; return s }
	for _, s := range []status{
		{act: actionCopy, host: host, start: t0},
		{act: actionPaste, slot: slot, get: client.GetOpts{FollowNext: true}, start: t0, connected: t0},
		{act: actionPaste, slot: slot, get: client.GetOpts{Wait: true}, start: t0, connected: t0},
		down,
		moving(up),
		still(up, true),
		still(up, false),
		func() status { s := still(up, false); s.drained = t0; return s }(),
		moving(answered(down, widest)),
		still(answered(down, widest), true),
		still(answered(down, widest), false),
		still(answered(down, 0), true),
	} {
		if got := s.text(now, widest); len(got) > lineMax {
			t.Errorf("%q is %d bytes, over the %d-byte line", got, len(got), lineMax)
		}
	}
}

// TestRateWindow pins the rate a line quotes: averaged from the instant bytes could first move, so
// time spent connecting dilutes nothing; over the newest span at least a window long once the
// transfer has run that far, so a change of pace shows within a window; and rounded up, so any
// movement at all reads above zero. Before the transfer begins there is no rate.
func TestRateWindow(t *testing.T) {
	t0 := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

	var w rateWindow
	w.add(at(250), time.Time{}, 0)
	if got := w.perSecond(); got != 0 {
		t.Errorf("before the transfer begins: %d B/s, want 0", got)
	}
	begun := at(3000) // three seconds connecting, which must not count
	for ms := 3250; ms <= 4000; ms += 250 {
		w.add(at(ms), begun, int64(ms-3000)) // 1000 B/s from begun
	}
	if got := w.perSecond(); got != 1000 {
		t.Errorf("a second in at 1000 B/s: %d B/s, want 1000 (connect time must not dilute it)", got)
	}
	n := int64(1000)
	for ms := 4250; ms <= 14000; ms += 250 {
		n += 500 // the pace halves
		w.add(at(ms), begun, n)
	}
	if got := w.perSecond(); got != 2000 {
		t.Errorf("ten seconds at 2000 B/s: %d B/s, want 2000 (the earlier pace aged out of the window)", got)
	}

	var trickle rateWindow
	trickle.add(at(5000), at(0), 1)
	if got := trickle.perSecond(); got != 1 {
		t.Errorf("one byte in five seconds: %d B/s, want 1 (rounded up, never 0 while moving)", got)
	}
}

// statusTerm is a status line's fake terminal: the two streams it writes, the IO a transfer runs
// under while the line lives, and the settle that ends it. It runs inside a synctest bubble, whose
// fake clock drives the line's ticker, so a test steps through the line's life by sleeping, and
// reads the streams after synctest.Wait — a happens-before edge from every goroutine the bubble has
// parked, so the plain buffers are safe to read.
type statusTerm struct {
	err, out bytes.Buffer
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
// gives way to a message and to content, how it settles, what it never does in the background, and
// what a Read blocked on its input reads as. The bytes it writes are pinned exactly, since they are
// all it does.
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
