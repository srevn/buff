package cli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	// appearAfter is how long a transfer runs before its status line appears: long enough that a
	// fast transfer never draws one, short enough to answer "is it hung?" while the question is still
	// being asked. It gates on time, not size, because a small clip on a dead link hangs as long as a
	// large one, and a piped stream, a tar, or a live clip has no size to gate on.
	appearAfter = time.Second
	// redrawEvery is the line's cadence, lively enough for a rate to read as current. A redraw whose
	// text is unchanged writes nothing until the row is refreshEvery old, so a still line touches the
	// terminal about once a second, whether its count is of seconds or of hours.
	redrawEvery = 250 * time.Millisecond
	// refreshEvery is how long the row trusts its memory of the screen. The process cannot read the
	// screen back, and it changes under the row unseen: a job stopped with Ctrl-Z and brought back
	// with fg resumes on a fresh row below the one it drew, and another process's output, or a
	// resize's reflow, pushes the row up. So an unchanged row is painted again once it is this old —
	// in place where the screen still holds it, back where the screen lost it — at the write rate a
	// still line's count of seconds already costs.
	refreshEvery = time.Second
)

// showProgress puts p's status line on the terminal when that terminal is the run's alone to draw
// on, returning the IO the transfer must write through and the settle that ends the line — unless
// ctx, the run's, is canceled first, which ends it at once. With the line off it returns std
// untouched and a settle that does nothing, so the run writes exactly what it would with no line at
// all.
//
// The terminal is the run's alone when standard error is a terminal — someone may be watching — and
// standard output reaches no other process that could write to that screen: it is a terminal, where
// any content the run shows takes the line's row over, or a regular file, which nothing reads while
// the run goes on. A terminal on standard output is taken for the one the line draws on; were it
// another, its content would only retire the line early, never be drawn over. A pipe is what this
// rules out. The next stage of `buff @log | grep x` or `| less` writes to the same screen, which a
// line rewritten in place would scribble on, and in `buff @a | buff @b` two lines would fight over
// one row — there only the copy end, whose standard output is the terminal, draws. Any other
// standard output keeps the line off too, so a run whose standard output carries nothing it needs —
// a copy, or a paste given -o a path — is silenced by redirecting it to /dev/null, with no flag. A
// paste that saves or extracts at a terminal has no such spare stream: its landing is chosen by
// standard output being the terminal, so standard error is the one to redirect there.
func showProgress(ctx context.Context, p *progress, std IO) (IO, func(error)) {
	if !std.ErrIsTTY || !(std.OutIsTTY || std.OutIsFile) {
		return std, func(error) {}
	}
	s := &statusLine{
		p: p, out: std.Out, err: std.Err, foreground: std.foreground,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go s.run(ctx)
	std.Err = writerFunc(s.message)
	if std.OutIsTTY {
		std.Out = writerFunc(s.content)
	}
	return std, s.settle
}

// statusLine owns one terminal row for the life of a transfer: it redraws the transfer's progress
// there, lifts the row off for anything else the run writes to the terminal, and settles it into
// the transfer's end state. It is the run's only writer to the terminal while it lives —
// showProgress routes the run's standard error through it, and standard output too when that is a
// terminal — which is what lets every line the run prints land on a row of its own, never glued to
// the status.
//
// Two locks keep a transfer's bytes from ever waiting on the terminal. The progress lock guards the
// transfer's facts and is never held across IO; mu serialises the terminal's writes and is the one
// lock held across one, so a draw blocked on a frozen terminal (XOFF, a stalled pty) holds up only
// what writes to that terminal, which the terminal would hold up regardless. The two are never held
// together: run snapshots the progress before it takes mu. Every hold of mu ends in a defer, so a
// writer or a Foreground that panics under it cannot leave it held for the settle that transfer
// runs as the panic unwinds.
type statusLine struct {
	p          *progress
	out        io.Writer     // the real standard output, which content passes through to untouched
	err        io.Writer     // the real standard error: the row, and every message above it
	foreground func() bool   // whether the process holds the terminal's foreground right now
	stop       chan struct{} // closed under mu to retire the row, by whichever of its ends comes first
	done       chan struct{} // closed by run as it returns; settle waits on it

	mu      sync.Mutex
	drawn   string    // the row as last painted; "" while it is clear
	painted time.Time // when drawn was written, which a redraw repeating it waits refreshEvery on
}

// run redraws the row every redrawEvery until the row is retired, once the transfer has run for
// appearAfter. It samples the transfer's count on every tick from the first, so the rate the first
// draw quotes is already averaged over what the transfer has done.
//
// A canceled run ends the row at once, before its flow settles it. The flow may take its time to
// unwind — a sink blocked in a write that ignores ctx — and the process need not wait for it, a
// second Ctrl-C exiting at once; a row still redrawn meanwhile would draw over the ^C the terminal
// echoed, and the exit would leave the cursor mid-row for the shell to write after.
func (s *statusLine) run(ctx context.Context) {
	defer close(s.done)
	tick := time.NewTicker(redrawEvery)
	defer tick.Stop()
	var rate rateWindow
	for {
		select {
		case <-s.stop:
			return
		case <-ctx.Done():
			s.end()
			return
		case <-tick.C:
			st, now := s.p.snapshot()
			rate.add(now, st.begun(), st.n)
			if now.Sub(st.start) >= appearAfter {
				s.draw(st.text(now, rate.perSecond()))
			}
		}
	}
}

// draw paints line onto the row, unless the row is retired — checked under mu, which every
// retirement also holds, so no draw can land after one — or line repeats the row's text and the
// row is younger than refreshEvery.
func (s *statusLine) draw(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired() || (line == s.drawn && time.Since(s.painted) < refreshEvery) {
		return
	}
	s.paint(line)
}

// paint replaces the row with line in a single write — "" erases it — and is the only way the
// row's text changes; mu is held. It blanks the old text by its length in bytes before drawing the
// new: a string never renders wider than its byte count, so the blank covers whatever was drawn
// however wide its glyphs came out, and lineMax keeps the blank itself on one row. The cursor is
// left just past the new text, so a ^C echo, or the newline that ends the row, follows the words.
// Each write starts at the left edge of the cursor's row, so the first draw covers anything another
// writer left unfinished there: telling where the cursor stands would mean reading the terminal,
// which no line redrawn with a carriage return does.
//
// Only a process in the terminal's foreground writes to it. A background job's write lands on the
// shell's prompt — or, under `stty tostop`, stops the job outright, stalling the transfer the line
// reports — so there paint writes nothing and forgets the row instead: the job left it behind in
// the scrollback when it left the foreground, and a later paint starts afresh on whatever row the
// cursor is on by then. A write that fails retires the row and forgets it too: a terminal that
// refused a write gets no more.
func (s *statusLine) paint(line string) {
	if line == "" && s.drawn == "" {
		return // nothing on the row, and nothing to put there
	}
	if !s.foreground() {
		s.drawn = ""
		return
	}
	if _, err := io.WriteString(s.err, "\r"+strings.Repeat(" ", len(s.drawn))+"\r"+line); err != nil {
		s.drawn = ""
		s.retire()
		return
	}
	s.drawn, s.painted = line, time.Now()
}

// message is the run's standard error while the row lives. It lifts the row off, writes p whole,
// and puts the row back beneath it, so a warning or a notice printed mid-transfer — a skipped
// source entry, a consume-once spent, a landing narrated — reads as a line of its own above the
// status. A write that does not end its line leaves the cursor mid-row, where any redraw would glue
// the status onto it, so the row retires instead of returning.
func (s *statusLine) message(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	line := s.drawn
	s.paint("")
	n, err := s.err.Write(p)
	switch {
	case !bytes.HasSuffix(p, []byte("\n")):
		s.retire()
	case !s.retired():
		s.paint(line)
	}
	return n, err
}

// content is the run's standard output while the row lives and that output is a terminal. The
// first byte the run shows there takes the row over for good — cleared, so nothing draws across
// what follows — since from then on the content is its own progress: a paste that waited under a
// line and then streams its clip onto the terminal must not have the line drawn through it.
func (s *statusLine) content(p []byte) (int, error) {
	s.clear()
	return s.out.Write(p)
}

// settle ends the row with the transfer's outcome, then waits for run to return so no goroutine
// outlives the transfer. Success clears the row, so success stays a silent zero on the terminal as
// in the exit status, whatever the link's speed made of the transfer — a run that drew nothing
// writes nothing. Failure ends it, kept as the error's context, so the diagnostic Run prints next
// lands on a line of its own, beneath what the transfer last said. A row already gone — ended by a
// cancellation, cleared by content — leaves either nothing to do.
func (s *statusLine) settle(err error) {
	if err != nil {
		s.end()
	} else {
		s.clear()
	}
	<-s.done
}

// clear retires the row and erases it, handing the terminal back as the line found it: the end for
// content, which takes the row over, and for success.
func (s *statusLine) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retire()
	s.paint("")
}

// end retires the row and keeps it, closing it with a newline so that whatever the run prints next
// lands beneath it: the end for failure and for cancellation. In the background it writes nothing,
// as paint does there, and the row stays in the scrollback where the job left it.
func (s *statusLine) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retire()
	if s.drawn != "" && s.foreground() {
		io.WriteString(s.err, "\n")
	}
	s.drawn = ""
}

// retire ends the row's life: run returns and no draw follows, so the retiring site's own erase or
// newline is the last the row writes. It is only ever called with mu held, which is what makes its
// check-then-close safe from every site that retires the row.
func (s *statusLine) retire() {
	if !s.retired() {
		close(s.stop)
	}
}

// retired reports whether the row has been retired.
func (s *statusLine) retired() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// writerFunc adapts a function to an io.Writer, which is how the status line installs its message
// and content methods as the run's standard streams.
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
