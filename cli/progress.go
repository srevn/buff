package cli

import (
	"io"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/units"
)

// window is the span a transfer's rate is averaged over, and the stillness after which its status
// line stops quoting a rate and names what the transfer waits on instead. One constant for both
// keeps the two readings exclusive: a line quoting a rate has movement inside the span it averages,
// and a line calling the transfer still has seen none for a whole span. Five seconds is what
// absorbs the bursts an upload moves in — the client counts what the kernel accepts, and on a slow
// link the kernel takes bytes a few hundred KiB at a time, seconds apart, as the server's receive
// window reopens — which a shorter span would read as flapping between a rate and a stall.
const window = 5 * time.Second

// The line is held to lineMax bytes, which is what keeps it on one row of a 50-column terminal
// without probing the terminal's width: no terminal renders a string wider than its byte count, so
// bytes bound columns whether the line's arrows and ellipsis render one column wide, two (an East
// Asian ambiguous-width setting), or three (a terminal reading UTF-8 as a single-byte code page).
// Every other field is bounded by what units renders at its widest — 1024.0MiB, a span of 59m — so
// the two free-text fields are each held to what the widest phase leaves them: the slot to slotMax,
// against "↓ @slot  1024.0MiB  waiting for writer 59m", and the host to hostMax, against "↑
// connecting to host 59m".
const (
	lineMax = 50
	slotMax = 11
	hostMax = 28
)

// status is what is known about a transfer at one instant — a plain value, copied out whole from
// the progress that records it, so rendering one takes no lock and a test can build one by hand.
// Each milestone is the instant it became true, zero while it has not, so the facts carry no flags
// and the milestones a transfer has passed are the state it is in.
type status struct {
	act   action         // actionCopy, an upload (↑), or actionPaste, a download (↓)
	slot  string         // the slot addressed, as the user typed it
	host  string         // the server being reached, which the connecting phase names
	get   client.GetOpts // a paste's read options, which name what its unanswered request waits for
	start time.Time      // when the transfer began

	connected time.Time // a connection carries the request
	attached  time.Time // the stream was handed over: an upload's source to the transport, a download's answer to its sink
	size      int64     // the length the server declared for the stream — a finalized clip's — or 0 for none
	n         int64     // bytes that crossed the stream so far
	reading   time.Time // a Read on the stream has been in flight since then; zero between Reads
	moved     time.Time // the last Read that carried bytes
	drained   time.Time // the stream reached its clean end
}

// progress is the live status of one transfer, written by the goroutines that move its bytes — the
// flow's own, a transport's, and the client's connect hook — and read whole by the status line. Its
// lock guards field updates and nothing else: no method of progress does IO, so a transfer's bytes
// never wait on whatever is reading its progress, a status line blocked writing to a frozen
// terminal included.
type progress struct {
	mu sync.Mutex
	st status
}

// newProgress opens the record of the transfer inv names, toward the server at host.
func newProgress(inv invocation, host string) *progress {
	return &progress{st: status{act: inv.act, slot: inv.slot, host: host, get: inv.get, start: time.Now()}}
}

// connected is the client's connect hook, so it may run on a transport goroutine. It keeps the
// first stamp: every connection a request obtains fires it, and Put's best-effort cleanup is a
// second request on top of the first.
func (p *progress) connected() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.st.connected.IsZero() {
		p.st.connected = time.Now()
	}
}

// attach hands the progress the stream its transfer moves and returns that stream wrapped, so that
// every Read through it is recorded. declared is the length the server declared for the stream: a
// finalized clip's size, which the client reports as 0 for a live one, and 0 for any upload, whose
// length the server measures rather than receives. For a download a declared length therefore
// means exactly a finalized clip — the server sends a size for nothing else — so the line reads a
// zero as live; the one clip that misreads, a finalized empty one, ends on its first Read, before
// any line could show it.
//
// The reader returned has no Close, deliberately. An upload lends it to a transport that closes
// whatever body it is handed, and the flow, not the transport, owns the source's Close — where a
// file descriptor is released — so a reader that cannot be closed is what keeps the two from racing
// over it.
func (p *progress) attach(r io.Reader, declared int64) io.Reader {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.attached, p.st.size = time.Now(), declared
	return progressReader{r: r, p: p}
}

// readStarted records that a Read on the stream is in flight from now.
func (p *progress) readStarted() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.reading = time.Now()
}

// readDone records the Read that was in flight returning n bytes and err: the bytes join the count,
// a Read that carried any marks the transfer moving, and a clean io.EOF marks the stream drained.
func (p *progress) readDone(n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.st.reading = time.Time{}
	p.st.n += int64(n)
	if n > 0 {
		p.st.moved = now
	}
	if err == io.EOF {
		p.st.drained = now
	}
}

// snapshot copies the transfer's status out whole.
func (p *progress) snapshot() status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.st
}

// progressReader is a transfer's stream as attached to its progress.
type progressReader struct {
	r io.Reader
	p *progress
}

// Read passes the stream's Read through untouched, an (n > 0, err) pair included, and records it
// on either side. The instant it began is what tells, when the transfer stops moving, which end the
// loop reading this stream is waiting on; what it returned moves the count. Each record is its own
// short critical section, so the progress lock is never held across the Read itself.
func (r progressReader) Read(b []byte) (int, error) {
	r.p.readStarted()
	n, err := r.r.Read(b)
	r.p.readDone(n, err)
	return n, err
}

// begun is the instant the transfer's bytes could first move: once its stream is attached and a
// connection carries it, whichever came last — an upload's connect, a download's answer. It is
// zero until both have happened, so neither time spent connecting nor time parked at the server
// counts as time the transfer was still.
func (s status) begun() time.Time {
	if s.attached.IsZero() || s.connected.IsZero() {
		return time.Time{}
	}
	return later(s.attached, s.connected)
}

// text renders s as the words of the status line at now, quoting rate — bytes a second, averaged
// over the last window — while the transfer moves. The phases are the order an HTTP exchange runs
// in, and the two directions differ only in where the body sits in it: an upload's travels before
// its answer, a download's after.
//
//	upload:   connecting → moving ⇄ still → waiting for server
//	download: connecting → waiting for its answer → moving ⇄ still
//
// The connecting line names the server rather than the slot, since the server is what the
// transfer is waiting to reach. Every other line leads with the direction and the slot. A phase
// that has lasted a second or more ends with how long. The slot is the user's own argv and the
// client does not vet names before sending them, so the line can be drawn before the server
// refuses one: both free-text fields go through safeField, so one carrying a control byte is shown
// quoted, never acted on.
func (s status) text(now time.Time, rate int64) string {
	if s.connected.IsZero() {
		return s.arrow() + " connecting to " + fit(safeField(s.host), hostMax) + since(now, s.start)
	}
	head := s.arrow() + " " + fit("@"+safeField(s.slot), slotMax) + "  "
	switch {
	case s.act == actionPaste && s.attached.IsZero():
		return head + parked(s.get) + since(now, s.connected)
	case s.act == actionCopy && !s.drained.IsZero():
		// The source is drained and what remains is the server's: taking in the bytes still buffered on
		// the way, then committing the clip. HTTP acknowledges an upload only with its answer, so until
		// then the count stands where the source left it.
		return head + s.tally() + "waiting for server" + since(now, s.drained)
	case s.n > 0 && now.Sub(s.moved) < window:
		return head + s.tally() + units.Size(rate) + "/s"
	default:
		// Still since the last byte moved, or — before the first — since bytes could first move: a still
		// state shows at once rather than after a window, since before any byte there is no burst to
		// absorb.
		return head + s.tally() + s.still() + since(now, later(s.moved, s.begun()))
	}
}

// still names what a transfer that has stopped moving is waiting on — never a guessed cause, since
// the stream shows only which end is quiet, not why. The loop moving the stream reads one side of
// it and writes the other, and a Read in flight on the stream says which: in flight, the loop waits
// on the side it reads from; idle, it is held up by the side it writes to. An upload's transport
// reads the source and writes the network; a download's sink reads the network and writes the
// disk. A download waiting on the network is waiting on two different things by the clip it reads:
// a finalized clip is wholly at the server, so only the path can be holding it up, while a live
// one's next bytes are its writer's to send — and the stream carries no heartbeat that could tell
// a quiet writer from a broken path, so the line names what it waits for and not why.
func (s status) still() string {
	switch {
	case s.act == actionCopy && !s.reading.IsZero():
		return "waiting for input"
	case s.act == actionCopy:
		return "stalled"
	case s.reading.IsZero():
		return "writing"
	case s.size > 0:
		return "stalled"
	default:
		return "waiting for writer"
	}
}

// tally renders the bytes moved so far — with the share of a declared length, when there is one —
// and the gap that sets it off from what follows, or nothing before the first byte, where a 0B
// would only crowd the line. The share is truncated, so it reads 100% only once the last byte is
// in, and computed in floating point, so a length near int64's ceiling cannot overflow it.
func (s status) tally() string {
	if s.n == 0 {
		return ""
	}
	t := units.Size(s.n)
	if s.size > 0 && s.n <= s.size {
		t += "  " + strconv.Itoa(int(float64(s.n)/float64(s.size)*100)) + "%"
	}
	return t + "  "
}

// arrow is the line's mark for the transfer's direction: up to the server, or down from it.
func (s status) arrow() string {
	if s.act == actionCopy {
		return "↑"
	}
	return "↓"
}

// parked names what a download's unanswered request waits for. The server parks a waiting read
// until a write lands and a follow-next read until the next one does, before it sends any header,
// so the request reaching a connection is the last thing the client sees until then; a plain read
// waits only for the server's answer.
func parked(o client.GetOpts) string {
	switch {
	case o.FollowNext:
		return "waiting for the next write"
	case o.Wait:
		return "waiting for a write"
	default:
		return "waiting for server"
	}
}

// since renders how long a phase has lasted, from t to now, with the space that sets it off — or
// nothing under a second, where a count would only flicker, and nothing for a phase with no start
// recorded, where a count from the zero instant would read as centuries.
func since(now, t time.Time) string {
	if d := now.Sub(t); !t.IsZero() && d >= time.Second {
		return " " + units.Duration(d)
	}
	return ""
}

// later returns the later of two instants, which time.Time offers no ordered max for.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// fit cuts s to at most limit bytes, ending it in an ellipsis when it had to cut, and never inside
// a rune.
func fit(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit - len("…")
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
