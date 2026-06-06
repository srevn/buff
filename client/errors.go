package client

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// ErrUnreachable marks a failure to complete a request at the transport layer — a refused
// connection, a dropped one, a dial timeout, a canceled context — as distinct from any status
// the server returned. It lives here, not in the domain package, because reaching the server is
// a transport concern the pure domain knows nothing about. Match it with errors.Is; the wrapped
// cause stays inspectable beneath it. A caller that needs to tell a canceled or timed-out context
// apart from a genuine network failure therefore tests context.Canceled or context.DeadlineExceeded
// first: both surface wrapped under this one identity, so the broad match would otherwise swallow
// the distinction.
var ErrUnreachable = errors.New("buff: server unreachable")

// ErrSource marks a PUT whose request body could not be fully read: the caller's own source — a
// file, standard input, the tar producer — faulted under the transport's read, as distinct from
// the server being unreachable. net/http returns a single error for a failed round-trip whether the
// connection broke or the body it was reading did, collapsing the two; the client tells them apart
// by watching the body it was handed (see Put's recordingReader). It is the read-side counterpart
// to ErrUnreachable's connection side, and the request-direction mirror of the completion check GET
// applies to the response body — the client classifies a transfer by what it observes in the bytes,
// not by the symptom the transport reports. Match it with errors.Is; the underlying read error
// rides beneath, so a caller reports the device fault that truly occurred rather than a phantom
// network one.
var ErrSource = errors.New("buff: cannot read source")

// HTTPError is a non-2xx response the reverse map has no faithful single domain error for: a
// bad_request or internal sentinel (each of which the server produces from more than one cause, so
// the inverse cannot pick one), or a response with no Buff-Error at all — a server-generated 405 or
// 404, or an error page from a proxy in front of the server. It preserves the status and whatever
// sentinel was present so a caller can still report something precise, and a generic mapping above
// it can treat it as an unclassified error.
type HTTPError struct {
	Status   int    // the HTTP status code
	Sentinel string // the Buff-Error sentinel if one was present, else empty
}

// Error renders the status and, when present, the sentinel. The sentinel is quoted with %q because,
// unlike a mapped row's, it is whatever bytes a foreign peer put in the Buff-Error header — a
// proxy's prose, mojibake, a TAB, an attacker-chosen printable. Quoting escapes a control or high
// byte and delimits the value so it cannot pose as the surrounding message, the same refusal that
// drains an error body rather than echoing it. A genuine buff sentinel is lowercase ASCII, so it
// survives unchanged but for the quotes.
func (e *HTTPError) Error() string {
	if e.Sentinel != "" {
		return fmt.Sprintf("buff: server returned %d (%q)", e.Status, e.Sentinel)
	}
	return fmt.Sprintf("buff: server returned %d", e.Status)
}

// errRows is the reverse of the server's forward error map, built from the same canonical wire
// rows so neither side hand-types a sentinel string. It is keyed on the Buff-Error sentinel rather
// than the status because two conditions share 409 (busy and closed) and two share 400 (an invalid
// name and a generic bad request), so the status alone cannot disambiguate them — the sentinel can.
// Only the rows with a single faithful domain counterpart appear. Three are deliberately absent:
// bad_request and internal each map from more than one server-side cause, so the inverse cannot
// honestly split them; unavailable is a transient shutdown 503 a caller should retry rather than
// match as a domain error. All three fall through to a generic HTTPError.
var errRows = []struct {
	info wire.ErrInfo
	err  error
}{
	{wire.ErrNotFound, clip.ErrNotFound},
	{wire.ErrConsumed, clip.ErrConsumed},
	{wire.ErrBusy, clip.ErrBusy},
	{wire.ErrClosed, clip.ErrClosed},
	{wire.ErrTooLarge, clip.ErrTooLarge},
	{wire.ErrNoSpace, clip.ErrNoSpace},
	{wire.ErrNameBad, clip.ErrNameInvalid},
}

// responseError turns a non-2xx response into a typed error and frees its body. It reads the Buff-
// Error sentinel, drains and closes the small constant error body so the connection can be reused,
// then resolves the sentinel: a known one becomes its domain error directly. That domain error
// already carries the faithful user-facing line ("buff: clip not found"), and the wire sentinel
// is one-to-one with it, so re-prefixing the raw token would only restate — in protocol jargon —
// an identity the exit code already carries, and would leave this the one diagnostic that does not
// lead with "buff:". An unknown or absent sentinel instead becomes a generic HTTPError carrying the
// status and the quoted token — the one place the raw sentinel is worth showing, because there no
// domain message names the condition. The body is never returned as the error text — it may carry
// a clip name or, from a foreign intermediary, hostile bytes — only the typed identity crosses back
// to the caller.
func responseError(resp *http.Response) error {
	sentinel := resp.Header.Get(wire.HeaderError)
	drain(resp.Body)
	for _, r := range errRows {
		if r.info.Sentinel == sentinel {
			return r.err
		}
	}
	return &HTTPError{Status: resp.StatusCode, Sentinel: sentinel}
}

// drainLimit bounds how much of a response body drain reads back before closing. A buff error body
// is a single short sentinel line, so this never truncates one; the cap only guards against reading
// a large error page from a foreign intermediary just to recycle a connection — past the cap it is
// cheaper to close the connection than to keep reading.
const drainLimit = 4 << 10

// drain reads and closes a response body so its connection returns to the pool. Reading to the
// end is what lets net/http reuse the connection; an unread body forces a close instead. The read
// is bounded and its outcome ignored — this is best-effort connection hygiene on a path that is
// already returning, not a data path.
func drain(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, drainLimit))
	_ = rc.Close()
}
