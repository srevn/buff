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
// connection, a dropped one, a dial timeout, a cancelled context — as distinct from any
// status the server returned. It lives here, not in the domain package, because reaching
// the server is a transport concern the pure domain knows nothing about. Match it with
// errors.Is; the wrapped cause stays inspectable beneath it.
var ErrUnreachable = errors.New("buff: server unreachable")

// HTTPError is a non-2xx response the reverse map has no faithful single domain error for:
// a bad_request or internal sentinel (each of which the server produces from more than one
// cause, so the inverse cannot pick one), or a response with no Buff-Error at all — a
// server-generated 405 or 404, or an error page from a proxy in front of the server. It
// preserves the status and whatever sentinel was present so a caller can still report
// something precise, and a generic mapping above it can treat it as an unclassified error.
type HTTPError struct {
	Status   int    // the HTTP status code
	Sentinel string // the Buff-Error sentinel if one was present, else empty
}

// Error renders the status and, when present, the sentinel.
func (e *HTTPError) Error() string {
	if e.Sentinel != "" {
		return fmt.Sprintf("buff: server returned %d (%s)", e.Status, e.Sentinel)
	}
	return fmt.Sprintf("buff: server returned %d", e.Status)
}

// errRows is the reverse of the server's forward error map, built from the same canonical
// wire rows so neither side hand-types a sentinel string. It is keyed on the Buff-Error
// sentinel rather than the status because two conditions share 409 (busy and closed) and
// two share 400 (an invalid name and a generic bad request), so the status alone cannot
// disambiguate them — the sentinel can. Only the rows with a single faithful domain
// counterpart appear: bad_request and internal are deliberately absent, since each maps
// from more than one server-side cause and the inverse cannot honestly split it, so they
// fall through to a generic HTTPError.
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

// responseError turns a non-2xx response into a typed error and frees its body. It reads
// the Buff-Error sentinel, drains and closes the small constant error body so the
// connection can be reused, then resolves the sentinel: a known one becomes its domain
// error, wrapped with the sentinel string for the message while errors.Is still matches;
// an unknown or absent one becomes a generic HTTPError carrying the status. The body is
// never returned as the error text — it may carry a clip name or, from a foreign
// intermediary, hostile bytes — only the typed identity crosses back to the caller.
func responseError(resp *http.Response) error {
	sentinel := resp.Header.Get(wire.HeaderError)
	drain(resp.Body)
	for _, r := range errRows {
		if r.info.Sentinel == sentinel {
			return fmt.Errorf("%s: %w", sentinel, r.err)
		}
	}
	return &HTTPError{Status: resp.StatusCode, Sentinel: sentinel}
}

// drainLimit bounds how much of a response body drain reads back before closing. A buff
// error body is a single short sentinel line, so this never truncates one; the cap only
// guards against reading a large error page from a foreign intermediary just to recycle a
// connection — past the cap it is cheaper to close the connection than to keep reading.
const drainLimit = 4 << 10

// drain reads and closes a response body so its connection returns to the pool. Reading to
// the end is what lets net/http reuse the connection; an unread body forces a close
// instead. The read is bounded and its outcome ignored — this is best-effort connection
// hygiene on a path that is already returning, not a data path.
func drain(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, drainLimit))
	_ = rc.Close()
}
