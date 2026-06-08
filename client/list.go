package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// listEnvelope and listClip are the client's own decode of the list JSON, kept separate from the
// server's encoding type because the hexagon forbids importing it. The JSON field names are the one
// part of the wire contract not anchored in a shared constant — Go struct tags are string literals
// and cannot reference one — so their agreement with the server is guarded by a round-trip test
// rather than a shared symbol. The times arrive as RFC 3339 strings; an absent filename or expiry
// is simply an empty field after decoding.
type listEnvelope struct {
	Clips []listClip `json:"clips"`
	Next  string     `json:"next"`
}

type listClip struct {
	Name        string    `json:"name"`
	Generation  string    `json:"generation"`
	Kind        clip.Kind `json:"kind"`
	Filename    string    `json:"filename"`
	Executable  bool      `json:"executable"`
	Size        int64     `json:"size"`
	CreatedAt   string    `json:"created_at"`
	FinalizedAt string    `json:"finalized_at"`
	ExpiresAt   string    `json:"expires_at"`
	ConsumeOnce bool      `json:"consume_once"`
}

// List returns every finalized clip the server holds. The result is a non-nil slice, empty for an
// empty store, so a caller can range over it without a nil check. The list reports only finalized
// clips, so each carries Finalized true; the times are parsed back from their RFC 3339 strings,
// with an absent expiry left as the zero time.
func (c *Client) List(ctx context.Context) ([]clip.Clip, error) {
	resp, err := c.do(ctx, http.MethodGet, c.base+wire.PathClips, nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, responseError(resp)
	}
	defer drain(resp.Body)
	var env listEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("buff: decoding clip list: %w", err)
	}
	if env.Next != "" {
		// v1 returns the whole store in one page, so a non-empty cursor means a newer, paginating server.
		// Returning only the first page would hand back a silently truncated list — the same partial-
		// passed-as-whole the completion rule refuses on a read — so fail loud instead. The cursor itself
		// is non-actionable to a caller, so the message names the capability gap, not the value; when
		// this client learns to paginate, the guard becomes the fetch-and-concatenate loop that follows
		// the cursor, which is what env.Next is decoded for.
		return nil, errors.New("buff: server returned a paginated clip list this client cannot follow; upgrade the client")
	}
	out := make([]clip.Clip, 0, len(env.Clips))
	for _, lc := range env.Clips {
		out = append(out, lc.toClip())
	}
	return out, nil
}

// toClip projects a decoded list entry onto a clip.Clip. Every listed clip is finalized, so
// Finalized is set and the created and finalized instants are always present; an empty expiry
// parses to the zero time, the "no expiry" sentinel. The metadata is normalized as it crosses into
// the domain — the list JSON is a second foreign-server decode, so it clears a file-scoped field
// on a non-file kind exactly as parseClip does on GET/HEAD, before the clip reaches the listing
// renderer.
func (lc listClip) toClip() clip.Clip {
	return clip.Clip{
		Name:        lc.Name,
		Generation:  lc.Generation,
		Meta:        clip.Meta{Kind: lc.Kind, Filename: lc.Filename, Executable: lc.Executable}.Normalized(),
		Size:        lc.Size,
		CreatedAt:   parseTime(lc.CreatedAt),
		FinalizedAt: parseTime(lc.FinalizedAt),
		ExpiresAt:   parseTime(lc.ExpiresAt),
		ConsumeOnce: lc.ConsumeOnce,
		Finalized:   true,
	}
}
