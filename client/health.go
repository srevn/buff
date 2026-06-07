package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	"github.com/srevn/buff/wire"
)

// Health is the server's liveness-and-capability report from /health. Unlike the content methods
// there is no domain type behind it — it is purely operational — so it is decoded directly here.
// Features is the capability list a caller checks before relying on an optional server feature.
type Health struct {
	Status   string   `json:"status"`
	Version  string   `json:"version"`
	API      []string `json:"api"`
	Features []string `json:"features"`
}

// Health probes the server's unversioned /health endpoint. It is the basis for a version or
// capability display, and a cheap liveness check. A non-2xx reply becomes a typed error through the
// reverse map, the same as any other request.
func (c *Client) Health(ctx context.Context) (Health, error) {
	resp, err := c.do(ctx, http.MethodGet, c.base+wire.PathHealth, nil, nil)
	if err != nil {
		return Health{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Health{}, responseError(resp)
	}
	defer drain(resp.Body)
	var h Health
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return Health{}, fmt.Errorf("buff: decoding health: %w", err)
	}
	return h, nil
}

// supports reports whether the server advertised a capability. It is unexported because callers ask
// through a typed predicate below, never by spelling a feature string: the capability vocabulary is
// the wire's, and keeping it on this side of the seam is what lets the cli — which may not import
// wire — gate on a capability while naming only a domain question.
func (h Health) supports(feature string) bool {
	return slices.Contains(h.Features, feature)
}

// ConditionalWrite reports whether the server interprets If-Match — whether a PutOpts.IfMatch will
// be honoured as a CAS rather than silently ignored. A caller checks it before a conditional write,
// because a server that lacks the capability replaces unconditionally: the clobber a CAS exists to
// prevent, which the caller would otherwise mistake for a satisfied precondition.
func (h Health) ConditionalWrite() bool {
	return h.supports(wire.FeatureConditionalWrite)
}

// FollowNext reports whether the server interprets Buff-Follow-Next — whether a GetOpts.FollowNext
// will skip the current value rather than be silently ignored. A caller checks it before a follow-
// next read, because a server that lacks the capability returns the current value, which the caller
// would mistake for the next one — the read-side twin of the ConditionalWrite gate.
func (h Health) FollowNext() bool {
	return h.supports(wire.FeatureFollowNext)
}
