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
// through Missing below, never by spelling a feature string here: the capability vocabulary is the
// wire's, and keeping it on this side of the seam is what lets the cli — which may not import wire
// — gate on a capability while forwarding only opaque names.
func (h Health) supports(feature string) bool {
	return slices.Contains(h.Features, feature)
}

// Missing reports which of req the server does not advertise, preserving req's order; an empty
// result means it honours them all. A caller passes the capabilities its options demand — the wire
// names a PutOpts or GetOpts reports through Requires — and gates the operation when the result is
// non-empty. It is the one capability-check primitive the cli drives: the cli forwards Requires()
// straight here and names no feature string itself, so the wire vocabulary stays on the client side
// of the seam. An embedder wanting a single raw check passes one wire.Feature* and tests for an
// empty result.
func (h Health) Missing(req []string) []string {
	var miss []string
	for _, f := range req {
		if !h.supports(f) {
			miss = append(miss, f)
		}
	}
	return miss
}
