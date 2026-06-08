package client_test

import (
	"slices"
	"testing"
	"time"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/wire"
)

// TestRequiresCoversGatedFeatures is the structural anchor that makes capability-gate completeness
// a build property rather than author memory: it ties what the option types can demand to the gated
// set wire declares, in both directions. Per-option it pins the exact capability each gated field
// maps to, so a mapping that names the wrong feature (IfMatch → follow-next) fails here even though
// the union alone would not notice the swap. It then proves the union of every option's Requires
// equals wire.GatedFeatures exactly — a gated feature with no Requires clause makes the union miss
// it; a Requires clause for a non-gated feature makes the union exceed it. A future gated field
// left unset in the maximal opts below makes the union miss its feature and fails here too: the
// omission is loud.
func TestRequiresCoversGatedFeatures(t *testing.T) {
	// Per-option exact mappings: a gated field in isolation demands exactly its one capability, and an
	// option with only non-gated fields demands nothing — so an ordinary copy or paste pays no gate,
	// and a Requires that keyed on the wrong field would fail one of these.
	pins := []struct {
		name string
		got  []string
		want []string
	}{
		{"if-match", client.PutOpts{IfMatch: "g"}.Requires(), []string{wire.FeatureConditionalWrite}},
		{"plain put", client.PutOpts{Keep: true, ConsumeOnce: true, TTL: time.Hour}.Requires(), nil},
		{"follow-next", client.GetOpts{FollowNext: true}.Requires(), []string{wire.FeatureFollowNext}},
		{"plain get", client.GetOpts{}.Requires(), nil},
	}
	for _, p := range pins {
		if !slices.Equal(p.got, p.want) {
			t.Errorf("%s Requires() = %v, want %v", p.name, p.got, p.want)
		}
	}

	// The union over a maximal opts — every gated field set at once — must equal the declared gated
	// set. A feature in the union but not GatedFeatures is gated without a need; one in GatedFeatures
	// but not the union is a capability declared gated with no option left to demand it — a gate
	// silently dropped.
	union := sortedSet(append(
		client.PutOpts{IfMatch: "g", Keep: true, ConsumeOnce: true, TTL: time.Hour}.Requires(),
		client.GetOpts{FollowNext: true}.Requires()...,
	))
	if want := sortedSet(wire.GatedFeatures); !slices.Equal(union, want) {
		t.Errorf("⋃Requires = %v, want wire.GatedFeatures = %v", union, want)
	}
}

// sortedSet returns the distinct elements of s, sorted, for an order-insensitive set comparison. It
// clones first so sorting never mutates a caller's slice — wire.GatedFeatures in particular.
func sortedSet(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return slices.Compact(out)
}

// TestNewRejects pins the base-URL validation New performs at construction, so a config typo
// becomes a clear error there rather than a corrupt request URL later. A scheme that is not http
// or https, a missing host, and a query or fragment — each of which would splice into the middle
// of every request URL, since the path and escaped name are appended to the raw base — are all
// rejected. A well-formed URL, including one with a trailing slash to trim, is accepted.
func TestNewRejects(t *testing.T) {
	bad := []struct{ name, url string }{
		{"empty", ""},
		{"no scheme", "host:8080"},
		{"wrong scheme", "ftp://host"},
		{"no host", "http://"},
		{"query", "http://host:8080?x=1"},
		{"fragment", "http://host:8080#frag"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.New(tc.url, nil); err == nil {
				t.Errorf("New(%q) = nil error, want a rejection", tc.url)
			}
		})
	}

	for _, ok := range []string{"http://host:8080", "http://host:8080/", "https://example.com/prefix"} {
		if _, err := client.New(ok, nil); err != nil {
			t.Errorf("New(%q) = %v, want it accepted", ok, err)
		}
	}
}
