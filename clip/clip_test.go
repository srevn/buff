package clip_test

import (
	"testing"

	"github.com/srevn/buff/clip"
)

func TestKindValid(t *testing.T) {
	tests := []struct {
		in    clip.Kind
		valid bool
	}{
		{clip.KindBytes, true},
		{clip.KindFile, true},
		{clip.KindArchive, true},
		{clip.Kind(""), false},         // absent is not a kind; defaulting is the HTTP layer's job
		{clip.Kind("BYTES"), false},    // exact match, no case folding
		{clip.Kind("bytes "), false},   // no trimming
		{clip.Kind("binary"), false},   // unknown
		{clip.Kind("archives"), false}, // near miss
	}
	for _, tt := range tests {
		if got := tt.in.Valid(); got != tt.valid {
			t.Errorf("Kind(%q).Valid() = %v, want %v", string(tt.in), got, tt.valid)
		}
	}
}

// TestMetaNormalized pins the cross-field rule the flat product cannot itself hold: Executable
// survives only on a file clip, Filename only on a file or an archive, and an empty or unknown kind
// carries neither — while the kind is never rewritten. The conforming shapes pass through untouched,
// which is what proves the normalizer can only sanitise an illegal combination, never regress a legal
// one; idempotence is checked on every row so the function is safe to apply at more than one seam.
func TestMetaNormalized(t *testing.T) {
	tests := []struct {
		name string
		in   clip.Meta
		want clip.Meta
	}{
		{"bytes carries nothing", clip.Meta{Kind: clip.KindBytes}, clip.Meta{Kind: clip.KindBytes}},
		{"bytes drops both file-scoped fields", clip.Meta{Kind: clip.KindBytes, Filename: "x", Executable: true}, clip.Meta{Kind: clip.KindBytes}},
		{"file keeps both", clip.Meta{Kind: clip.KindFile, Filename: "prog", Executable: true}, clip.Meta{Kind: clip.KindFile, Filename: "prog", Executable: true}},
		{"file without the bit is unchanged", clip.Meta{Kind: clip.KindFile, Filename: "doc"}, clip.Meta{Kind: clip.KindFile, Filename: "doc"}},
		{"archive keeps the name, drops the bit", clip.Meta{Kind: clip.KindArchive, Filename: "tree", Executable: true}, clip.Meta{Kind: clip.KindArchive, Filename: "tree"}},
		{"empty kind drops both", clip.Meta{Filename: "x", Executable: true}, clip.Meta{}},
		{"unknown kind drops both but stays raw", clip.Meta{Kind: clip.Kind("bogus"), Filename: "x", Executable: true}, clip.Meta{Kind: clip.Kind("bogus")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.Normalized()
			if got != tt.want {
				t.Errorf("Normalized() = %+v, want %+v", got, tt.want)
			}
			if again := got.Normalized(); again != got {
				t.Errorf("not idempotent: second pass = %+v, want %+v", again, got)
			}
		})
	}
}
