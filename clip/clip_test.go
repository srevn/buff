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
