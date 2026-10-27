package gcpbigquery

import "testing"

func TestOptionsMetaLabelsAllocs(t *testing.T) { //nolint:paralleltest // AllocsPerRun panics under t.Parallel
	opts := map[string]string{"labels": `[("a","b"),("c","d")]`}

	if got := testing.AllocsPerRun(50, func() { _ = optionsMeta(opts) }); got > 40 {
		t.Fatalf("optionsMeta allocs/op = %v, want <= 40 (labels regexp must be compiled once)", got)
	}
}
