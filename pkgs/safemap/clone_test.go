package safemap_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

func TestClonePtrMapDeepCopies(t *testing.T) {
	t.Parallel()

	type val struct{ N int }

	orig := map[string]*val{"a": {N: 1}, "b": {N: 2}, "nil": nil}
	clone := safemap.ClonePtrMap(orig)

	// Mutating the clone's values must not affect the original.
	clone["a"].N = 99
	if orig["a"].N != 1 {
		t.Fatalf("ClonePtrMap shared pointer: orig mutated to %d", orig["a"].N)
	}

	if clone["a"] == orig["a"] {
		t.Fatal("ClonePtrMap returned the same pointer instead of a copy")
	}

	if clone["nil"] != nil {
		t.Fatal("ClonePtrMap did not preserve nil value")
	}

	if len(clone) != len(orig) {
		t.Fatalf("ClonePtrMap length mismatch: got %d want %d", len(clone), len(orig))
	}
}

func TestClonePtrMapNil(t *testing.T) {
	t.Parallel()

	if got := safemap.ClonePtrMap[string, int](nil); got != nil {
		t.Fatalf("ClonePtrMap(nil) = %v, want nil", got)
	}
}

func TestCloneRegionalPtrMapDeepCopies(t *testing.T) {
	t.Parallel()

	type val struct{ N int }

	orig := map[string]map[string]*val{
		"us-east-1": {"r1": {N: 1}},
		"eu-west-1": {"r2": {N: 2}},
	}
	clone := safemap.CloneRegionalPtrMap(orig)

	clone["us-east-1"]["r1"].N = 99
	if orig["us-east-1"]["r1"].N != 1 {
		t.Fatalf("CloneRegionalPtrMap shared pointer: orig mutated to %d", orig["us-east-1"]["r1"].N)
	}

	// Adding to the clone's inner map must not affect the original.
	clone["us-east-1"]["new"] = &val{N: 5}
	if _, ok := orig["us-east-1"]["new"]; ok {
		t.Fatal("CloneRegionalPtrMap shared inner map")
	}

	if got := safemap.CloneRegionalPtrMap[string, string, val](nil); got != nil {
		t.Fatalf("CloneRegionalPtrMap(nil) = %v, want nil", got)
	}
}
