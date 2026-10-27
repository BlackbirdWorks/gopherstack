package neptune

import "testing"

func BenchmarkWireMessageNotFound(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		_ = wireMessage("DBClusterNotFoundFault", "instance db-1 not found")
	}
}

func TestWireMessageAllocs(t *testing.T) { //nolint:paralleltest // AllocsPerRun panics under t.Parallel
	if got := testing.AllocsPerRun(50, func() {
		_ = wireMessage("DBClusterNotFoundFault", "instance db-1 not found")
	}); got > 8 {
		t.Fatalf("wireMessage allocs/op = %v, want <= 8 (regexps must be compiled once)", got)
	}
}
