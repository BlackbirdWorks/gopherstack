package cloudwatchlogs

import "testing"

func TestNormalizeNamedGroupsAllocs(t *testing.T) { //nolint:paralleltest // AllocsPerRun panics under t.Parallel
	if got := testing.AllocsPerRun(50, func() { _ = normalizeNamedGroups(`(?<a>x) (?<b>y)`) }); got > 16 {
		t.Fatalf("normalizeNamedGroups allocs/op = %v, want <= 16 (regexp must be compiled once)", got)
	}
}
