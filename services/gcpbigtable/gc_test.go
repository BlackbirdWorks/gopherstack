package gcpbigtable_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGarbageCollectionPolicies(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ts := func(age time.Duration) bigtable.Timestamp {
		return bigtable.Time(now.Add(-age)).TruncateToMilliseconds()
	}

	// ages 0, 10m, 2h: newest first.
	ages := []time.Duration{0, 10 * time.Minute, 2 * time.Hour}

	tests := []struct {
		policy bigtable.GCPolicy
		name   string
		keep   []int
	}{
		{bigtable.NoGcPolicy(), "no policy", []int{0, 1, 2}},
		{bigtable.MaxVersionsPolicy(2), "max versions", []int{0, 1}},
		{bigtable.MaxVersionsPolicy(1), "single version", []int{0}},
		{bigtable.MaxAgePolicy(time.Hour), "max age", []int{0, 1}},
		{bigtable.MaxAgePolicy(time.Minute), "short max age", []int{0}},
		{bigtable.UnionPolicy(bigtable.MaxVersionsPolicy(1), bigtable.MaxAgePolicy(time.Hour)), "union deletes on either", []int{0}},
		{bigtable.IntersectionPolicy(bigtable.MaxVersionsPolicy(1), bigtable.MaxAgePolicy(time.Hour)), "intersection needs both", []int{0, 1}},
		{bigtable.IntersectionPolicy(bigtable.MaxVersionsPolicy(3), bigtable.MaxAgePolicy(time.Hour)), "intersection with loose versions", []int{0, 1, 2}},
		{bigtable.UnionPolicy(
			bigtable.IntersectionPolicy(bigtable.MaxVersionsPolicy(1), bigtable.MaxAgePolicy(5*time.Minute)),
			bigtable.MaxAgePolicy(90*time.Minute),
		), "nested", []int{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tbl := e.seeded(t, "t", "cf")
			require.NoError(t, e.ac.SetGCPolicy(t.Context(), "t", "cf", tt.policy))

			for i, age := range ages {
				put(t, tbl, seedCell{"r", "cf", "q", "v" + string(rune('0'+i)), ts(age)})
			}

			row, err := tbl.ReadRow(t.Context(), "r")
			require.NoError(t, err)

			var got []string
			for _, it := range row["cf"] {
				got = append(got, string(it.Value))
			}

			var want []string
			for _, i := range tt.keep {
				want = append(want, "v"+string(rune('0'+i)))
			}

			assert.Equal(t, want, got)
		})
	}
}

func TestGarbageCollectionAppliesToExistingData(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf")
	put(t, tbl, seedCell{"r", "cf", "q", "v1", 1000}, seedCell{"r", "cf", "q", "v2", 2000}, seedCell{"r", "cf", "q", "v3", 3000})
	assert.Len(t, readAll(t, tbl, bigtable.RowRange{}), 3)

	require.NoError(t, e.ac.SetGCPolicy(t.Context(), "t", "cf", bigtable.MaxVersionsPolicy(1)))
	assert.Equal(t, []string{"r|cf:q|3000|v3"}, readAll(t, tbl, bigtable.RowRange{}))
}

func TestGarbageCollectionOnWrite(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf")
	require.NoError(t, e.ac.SetGCPolicy(t.Context(), "t", "cf", bigtable.MaxVersionsPolicy(2)))

	for i := 1; i <= 5; i++ {
		put(t, tbl, seedCell{"r", "cf", "q", string(rune('0' + i)), bigtable.Timestamp(i * 1000)})
	}

	assert.Equal(t, []string{"r|cf:q|5000|5", "r|cf:q|4000|4"}, readAll(t, tbl, bigtable.RowRange{}))

	put(t, tbl, seedCell{"r", "cf", "q", "old", 1000})
	assert.Equal(t, []string{"r|cf:q|5000|5", "r|cf:q|4000|4"}, readAll(t, tbl, bigtable.RowRange{}), "a write below the retained versions is collected")
}

func TestGarbageCollectionPerFamily(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "one", "all")
	require.NoError(t, e.ac.SetGCPolicy(t.Context(), "t", "one", bigtable.MaxVersionsPolicy(1)))

	put(t, tbl,
		seedCell{"r", "one", "q", "a", 1000}, seedCell{"r", "one", "q", "b", 2000},
		seedCell{"r", "all", "q", "a", 1000}, seedCell{"r", "all", "q", "b", 2000})

	assert.Equal(t, []string{"r|all:q|2000|b", "r|all:q|1000|a", "r|one:q|2000|b"}, readAll(t, tbl, bigtable.RowRange{}))
}

func TestFullyCollectedRowsDisappear(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf")
	require.NoError(t, e.ac.SetGCPolicy(t.Context(), "t", "cf", bigtable.MaxAgePolicy(time.Hour)))

	put(t, tbl, seedCell{"stale", "cf", "q", "v", bigtable.Time(time.Now().Add(-3 * time.Hour))},
		seedCell{"fresh", "cf", "q", "v", bigtable.Now()})

	var keys []string

	require.NoError(t, tbl.ReadRows(t.Context(), bigtable.RowRange{}, func(r bigtable.Row) bool {
		keys = append(keys, r.Key())

		return true
	}))
	assert.Equal(t, []string{"fresh"}, keys)
}
