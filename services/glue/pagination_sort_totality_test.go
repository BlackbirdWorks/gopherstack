package glue_test

import (
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// walkPaginated replays paginateSlice's own offset-token semantics
// (handler.go) against fetchAll, run repeatedly. A total (unique-keyed) sort
// returns the exact same order on every call, so every walk reproduces the
// full ID set exactly. An unstable sort re-computed fresh from unordered
// map storage on each call can disagree between two honest calls about the
// relative order of tied items, dropping or duplicating a record across a
// page boundary with nothing else changed -- this is caught by running many
// iterations, since Go's map iteration order is randomized per range.
func walkPaginated[T any](
	t *testing.T,
	iterations, pageSize int,
	fetchAll func() []T,
	id func(T) string,
	wantIDs []string,
) {
	t.Helper()

	wantSorted := append([]string(nil), wantIDs...)
	sort.Strings(wantSorted)

	for iter := range iterations {
		var got []string

		token := ""
		for {
			all := fetchAll()

			start := 0
			if token != "" {
				if n, err := strconv.Atoi(token); err == nil && n > 0 && n < len(all) {
					start = n
				}
			}

			if start >= len(all) {
				break
			}

			end := start + pageSize

			var next string
			if end < len(all) {
				next = strconv.Itoa(end)
			} else {
				end = len(all)
			}

			for _, item := range all[start:end] {
				got = append(got, id(item))
			}

			token = next
			if token == "" {
				break
			}
		}

		gotSorted := append([]string(nil), got...)
		sort.Strings(gotSorted)

		require.Equalf(
			t,
			wantSorted,
			gotSorted,
			"iteration %d: paginated walk (page size %d) produced %v, want exactly %v (no drop/dup across a page boundary)",
			iter,
			pageSize,
			got,
			wantIDs,
		)
	}
}

const tieWalkIterations = 30

func TestGetBlueprintRuns_TotalSortAcrossTiedStartedOn(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateBlueprint("bp1", "s3://bucket/bp1", "", nil)
	require.NoError(t, err)

	const n = 6

	want := make([]string, 0, n)

	for range n {
		run, runErr := b.StartBlueprintRun("bp1", "", "")
		require.NoError(t, runErr)
		want = append(want, run.RunID)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.BlueprintRun { return b.GetBlueprintRuns("bp1") },
		func(r *glue.BlueprintRun) string { return r.RunID },
		want)
}

func TestListColumnStatisticsTaskRuns_TotalSortAcrossTiedStartedOn(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 6

	want := make([]string, 0, n)

	for range n {
		run, err := b.StartColumnStatisticsTaskRun("db1", "tbl1", "role1")
		require.NoError(t, err)
		want = append(want, run.ColumnStatisticsTaskRunID)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.ColumnStatisticsTaskRun { return b.ListColumnStatisticsTaskRuns() },
		func(r *glue.ColumnStatisticsTaskRun) string { return r.ColumnStatisticsTaskRunID },
		want)
}

func TestListDataQualityRuleRecommendationRuns_TotalSortAcrossTiedStartedOn(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 6

	want := make([]string, 0, n)

	for range n {
		run, err := b.StartDataQualityRuleRecommendationRun("s3://bucket/data")
		require.NoError(t, err)
		want = append(want, run.RecommendationRunID)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.DQRuleRecommendationRun { return b.ListDataQualityRuleRecommendationRuns() },
		func(r *glue.DQRuleRecommendationRun) string { return r.RecommendationRunID },
		want)
}

func TestListMaterializedViewRefreshTaskRuns_TotalSortAcrossTiedStartedOn(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 6

	want := make([]string, 0, n)

	for range n {
		run, err := b.StartMaterializedViewRefreshTaskRun("db1", "mv1")
		require.NoError(t, err)
		want = append(want, run.TaskRunID)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.MaterializedViewRefreshRun { return b.ListMaterializedViewRefreshTaskRuns() },
		func(r *glue.MaterializedViewRefreshRun) string { return r.TaskRunID },
		want)
}

func TestListDataQualityEvaluationRuns_TotalSortAcrossTiedStartedOn(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 6

	want := make([]string, 0, n)

	for range n {
		run, err := b.StartDataQualityRulesetEvaluationRun(nil)
		require.NoError(t, err)
		want = append(want, run.RunID)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.DataQualityEvaluationRun { return b.ListDataQualityEvaluationRuns() },
		func(r *glue.DataQualityEvaluationRun) string { return r.RunID },
		want)
}

func TestGetMLTransforms_TotalSortAcrossTiedName(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 6

	want := make([]string, 0, n)

	for range n {
		// Real AWS ML transform Name is not unique -- only TransformId is.
		m, err := b.CreateMLTransform("dup-name", "", "role1", nil, glue.MLTransformParameter{}, nil)
		require.NoError(t, err)
		want = append(want, m.TransformID)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.MLTransform { return b.GetMLTransforms() },
		func(m *glue.MLTransform) string { return m.TransformID },
		want)
}

func TestSearchAssets_TotalSortAcrossTiedName(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.PutFormType("Ft1", `{"type":"object"}`)
	require.NoError(t, err)
	_, err = b.PutAssetType("at1", map[string]glue.AssetTypeFormReference{
		"Ft1": {FormTypeIdentifier: "Ft1"},
	})
	require.NoError(t, err)

	const n = 6

	want := make([]string, 0, n)

	for i := range n {
		id := "asset-" + strconv.Itoa(i)
		// Real AWS Asset.Name is not unique -- only the Identifier (ID) is.
		_, putErr := b.PutAsset(id, "DupName", "", "at1", nil)
		require.NoError(t, putErr)
		want = append(want, id)
	}

	walkPaginated(t, tieWalkIterations, 2,
		func() []*glue.Asset { return b.SearchAssets("", nil, "Name", false) },
		func(a *glue.Asset) string { return a.ID },
		want)
}
