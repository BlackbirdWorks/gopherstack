package databrew //nolint:testpackage // needs access to the unexported region context key (see isolation_test.go).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataBrew_RestoreInvalidData verifies that malformed JSON is reported as
// an error rather than silently discarded or partially applied.
func TestDataBrew_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestDataBrew_RestoreVersionMismatch verifies that a snapshot whose version
// doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestDataBrew_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDataset(
		databrewCtxRegion("us-east-1"), "seed-ds", "CSV", DatasetInput{}, DatasetFormatOptions{}, nil,
		nil,
	)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	list, _ := b.ListDatasets(databrewCtxRegion("us-east-1"), 0, "")
	assert.Empty(t, list)
}

// TestDataBrew_RestoreOldSnapshotDecodesAsZero verifies that a snapshot with
// no version field at all decodes with Version == 0, which mismatches
// databrewSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied. This also covers the
// pre-Phase-3.3 on-disk shape (DataBrew had no persistence at all before this
// conversion, so there is no legacy shape to actually collide with -- any
// input lacking "version" hits this same path).
func TestDataBrew_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDataset(
		databrewCtxRegion("us-east-1"), "seed-ds", "CSV", DatasetInput{}, DatasetFormatOptions{}, nil,
		nil,
	)
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"datasets":{}}`))
	require.NoError(t, err)

	list, _ := b.ListDatasets(databrewCtxRegion("us-east-1"), 0, "")
	assert.Empty(t, list)
}

// persistenceSeed is every resource created by seedPersistenceState, kept
// together so the post-restore assertions in
// TestDataBrew_SnapshotRestore_FullState can refer back to the original
// names/ARNs.
type persistenceSeed struct {
	dataset  *Dataset
	recipe   *Recipe
	project  *Project
	job      *Job
	ruleset  *Ruleset
	schedule *Schedule
	run1ID   string
	run2ID   string
}

// seedPersistenceState populates one instance of every store.Table-backed
// resource family this conversion touched (datasets, recipes, projects,
// jobs, rulesets, schedules) in region "us-east-1", plus the raw jobRuns map
// (two runs on the seeded job, to prove chronological order survives the
// round trip), and a second dataset in region "us-west-2" to prove
// preRegisterSnapshotTables restores every touched region, not just the
// first one found.
func seedPersistenceState(t *testing.T, b *InMemoryBackend) persistenceSeed {
	t.Helper()

	ctxEast := databrewCtxRegion("us-east-1")
	ctxWest := databrewCtxRegion("us-west-2")

	dataset, err := b.CreateDataset(
		ctxEast, "ds-1", "CSV", DatasetInput{S3InputDefinition: &S3Location{Bucket: "b", Key: "k"}},
		DatasetFormatOptions{}, map[string]string{"env": "test"},
		nil,
	)
	require.NoError(t, err)

	recipe, err := b.CreateRecipe(
		ctxEast, "recipe-1", "a recipe", []RecipeStep{{Action: map[string]any{"Operation": "UPPER_CASE"}}},
		map[string]string{"team": "core"},
	)
	require.NoError(t, err)

	project, err := b.CreateProject(
		ctxEast, "project-1", dataset.Name, recipe.Name, "arn:aws:iam::123456789012:role/r",
		Sample{Type: "", Size: 100}, map[string]string{"purpose": "analysis"},
	)
	require.NoError(t, err)

	job, err := b.CreateJob(
		ctxEast, "job-1", "RECIPE", dataset.Name, project.Name, recipe.Name,
		"arn:aws:iam::123456789012:role/r", []Output{{Format: "CSV", Location: S3Location{Bucket: "out"}}},
		map[string]string{"kind": "recipe"},
		JobExtras{},
	)
	require.NoError(t, err)

	ruleset, err := b.CreateRuleset(
		ctxEast, "ruleset-1", "a ruleset", dataset.Arn,
		[]Rule{{Name: "rule-1", CheckExpression: ":col1 > 0"}}, map[string]string{"kind": "ruleset"},
	)
	require.NoError(t, err)

	schedule, err := b.CreateSchedule(
		ctxEast, "schedule-1", []string{job.Name}, "cron(0 12 * * ? *)", map[string]string{"kind": "schedule"},
	)
	require.NoError(t, err)

	run1, err := b.StartJobRun(ctxEast, job.Name)
	require.NoError(t, err)
	run2, err := b.StartJobRun(ctxEast, job.Name)
	require.NoError(t, err)

	// A second region, to prove preRegisterSnapshotTables restores every
	// touched region -- not just the one the other resources happen to share.
	_, err = b.CreateDataset(
		ctxWest, "ds-west", "JSON", DatasetInput{}, DatasetFormatOptions{}, nil,
		nil,
	)
	require.NoError(t, err)

	return persistenceSeed{
		dataset: dataset, recipe: recipe, project: project, job: job,
		ruleset: ruleset, schedule: schedule, run1ID: run1.RunID, run2ID: run2.RunID,
	}
}

// TestDataBrew_SnapshotRestore_FullState exercises a Snapshot -> Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (datasets, recipes, projects, jobs, rulesets,
// schedules), the raw jobRuns map (order-sensitive: run1 must still precede
// run2 chronologically after restore), and multi-region persistence.
func TestDataBrew_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("123456789012", "us-east-1")
	seed := seedPersistenceState(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "eu-west-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	ctxEast := databrewCtxRegion("us-east-1")
	ctxWest := databrewCtxRegion("us-west-2")

	assertResourceTablesRestored(ctxEast, t, fresh, seed)
	assertJobRunsRestored(ctxEast, t, fresh, seed)

	// Second region survived the round trip too.
	westList, _ := fresh.ListDatasets(ctxWest, 0, "")
	require.Len(t, westList, 1)
	assert.Equal(t, "ds-west", westList[0].Name)

	// accountID/region scalars survived.
	assert.Equal(t, "123456789012", fresh.AccountID())
	assert.Equal(t, "us-east-1", fresh.Region())
}

// assertResourceTablesRestored checks the six store.Table-backed resource
// families: datasets, recipes, projects, jobs, rulesets, schedules.
func assertResourceTablesRestored(ctx context.Context, t *testing.T, fresh *InMemoryBackend, seed persistenceSeed) {
	t.Helper()

	gotDS, err := fresh.DescribeDataset(ctx, seed.dataset.Name)
	require.NoError(t, err)
	assert.Equal(t, seed.dataset.Format, gotDS.Format)
	assert.Equal(t, "test", gotDS.Tags["env"])

	gotRecipe, err := fresh.DescribeRecipe(ctx, seed.recipe.Name, "")
	require.NoError(t, err)
	assert.Equal(t, seed.recipe.Description, gotRecipe.Description)
	require.Len(t, gotRecipe.Steps, 1)

	gotProject, err := fresh.DescribeProject(ctx, seed.project.Name)
	require.NoError(t, err)
	assert.Equal(t, seed.project.DatasetName, gotProject.DatasetName)

	gotJob, err := fresh.DescribeJob(ctx, seed.job.Name)
	require.NoError(t, err)
	assert.Equal(t, seed.job.Type, gotJob.Type)
	require.Len(t, gotJob.Outputs, 1)

	gotRuleset, err := fresh.DescribeRuleset(ctx, seed.ruleset.Name)
	require.NoError(t, err)
	require.Len(t, gotRuleset.Rules, 1)
	assert.Equal(t, "rule-1", gotRuleset.Rules[0].Name)

	gotSchedule, err := fresh.DescribeSchedule(ctx, seed.schedule.Name)
	require.NoError(t, err)
	require.Len(t, gotSchedule.JobNames, 1)
	assert.Equal(t, seed.job.Name, gotSchedule.JobNames[0])
}

// TestDataBrew_SnapshotRestore_TypedJobFields proves the typed shapes that
// replaced map[string]any (JobSample, DataCatalogOutputs, DatabaseOutputs,
// DatasetFormatOptions.Csv/Excel/Json) still round-trip through Snapshot ->
// Restore: a raw JSON blob round-trips through map[string]any trivially, but
// a typed struct field can silently lose data if its json tags don't match
// what was written, or if a pointer field decodes to nil.
func TestDataBrew_SnapshotRestore_TypedJobFields(t *testing.T) {
	t.Parallel()

	ctx := databrewCtxRegion("us-east-1")
	original := NewInMemoryBackend("123456789012", "us-east-1")

	headerRow := true
	_, err := original.CreateDataset(
		ctx, "typed-ds", "CSV", DatasetInput{S3InputDefinition: &S3Location{Bucket: "b"}},
		DatasetFormatOptions{
			Csv:   &CsvOptions{Delimiter: ";", HeaderRow: &headerRow},
			Excel: &ExcelOptions{SheetNames: []string{"Sheet1"}},
			JSON:  &JSONOptions{MultiLine: true},
		},
		nil, nil,
	)
	require.NoError(t, err)

	job, err := original.CreateJob(
		ctx, "typed-job", "RECIPE", "typed-ds", "", "", "arn:aws:iam::123456789012:role/r",
		nil, nil,
		JobExtras{
			JobSample: &JobSample{Mode: "CUSTOM_ROWS", Size: 42},
			DataCatalogOutputs: []DataCatalogOutput{{
				DatabaseName: "db1", TableName: "t1", Overwrite: true,
			}},
			DatabaseOutputs: []DatabaseOutput{{
				GlueConnectionName: "conn1",
				DatabaseOptions:    &DatabaseTableOutputOptions{TableName: "t2"},
			}},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, job.JobSample)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "eu-west-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	gotDS, err := fresh.DescribeDataset(ctx, "typed-ds")
	require.NoError(t, err)
	require.NotNil(t, gotDS.FormatOptions.Csv)
	assert.Equal(t, ";", gotDS.FormatOptions.Csv.Delimiter)
	require.NotNil(t, gotDS.FormatOptions.Csv.HeaderRow)
	assert.True(t, *gotDS.FormatOptions.Csv.HeaderRow)
	require.NotNil(t, gotDS.FormatOptions.Excel)
	assert.Equal(t, []string{"Sheet1"}, gotDS.FormatOptions.Excel.SheetNames)
	require.NotNil(t, gotDS.FormatOptions.JSON)
	assert.True(t, gotDS.FormatOptions.JSON.MultiLine)

	gotJob, err := fresh.DescribeJob(ctx, "typed-job")
	require.NoError(t, err)
	require.NotNil(t, gotJob.JobSample)
	assert.Equal(t, "CUSTOM_ROWS", gotJob.JobSample.Mode)
	assert.Equal(t, int64(42), gotJob.JobSample.Size)
	require.Len(t, gotJob.DataCatalogOutputs, 1)
	assert.Equal(t, "db1", gotJob.DataCatalogOutputs[0].DatabaseName)
	assert.True(t, gotJob.DataCatalogOutputs[0].Overwrite)
	require.Len(t, gotJob.DatabaseOutputs, 1)
	assert.Equal(t, "conn1", gotJob.DatabaseOutputs[0].GlueConnectionName)
	require.NotNil(t, gotJob.DatabaseOutputs[0].DatabaseOptions)
	assert.Equal(t, "t2", gotJob.DatabaseOutputs[0].DatabaseOptions.TableName)
}

// assertJobRunsRestored checks the raw (non-store.Table) jobRuns map: the two
// seeded runs must both survive the round trip and ListJobRuns must still
// return them newest-first (run2 before run1), proving chronological
// insertion order was preserved rather than reshuffled by an Index.
func assertJobRunsRestored(ctx context.Context, t *testing.T, fresh *InMemoryBackend, seed persistenceSeed) {
	t.Helper()

	runs, _, err := fresh.ListJobRuns(ctx, seed.job.Name, 0, "")
	require.NoError(t, err)
	require.Len(t, runs, 2)
	assert.Equal(t, seed.run2ID, runs[0].RunID, "newest run must be listed first")
	assert.Equal(t, seed.run1ID, runs[1].RunID, "oldest run must be listed second")
}
