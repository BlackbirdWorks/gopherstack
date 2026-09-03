package translate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/translate"
)

// seedState is every resource created by seedFullState, kept together so the
// post-restore assertions can refer back to the original names/IDs/ARNs.
type seedState struct {
	terminology *translate.Terminology
	parallel    *translate.ParallelData
	job         *translate.TranslationJob
}

// seedFullState populates one instance of every store.Table-backed resource
// family the Phase 3.3 conversion touched (terminologies, parallelData,
// jobs), plus the tags raw map (via each call's tags argument), so
// Test_SnapshotRestore_FullState can exercise a Snapshot->Restore round trip
// across all of them.
func seedFullState(t *testing.T, b *translate.InMemoryBackend) seedState {
	t.Helper()

	term, err := b.ImportTerminology(
		"term-1", "a terminology",
		&translate.TerminologyData{Format: "CSV", File: []byte("en,es\nhello,hola\n")},
		nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	pd, err := b.CreateParallelData(
		"pd-1", "a parallel data set",
		&translate.ParallelDataConfig{S3URI: "s3://bucket/pd", Format: "CSV"},
		nil,
		map[string]string{"team": "core"},
	)
	require.NoError(t, err)

	job, err := b.StartTextTranslationJob(
		"job-1", "arn:aws:iam::111122223333:role/translate", "en",
		[]string{"es", "fr"}, []string{"term-1"}, nil,
		map[string]any{"S3Uri": "s3://bucket/in"},
		map[string]any{"S3Uri": "s3://bucket/out"},
		map[string]any{"Formality": "FORMAL"},
		map[string]string{"kind": "job"},
	)
	require.NoError(t, err)

	// Advance the job past its default SUBMITTED status so the round trip
	// is proven to carry a non-default JobStatus (and a non-zero EndAt), not
	// just the freshly-created defaults.
	stopped, err := b.StopTextTranslationJob(job.JobID)
	require.NoError(t, err)

	return seedState{terminology: term, parallel: pd, job: stopped}
}

// Test_SnapshotRestore_FullState exercises a Snapshot->Restore round trip
// across every store.Table-backed resource family the Phase 3.3 conversion
// touched (terminologies, parallelData, jobs) and the one plain map left
// unconverted (tags, keyed by resource ARN -- its values are
// map[string]string, not *T, so there is nothing for store.Table to key on;
// see store_setup.go and persistence.go).
func Test_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := translate.NewInMemoryBackend("111122223333", "us-west-2")
	seed := seedFullState(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := translate.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertTerminologyRestored(t, fresh, seed)
	assertParallelDataRestored(t, fresh, seed)
	assertJobRestored(t, fresh, seed)
}

func assertTerminologyRestored(t *testing.T, fresh *translate.InMemoryBackend, seed seedState) {
	t.Helper()

	gotTerm, err := fresh.GetTerminology(seed.terminology.Name)
	require.NoError(t, err)
	assert.Equal(t, seed.terminology.Description, gotTerm.Description)
	assert.Contains(t, gotTerm.ARN, "111122223333")
	assert.Contains(t, gotTerm.ARN, "us-west-2")

	list, _ := fresh.ListTerminologies(0, "")
	require.Len(t, list, 1)

	tags, err := fresh.ListTagsForResource(seed.terminology.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func assertParallelDataRestored(t *testing.T, fresh *translate.InMemoryBackend, seed seedState) {
	t.Helper()

	gotPD, err := fresh.GetParallelData(seed.parallel.Name)
	require.NoError(t, err)
	assert.Equal(t, seed.parallel.Description, gotPD.Description)
	require.NotNil(t, gotPD.ParallelDataConfig)
	assert.Equal(t, "s3://bucket/pd", gotPD.ParallelDataConfig.S3URI)

	list, _ := fresh.ListParallelData(0, "")
	require.Len(t, list, 1)

	tags, err := fresh.ListTagsForResource(seed.parallel.ARN)
	require.NoError(t, err)
	assert.Equal(t, "core", tags["team"])
}

// assertJobRestored checks that the job's JobStatus/EndAt -- mutated in
// place after creation by StopTextTranslationJob, not present in the
// zero-value job -- round-trips through the snapshot, proving the restored
// table entry is the post-mutation state and not the original. It reads the
// job back via ListTextTranslationJobs rather than
// DescribeTextTranslationJob because Describe advances the job one step
// through its lifecycle on every call (see backend.go's advanceJob) and
// this assertion must observe the state exactly as restored, not
// post-advance.
func assertJobRestored(t *testing.T, fresh *translate.InMemoryBackend, seed seedState) {
	t.Helper()

	list, _ := fresh.ListTextTranslationJobs(translate.TextTranslationJobFilter{}, 0, "")
	require.Len(t, list, 1)

	gotJob := list[0]
	assert.Equal(t, "STOP_REQUESTED", gotJob.JobStatus)
	assert.False(t, gotJob.EndAt.IsZero())
	assert.Equal(t, seed.job.JobName, gotJob.JobName)
	assert.Equal(t, []string{"es", "fr"}, gotJob.TargetLanguages)
}

// Test_RestoreVersionMismatch verifies that a snapshot whose version doesn't
// match the current backend -- including a version-less snapshot, which
// decodes with Version == 0 -- is discarded cleanly rather than partially
// decoded: every table and the raw tags map reset to empty, and Restore
// itself returns no error.
func Test_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{name: "explicit_mismatch", data: `{"version":999,"tables":{}}`},
		{name: "old_or_absent_version_decodes_as_zero", data: `{"tables":{}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := translate.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			seed := seedFullState(t, b)

			require.NoError(t, b.Restore(t.Context(), []byte(test.data)))

			list, _ := b.ListTerminologies(0, "")
			assert.Empty(t, list)

			pdList, _ := b.ListParallelData(0, "")
			assert.Empty(t, pdList)

			jobList, _ := b.ListTextTranslationJobs(translate.TextTranslationJobFilter{}, 0, "")
			assert.Empty(t, jobList)

			_, err := b.GetTerminology(seed.terminology.Name)
			require.ErrorIs(t, err, translate.ErrNotFound)

			_, err = b.ListTagsForResource(seed.terminology.ARN)
			require.ErrorIs(t, err, translate.ErrNotFound)
		})
	}
}

// Test_RestoreInvalidData verifies that malformed JSON is reported as an
// error rather than silently discarded or partially applied.
func Test_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := translate.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_HandlerSnapshotRestore verifies the dead-wiring fix: Handler now
// implements persistence.Persistable by delegating to the backend, which it
// did not before Phase 3.3 (Translate had no persistence at all -- see
// persistence.go's file doc comment).
func Test_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	var _ persistence.Persistable = (*translate.Handler)(nil)

	original := translate.NewHandler(translate.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	_, err := original.Backend.ImportTerminology(
		"seed", "", &translate.TerminologyData{Format: "CSV", File: []byte("en,es\nhi,hola\n")}, nil, nil,
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := translate.NewHandler(translate.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	require.NoError(t, fresh.Restore(t.Context(), snap))

	list, _ := fresh.Backend.ListTerminologies(0, "")
	assert.Len(t, list, 1)
}
