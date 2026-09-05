package translate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

func startJob(t *testing.T, b *translate.InMemoryBackend, jobName string) *translate.TranslationJob {
	t.Helper()

	job, err := b.StartTextTranslationJob(
		jobName, "arn:aws:iam::000000000000:role/r", "en",
		[]string{"es"}, nil, nil,
		map[string]any{"S3Uri": "s3://b/i/"},
		map[string]any{"S3Uri": "s3://b/o/"},
		nil, nil,
	)
	require.NoError(t, err)

	return job
}

// TestInMemoryBackend_DescribeTextTranslationJob_AdvancesToCompleted verifies that a
// translation job starts at SUBMITTED (matching
// aws-sdk-go-v2/service/translate/types.JobStatusSubmitted, the documented
// initial value of StartTextTranslationJobOutput.JobStatus) and progresses
// one step per DescribeTextTranslationJob call until it reaches a terminal
// state. Previously jobs were created directly at IN_PROGRESS and never
// advanced, so a client polling DescribeTextTranslationJob -- the documented
// way to track an async batch translation job -- would see IN_PROGRESS
// forever and never observe completion.
func TestInMemoryBackend_DescribeTextTranslationJob_AdvancesToCompleted(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	job := startJob(t, b, "advance-job")
	assert.Equal(t, "SUBMITTED", job.JobStatus)

	got, err := b.DescribeTextTranslationJob(job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "IN_PROGRESS", got.JobStatus)

	got, err = b.DescribeTextTranslationJob(job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", got.JobStatus)
	assert.False(t, got.EndAt.IsZero(), "EndAt must be set once the job reaches a terminal state")

	// Further polling must not regress past the terminal state.
	got, err = b.DescribeTextTranslationJob(job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", got.JobStatus)
}

// TestInMemoryBackend_DescribeTextTranslationJob_AdvancesToFailed verifies that a job
// whose name carries the "[fail]" marker (matching the
// services/comprehend convention for deterministically testing the failure
// path) reaches FAILED instead of COMPLETED, with a Message explaining why.
func TestInMemoryBackend_DescribeTextTranslationJob_AdvancesToFailed(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	job := startJob(t, b, "[fail] job")

	_, err := b.DescribeTextTranslationJob(job.JobID) // SUBMITTED -> IN_PROGRESS
	require.NoError(t, err)

	got, err := b.DescribeTextTranslationJob(job.JobID) // IN_PROGRESS -> FAILED
	require.NoError(t, err)
	assert.Equal(t, "FAILED", got.JobStatus)
	assert.NotEmpty(t, got.Message)
}

// TestInMemoryBackend_StopTextTranslationJob_AdvancesToStopped verifies that a
// STOP_REQUESTED job reaches STOPPED on a subsequent
// DescribeTextTranslationJob call, rather than sitting at STOP_REQUESTED
// forever.
func TestInMemoryBackend_StopTextTranslationJob_AdvancesToStopped(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	job := startJob(t, b, "stop-then-advance")

	stopped, err := b.StopTextTranslationJob(job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "STOP_REQUESTED", stopped.JobStatus)

	got, err := b.DescribeTextTranslationJob(job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", got.JobStatus)
}

// TestInMemoryBackend_ListTextTranslationJobs_DoesNotAdvance verifies that listing
// jobs is a pure read: it must not itself advance job status the way
// DescribeTextTranslationJob does, since AWS's List operation does not
// affect job state.
func TestInMemoryBackend_ListTextTranslationJobs_DoesNotAdvance(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	job := startJob(t, b, "list-no-advance")

	for range 3 {
		list, _ := b.ListTextTranslationJobs(translate.TextTranslationJobFilter{}, 0, "")
		require.Len(t, list, 1)
		assert.Equal(t, "SUBMITTED", list[0].JobStatus)
	}

	_ = job
}
