package textract_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestInMemoryBackend_ExpenseJobHistoryCap(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendWithCap(3)

	for range 6 {
		_, err := b.StartExpenseAnalysis(context.Background(), "s3://bucket/receipt.pdf")
		require.NoError(t, err)
	}

	assert.Equal(t, 3, textract.ExpenseJobCount(b),
		"expense jobs map should be capped at 3 once over the cap")
}

// TestInMemoryBackend_StartExpenseAnalysisWithOptions_TrimmedBeforeReadback covers gopherstack-0ho6.
func TestInMemoryBackend_StartExpenseAnalysisWithOptions_TrimmedBeforeReadback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		maxJobs int
		wantErr bool
	}{
		{name: "zero_cap", maxJobs: 0, wantErr: true},
		{name: "normal_cap", maxJobs: 5, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackendWithCap(tt.maxJobs)

			job, err := b.StartExpenseAnalysisWithOptions(
				context.Background(), "s3://bucket/receipt.pdf", nil, nil, "", "",
			)

			if tt.wantErr {
				// gopherstack-uox6: a job evicted mid-write is a server
				// invariant violation, not a client-supplied bad job ID --
				// must not be ErrNotFound.
				require.Error(t, err)
				require.NotErrorIs(t, err, awserr.ErrNotFound)
				assert.Nil(t, job)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, job.JobID)
		})
	}
}

func TestInMemoryBackend_StartExpenseAnalysisWithOptions_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	outputConfig := &textract.OutputConfig{S3Bucket: "out-bucket", S3Prefix: "out/"}
	notificationChannel := &textract.NotificationChannel{
		SNSTopicArn: "arn:aws:sns:us-east-1:123456789012:topic",
		RoleArn:     "arn:aws:iam::123456789012:role/role",
	}

	job1, err := b.StartExpenseAnalysisWithOptions(
		context.Background(), "s3://bucket/receipt.pdf",
		outputConfig, notificationChannel, "tag-1", "expense-token-1",
	)
	require.NoError(t, err)
	require.NotEmpty(t, job1.JobID)

	// Same token must return the same job, not create a second one.
	job2, err := b.StartExpenseAnalysisWithOptions(
		context.Background(), "s3://bucket/receipt.pdf",
		outputConfig, notificationChannel, "tag-1", "expense-token-1",
	)
	require.NoError(t, err)
	assert.Equal(t, job1.JobID, job2.JobID, "same ClientRequestToken must return same JobId")
	assert.Equal(t, 1, textract.ExpenseJobCount(b), "idempotent retry must not create a second job")

	// A different token creates a new job.
	job3, err := b.StartExpenseAnalysisWithOptions(
		context.Background(), "s3://bucket/receipt.pdf",
		nil, nil, "", "expense-token-2",
	)
	require.NoError(t, err)
	assert.NotEqual(t, job1.JobID, job3.JobID)
	assert.Equal(t, 2, textract.ExpenseJobCount(b))
}

// TestInMemoryBackend_AddExpenseJobInternal tests the AddExpenseJobInternal
// seed helper, including that ExpenseDocuments Blocks are deep-copied so
// mutating a fetched job does not corrupt the stored one.
func TestInMemoryBackend_AddExpenseJobInternal(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	expJob := &textract.ExpenseJob{
		JobID:        "expense-seed-job",
		JobStatus:    "SUCCEEDED",
		CreationTime: time.Now(),
		ExpenseDocuments: []textract.ExpenseDocument{
			{ExpenseIndex: 1, Blocks: []textract.Block{{BlockType: "PAGE", ID: "blk-1"}}},
		},
	}
	textract.AddExpenseJobInternal(b, expJob)
	assert.Equal(t, 1, textract.ExpenseJobCount(b))

	fetchedExp, err := b.GetExpenseAnalysis(context.Background(), "expense-seed-job")
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", fetchedExp.JobStatus)
	assert.Len(t, fetchedExp.ExpenseDocuments, 1)
	// Deep copy check: blocks inside expense documents must be independent.
	fetchedExp.ExpenseDocuments[0].Blocks[0].BlockType = "MUTATED"

	fetchedExp2, err := b.GetExpenseAnalysis(context.Background(), "expense-seed-job")
	require.NoError(t, err)
	assert.Equal(t, "PAGE", fetchedExp2.ExpenseDocuments[0].Blocks[0].BlockType)
}

// TestInMemoryBackend_PersistenceWithExpenseAndLendingJobs tests Snapshot/Restore
// with expense and lending jobs present in the same snapshot.
func TestInMemoryBackend_PersistenceWithExpenseAndLendingJobs(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	expJob, err := b.StartExpenseAnalysis(context.Background(), "s3://bucket/invoice.pdf")
	require.NoError(t, err)

	lendJob, err := b.StartLendingAnalysis(context.Background(), "s3://bucket/loan.pdf")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := textract.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, textract.ExpenseJobCount(b2))
	assert.Equal(t, 1, textract.LendingJobCount(b2))

	fetched, err := b2.GetExpenseAnalysis(context.Background(), expJob.JobID)
	require.NoError(t, err)
	assert.Equal(t, expJob.JobID, fetched.JobID)
	assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
	assert.NotEmpty(t, fetched.ExpenseDocuments)

	fetchedL, err := b2.GetLendingAnalysis(context.Background(), lendJob.JobID)
	require.NoError(t, err)
	assert.Equal(t, lendJob.JobID, fetchedL.JobID)
}
