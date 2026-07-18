package textract_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestInMemoryBackend_LendingJobHistoryCap(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendWithCap(2)

	for range 5 {
		_, err := b.StartLendingAnalysis(context.Background(), "s3://bucket/loan.pdf")
		require.NoError(t, err)
	}

	assert.Equal(t, 2, textract.LendingJobCount(b),
		"lending jobs map should be capped at 2 once over the cap")
}

func TestInMemoryBackend_StartLendingAnalysisWithOptions_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	outputConfig := &textract.OutputConfig{S3Bucket: "out-bucket", S3Prefix: "out/"}
	notificationChannel := &textract.NotificationChannel{
		SNSTopicArn: "arn:aws:sns:us-east-1:123456789012:topic",
		RoleArn:     "arn:aws:iam::123456789012:role/role",
	}

	job1, err := b.StartLendingAnalysisWithOptions(
		context.Background(), "s3://bucket/loan.pdf",
		outputConfig, notificationChannel, "tag-1", "lending-token-1",
	)
	require.NoError(t, err)
	require.NotEmpty(t, job1.JobID)

	// Same token must return the same job, not create a second one.
	job2, err := b.StartLendingAnalysisWithOptions(
		context.Background(), "s3://bucket/loan.pdf",
		outputConfig, notificationChannel, "tag-1", "lending-token-1",
	)
	require.NoError(t, err)
	assert.Equal(t, job1.JobID, job2.JobID, "same ClientRequestToken must return same JobId")
	assert.Equal(t, 1, textract.LendingJobCount(b), "idempotent retry must not create a second job")

	// A different token creates a new job.
	job3, err := b.StartLendingAnalysisWithOptions(
		context.Background(), "s3://bucket/loan.pdf",
		nil, nil, "", "lending-token-2",
	)
	require.NoError(t, err)
	assert.NotEqual(t, job1.JobID, job3.JobID)
	assert.Equal(t, 2, textract.LendingJobCount(b))
}

func TestInMemoryBackend_GetLendingAnalysisSummary_ReturnsWarnings(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	job, err := b.StartLendingAnalysis(context.Background(), "s3://bucket/loan.pdf")
	require.NoError(t, err)

	textract.AddLendingJobInternal(b, &textract.LendingJob{
		JobID:     job.JobID,
		JobStatus: "SUCCEEDED",
		Warnings: []textract.WarningBlock{
			{ErrorCode: "InvalidPageException", Pages: []int{2}},
		},
	})

	summary, err := b.GetLendingAnalysisSummary(context.Background(), job.JobID)
	require.NoError(t, err)
	require.Len(t, summary.Warnings, 1)
	assert.Equal(t, "InvalidPageException", summary.Warnings[0].ErrorCode)
}

// TestInMemoryBackend_AddLendingJobInternal tests the AddLendingJobInternal seed helper.
func TestInMemoryBackend_AddLendingJobInternal(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	lendJob := &textract.LendingJob{
		JobID:        "lending-seed-job",
		JobStatus:    "SUCCEEDED",
		CreationTime: time.Now(),
		Results:      []textract.LendingResult{{Page: 1}},
	}
	textract.AddLendingJobInternal(b, lendJob)
	assert.Equal(t, 1, textract.LendingJobCount(b))

	fetched, err := b.GetLendingAnalysis(context.Background(), "lending-seed-job")
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
}
