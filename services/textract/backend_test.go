package textract_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestInMemoryBackend_AnalyzeDocument(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	blocks := b.AnalyzeDocument(context.Background(), "s3://my-bucket/doc.pdf")

	assert.NotEmpty(t, blocks)
	assert.Equal(t, "PAGE", blocks[0].BlockType)
}

func TestInMemoryBackend_DetectDocumentText(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	blocks := b.DetectDocumentText(context.Background(), "s3://my-bucket/doc.pdf")

	assert.NotEmpty(t, blocks)
}

func TestInMemoryBackend_StartAndGetDocumentAnalysis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		documentURI string
		wantErr     bool
	}{
		{
			name:        "success",
			documentURI: "s3://my-bucket/analysis.pdf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

			job, err := b.StartDocumentAnalysis(context.Background(), tt.documentURI)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, job.JobID)
			assert.Equal(t, "SUCCEEDED", job.JobStatus)
			assert.Equal(t, "DocumentAnalysis", job.JobType)
			assert.NotEmpty(t, job.Blocks)

			// Retrieve the job
			fetched, err := b.GetDocumentAnalysis(context.Background(), job.JobID)
			require.NoError(t, err)
			assert.Equal(t, job.JobID, fetched.JobID)
			assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
		})
	}
}

func TestInMemoryBackend_GetDocumentAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	_, err := b.GetDocumentAnalysis(context.Background(), "nonexistent-job-id")

	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_StartAndGetDocumentTextDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		documentURI string
		wantErr     bool
	}{
		{
			name:        "success",
			documentURI: "s3://my-bucket/page.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

			job, err := b.StartDocumentTextDetection(context.Background(), tt.documentURI)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, job.JobID)
			assert.Equal(t, "SUCCEEDED", job.JobStatus)
			assert.Equal(t, "TextDetection", job.JobType)
			assert.NotEmpty(t, job.Blocks)

			// Retrieve the job
			fetched, err := b.GetDocumentTextDetection(context.Background(), job.JobID)
			require.NoError(t, err)
			assert.Equal(t, job.JobID, fetched.JobID)
			assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
		})
	}
}

func TestInMemoryBackend_GetDocumentTextDetection_NotFound(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	_, err := b.GetDocumentTextDetection(context.Background(), "nonexistent-job-id")

	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListJobs(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	_, err := b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc1.pdf")
	require.NoError(t, err)

	_, err = b.StartDocumentTextDetection(context.Background(), "s3://bucket/doc2.png")
	require.NoError(t, err)

	jobs := b.ListJobs(context.Background())
	assert.Len(t, jobs, 2)
}

func TestInMemoryBackend_GetDocumentAnalysis_WrongType(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	job, err := b.StartDocumentTextDetection(context.Background(), "s3://bucket/doc.png")
	require.NoError(t, err)

	// Try to retrieve it as a DocumentAnalysis job (wrong type)
	_, err = b.GetDocumentAnalysis(context.Background(), job.JobID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetDocumentTextDetection_WrongType(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	job, err := b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc.pdf")
	require.NoError(t, err)

	// Try to retrieve it as a TextDetection job (wrong type)
	_, err = b.GetDocumentTextDetection(context.Background(), job.JobID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_JobHistoryCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		insertAna int
		insertDet int
		wantLen   int
	}{
		{
			name:      "below_cap",
			insertAna: 5,
			insertDet: 5,
			wantLen:   10,
		},
		{
			name:      "above_cap_trims_oldest",
			insertAna: 6,
			insertDet: 0,
			wantLen:   5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var b *textract.InMemoryBackend

			if tt.name == "above_cap_trims_oldest" {
				b = textract.NewInMemoryBackendWithCap(5)
			} else {
				b = textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			}

			for range tt.insertAna {
				_, err := b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc.pdf")
				require.NoError(t, err)
			}

			for range tt.insertDet {
				_, err := b.StartDocumentTextDetection(context.Background(), "s3://bucket/doc.png")
				require.NoError(t, err)
			}

			jobs := b.ListJobs(context.Background())
			assert.Len(t, jobs, tt.wantLen)
		})
	}
}

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

func TestInMemoryBackend_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobCount int
	}{
		{
			name:     "empty_backend",
			jobCount: 0,
		},
		{
			name:     "with_jobs",
			jobCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

			var lastJobID string

			for i := range tt.jobCount {
				var job *textract.DocumentJob
				var err error

				if i%2 == 0 {
					job, err = b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc.pdf")
				} else {
					job, err = b.StartDocumentTextDetection(context.Background(), "s3://bucket/doc.png")
				}

				require.NoError(t, err)
				lastJobID = job.JobID
			}

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			jobs := b2.ListJobs(context.Background())
			assert.Len(t, jobs, tt.jobCount)

			if tt.jobCount > 0 {
				// The last job from original backend should be retrievable after restore.
				retrieved, err := b2.GetDocumentAnalysis(context.Background(), lastJobID)
				if err != nil {
					// May be text detection type; try that.
					retrieved, err = b2.GetDocumentTextDetection(context.Background(), lastJobID)
					require.NoError(t, err)
				}

				assert.Equal(t, lastJobID, retrieved.JobID)

				// Snapshot isolation: adding to b2 after restore should not affect original snap.
				_, _ = b2.StartDocumentAnalysis(context.Background(), "s3://bucket/extra.pdf")
				snap2 := b2.Snapshot(t.Context())
				assert.NotEqual(t, snap, snap2)
			}
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
