package textract_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestInMemoryBackend_AnalyzeDocument(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	blocks := b.AnalyzeDocument("s3://my-bucket/doc.pdf")

	assert.NotEmpty(t, blocks)
	assert.Equal(t, "PAGE", blocks[0].BlockType)
}

func TestInMemoryBackend_DetectDocumentText(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	blocks := b.DetectDocumentText("s3://my-bucket/doc.pdf")

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

			job, err := b.StartDocumentAnalysis(tt.documentURI)

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
			fetched, err := b.GetDocumentAnalysis(job.JobID)
			require.NoError(t, err)
			assert.Equal(t, job.JobID, fetched.JobID)
			assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
		})
	}
}

func TestInMemoryBackend_GetDocumentAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	_, err := b.GetDocumentAnalysis("nonexistent-job-id")

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

			job, err := b.StartDocumentTextDetection(tt.documentURI)

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
			fetched, err := b.GetDocumentTextDetection(job.JobID)
			require.NoError(t, err)
			assert.Equal(t, job.JobID, fetched.JobID)
			assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
		})
	}
}

func TestInMemoryBackend_GetDocumentTextDetection_NotFound(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	_, err := b.GetDocumentTextDetection("nonexistent-job-id")

	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListJobs(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	_, err := b.StartDocumentAnalysis("s3://bucket/doc1.pdf")
	require.NoError(t, err)

	_, err = b.StartDocumentTextDetection("s3://bucket/doc2.png")
	require.NoError(t, err)

	jobs := b.ListJobs()
	assert.Len(t, jobs, 2)
}

func TestInMemoryBackend_GetDocumentAnalysis_WrongType(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	job, err := b.StartDocumentTextDetection("s3://bucket/doc.png")
	require.NoError(t, err)

	// Try to retrieve it as a DocumentAnalysis job (wrong type)
	_, err = b.GetDocumentAnalysis(job.JobID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetDocumentTextDetection_WrongType(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	job, err := b.StartDocumentAnalysis("s3://bucket/doc.pdf")
	require.NoError(t, err)

	// Try to retrieve it as a TextDetection job (wrong type)
	_, err = b.GetDocumentTextDetection(job.JobID)
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
				_, err := b.StartDocumentAnalysis("s3://bucket/doc.pdf")
				require.NoError(t, err)
			}

			for range tt.insertDet {
				_, err := b.StartDocumentTextDetection("s3://bucket/doc.png")
				require.NoError(t, err)
			}

			jobs := b.ListJobs()
			assert.Len(t, jobs, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_ExpenseJobHistoryCap(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendWithCap(3)

	for range 6 {
		_, err := b.StartExpenseAnalysis("s3://bucket/receipt.pdf")
		require.NoError(t, err)
	}

	assert.Equal(t, 3, textract.ExpenseJobCount(b),
		"expense jobs map should be capped at 3 once over the cap")
}

func TestInMemoryBackend_LendingJobHistoryCap(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendWithCap(2)

	for range 5 {
		_, err := b.StartLendingAnalysis("s3://bucket/loan.pdf")
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
					job, err = b.StartDocumentAnalysis("s3://bucket/doc.pdf")
				} else {
					job, err = b.StartDocumentTextDetection("s3://bucket/doc.png")
				}

				require.NoError(t, err)
				lastJobID = job.JobID
			}

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			require.NoError(t, b2.Restore(snap))

			jobs := b2.ListJobs()
			assert.Len(t, jobs, tt.jobCount)

			if tt.jobCount > 0 {
				// The last job from original backend should be retrievable after restore.
				retrieved, err := b2.GetDocumentAnalysis(lastJobID)
				if err != nil {
					// May be text detection type; try that.
					retrieved, err = b2.GetDocumentTextDetection(lastJobID)
					require.NoError(t, err)
				}

				assert.Equal(t, lastJobID, retrieved.JobID)

				// Snapshot isolation: adding to b2 after restore should not affect original snap.
				_, _ = b2.StartDocumentAnalysis("s3://bucket/extra.pdf")
				snap2 := b2.Snapshot()
				assert.NotEqual(t, snap, snap2)
			}
		})
	}
}
