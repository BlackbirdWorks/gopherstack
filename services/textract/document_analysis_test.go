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

// TestInMemoryBackend_StartDocumentAnalysisWithOptions_TrimmedBeforeReadback covers gopherstack-0ho6.
func TestInMemoryBackend_StartDocumentAnalysisWithOptions_TrimmedBeforeReadback(t *testing.T) {
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

			job, err := b.StartDocumentAnalysisWithOptions(
				context.Background(), "s3://bucket/doc.pdf", nil, nil, nil, "", "",
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

func TestInMemoryBackend_GetDocumentAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	_, err := b.GetDocumentAnalysis(context.Background(), "nonexistent-job-id")

	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
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
