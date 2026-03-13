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

	b := textract.NewInMemoryBackend()
	blocks := b.AnalyzeDocument("s3://my-bucket/doc.pdf")

	assert.NotEmpty(t, blocks)
	assert.Equal(t, "PAGE", blocks[0].BlockType)
}

func TestInMemoryBackend_DetectDocumentText(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackend()
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

			b := textract.NewInMemoryBackend()

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

	b := textract.NewInMemoryBackend()
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

			b := textract.NewInMemoryBackend()

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

	b := textract.NewInMemoryBackend()
	_, err := b.GetDocumentTextDetection("nonexistent-job-id")

	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListJobs(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackend()

	_, err := b.StartDocumentAnalysis("s3://bucket/doc1.pdf")
	require.NoError(t, err)

	_, err = b.StartDocumentTextDetection("s3://bucket/doc2.png")
	require.NoError(t, err)

	jobs := b.ListJobs()
	assert.Len(t, jobs, 2)
}

func TestInMemoryBackend_GetDocumentAnalysis_WrongType(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackend()

	job, err := b.StartDocumentTextDetection("s3://bucket/doc.png")
	require.NoError(t, err)

	// Try to retrieve it as a DocumentAnalysis job (wrong type)
	_, err = b.GetDocumentAnalysis(job.JobID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetDocumentTextDetection_WrongType(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackend()

	job, err := b.StartDocumentAnalysis("s3://bucket/doc.pdf")
	require.NoError(t, err)

	// Try to retrieve it as a TextDetection job (wrong type)
	_, err = b.GetDocumentTextDetection(job.JobID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
