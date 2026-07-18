package textract_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestInMemoryBackend_DetectDocumentText(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	blocks := b.DetectDocumentText(context.Background(), "s3://my-bucket/doc.pdf")

	assert.NotEmpty(t, blocks)
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
