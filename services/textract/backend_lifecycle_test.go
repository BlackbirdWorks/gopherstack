package textract_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// TestInMemoryBackend_Shutdown verifies that the backend's delayed async-job
// completion goroutines are cancelled on Shutdown and do not leak, while
// zero-delay (sync) completions still transition promptly.
func TestInMemoryBackend_Shutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// run starts an async job on b and returns a getter for its status.
		run func(t *testing.T, b *textract.InMemoryBackend) func() string
		// wantSucceededOnZeroDelay is the status expected immediately when the
		// backend uses a zero async delay (sync transition path).
		wantSucceededOnZeroDelay string
	}{
		{
			name: "document analysis",
			run: func(t *testing.T, b *textract.InMemoryBackend) func() string {
				t.Helper()
				job, err := b.StartDocumentAnalysis("s3://bucket/doc.pdf")
				require.NoError(t, err)

				return func() string {
					got, gErr := b.GetDocumentAnalysis(job.JobID)
					require.NoError(t, gErr)

					return got.JobStatus
				}
			},
			wantSucceededOnZeroDelay: "SUCCEEDED",
		},
		{
			name: "document text detection",
			run: func(t *testing.T, b *textract.InMemoryBackend) func() string {
				t.Helper()
				job, err := b.StartDocumentTextDetection("s3://bucket/doc.pdf")
				require.NoError(t, err)

				return func() string {
					got, gErr := b.GetDocumentTextDetection(job.JobID)
					require.NoError(t, gErr)

					return got.JobStatus
				}
			},
			wantSucceededOnZeroDelay: "SUCCEEDED",
		},
		{
			name: "expense analysis",
			run: func(t *testing.T, b *textract.InMemoryBackend) func() string {
				t.Helper()
				job, err := b.StartExpenseAnalysis("s3://bucket/doc.pdf")
				require.NoError(t, err)

				return func() string {
					got, gErr := b.GetExpenseAnalysis(job.JobID)
					require.NoError(t, gErr)

					return got.JobStatus
				}
			},
			wantSucceededOnZeroDelay: "SUCCEEDED",
		},
		{
			name: "lending analysis",
			run: func(t *testing.T, b *textract.InMemoryBackend) func() string {
				t.Helper()
				job, err := b.StartLendingAnalysis("s3://bucket/doc.pdf")
				require.NoError(t, err)

				return func() string {
					got, gErr := b.GetLendingAnalysis(job.JobID)
					require.NoError(t, gErr)

					return got.JobStatus
				}
			},
			wantSucceededOnZeroDelay: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"/zero delay transitions promptly", func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			status := tt.run(t, b)

			assert.Equal(t, tt.wantSucceededOnZeroDelay, status())
		})

		t.Run(tt.name+"/shutdown cancels pending transition", func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")
			// Long delay so the transition cannot fire before Shutdown.
			textract.SetBackendAsyncDelay(b, time.Hour)

			status := tt.run(t, b)
			require.Equal(t, "IN_PROGRESS", status())

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			b.Shutdown(ctx)
			require.NoError(t, ctx.Err(), "Shutdown should return before its bound context expires")

			// Goroutine was cancelled before mutating state.
			assert.Equal(t, "IN_PROGRESS", status())
		})
	}
}

// TestInMemoryBackend_ShutdownIdempotent verifies Shutdown is safe to call when
// there is no pending work and returns promptly.
func TestInMemoryBackend_ShutdownIdempotent(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	b.Shutdown(ctx)
	assert.NoError(t, ctx.Err())
}
