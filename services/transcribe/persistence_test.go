package transcribe_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *transcribe.InMemoryBackend) string
		verify func(t *testing.T, b *transcribe.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *transcribe.InMemoryBackend) string {
				job, err := b.StartTranscriptionJob("test-job", "en-US", "s3://my-bucket/audio.mp3")
				if err != nil {
					return ""
				}

				return job.JobName
			},
			verify: func(t *testing.T, b *transcribe.InMemoryBackend, id string) {
				t.Helper()

				job, err := b.GetTranscriptionJob(id)
				require.NoError(t, err)
				assert.Equal(t, id, job.JobName)
				assert.Equal(t, "en-US", job.LanguageCode)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *transcribe.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *transcribe.InMemoryBackend, _ string) {
				t.Helper()

				jobs, _ := b.ListTranscriptionJobs("", "")
				assert.Empty(t, jobs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := transcribe.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := transcribe.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
