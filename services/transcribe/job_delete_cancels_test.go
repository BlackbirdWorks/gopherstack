package transcribe_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

type cancelProbeEngine struct {
	entered   chan struct{}
	cancelled chan struct{}
}

func (cancelProbeEngine) SupportsLanguage(string) bool { return true }

func (e cancelProbeEngine) Recognize(ctx context.Context, _ string, _ int, _ []byte) ([]transcribe.Word, error) {
	close(e.entered)
	<-ctx.Done()
	close(e.cancelled)

	return nil, ctx.Err()
}

func TestDeleteJobCancelsRecognition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		start  func(b *transcribe.InMemoryBackend) error
		delete func(b *transcribe.InMemoryBackend) error
		name   string
	}{
		{
			name: "transcription",
			start: func(b *transcribe.InMemoryBackend) error {
				_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
					JobName: "j", LanguageCode: "en-US", MediaFormat: "wav",
					Media: transcribe.Media{MediaFileURI: "s3://in/a.wav"},
				})

				return err
			},
			delete: func(b *transcribe.InMemoryBackend) error { return b.DeleteTranscriptionJob("j") },
		},
		{
			name: "medical",
			start: func(b *transcribe.InMemoryBackend) error {
				_, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
					MedicalTranscriptionJobName: "j", LanguageCode: "en-US", MediaFormat: "wav",
					Specialty: "PRIMARYCARE", Type: "CONVERSATION",
					Media: transcribe.Media{MediaFileURI: "s3://in/a.wav"}, OutputBucketName: "out",
				})

				return err
			},
			delete: func(b *transcribe.InMemoryBackend) error { return b.DeleteMedicalTranscriptionJob("j") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := cancelProbeEngine{entered: make(chan struct{}), cancelled: make(chan struct{})}
			b := transcribe.NewInMemoryBackend()
			b.SetMediaReader(staticMedia{data: wavFile(1, 0.1)})
			b.SetS3Writer(&fakeS3{})
			b.SetEngine(eng)
			t.Cleanup(func() { b.Shutdown(context.WithoutCancel(t.Context())) })

			require.NoError(t, tt.start(b))

			select {
			case <-eng.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("engine never started")
			}

			require.NoError(t, tt.delete(b))

			select {
			case <-eng.cancelled:
			case <-time.After(10 * time.Second):
				t.Fatal("recognition kept running after the job was deleted")
			}
		})
	}
}
