package transcribe_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

type gatedEngine struct{ release chan struct{} }

func (gatedEngine) SupportsLanguage(string) bool { return true }

func (e gatedEngine) Recognize(ctx context.Context, _ string, _ int, _ []byte) ([]transcribe.Word, error) {
	select {
	case <-e.release:
	case <-ctx.Done():
	}

	return []transcribe.Word{{Text: "hi", Start: 0, End: 0.2, Confidence: 1}}, nil
}

type staticMedia struct{ data []byte }

func (m staticMedia) GetObject(context.Context, string, string) ([]byte, error) { return m.data, nil }

// A copy handed out while the job is in progress must not alias state the run goroutine
// writes on completion: handlers read the copy after the backend lock is released.
func TestJobCopy_NotAliasedToCompletion(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	b := transcribe.NewInMemoryBackend()
	b.SetMediaReader(staticMedia{data: wavFile(1, 0.1)})
	b.SetS3Writer(&fakeS3{})
	b.SetEngine(gatedEngine{release: release})
	t.Cleanup(func() { b.Shutdown(context.WithoutCancel(t.Context())) })

	started, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName: "j", LanguageCode: "en-US", MediaFormat: "wav",
		Media:            transcribe.Media{MediaFileURI: "s3://in/a.wav"},
		OutputBucketName: "out",
		Subtitles:        &transcribe.SubtitlesOutput{Formats: []string{"vtt"}},
	})
	require.NoError(t, err)
	require.Equal(t, "IN_PROGRESS", started.JobStatus)

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = started.Subtitles.SubtitleFileURIs
			}
		}
	})

	close(release)

	require.Eventually(t, func() bool {
		job, getErr := b.GetTranscriptionJob("j")

		return getErr == nil && job.JobStatus == "COMPLETED"
	}, 10*time.Second, time.Millisecond)

	close(stop)
	wg.Wait()

	assert.Empty(t, started.Subtitles.SubtitleFileURIs)
}
