package transcribe_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// testdata/speech16k.wav is "one two three four" synthesised with espeak-ng 1.51 (en-us, 120 wpm),
// resampled to 16 kHz mono PCM16.
func newDockerVoskEngine(t *testing.T) *transcribe.VoskEngine {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine test skipped in -short mode")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	if err = rt.PullImage(t.Context(), transcribe.DefaultVoskImage); err != nil {
		t.Skipf("image %s unavailable: %v", transcribe.DefaultVoskImage, err)
	}

	return transcribe.NewVoskEngine(transcribe.VoskConfig{Runtime: rt})
}

func TestVoskEngine_Docker(t *testing.T) {
	t.Parallel()

	engine := newDockerVoskEngine(t)
	ctx := t.Context()

	t.Cleanup(func() { engine.Close(ctx) })

	wav, err := os.ReadFile("testdata/speech16k.wav")
	require.NoError(t, err)

	env := newJobEnv(t, engine)
	env.put(t, "speech.wav", wav)

	_, err = env.tr.StartTranscriptionJob(ctx, &transcribesdk.StartTranscriptionJobInput{
		TranscriptionJobName: aws.String("speech"), LanguageCode: sdktypes.LanguageCodeEnUs,
		Media:            &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/speech.wav")},
		MediaFormat:      sdktypes.MediaFormatWav,
		OutputBucketName: aws.String("media-out"),
	})
	require.NoError(t, err)

	var job *sdktypes.TranscriptionJob

	require.Eventually(t, func() bool {
		job = env.job(t, "speech")

		return job.TranscriptionJobStatus != sdktypes.TranscriptionJobStatusInProgress
	}, 10*time.Minute, 500*time.Millisecond)
	require.Equal(
		t,
		sdktypes.TranscriptionJobStatusCompleted,
		job.TranscriptionJobStatus,
		aws.ToString(job.FailureReason),
	)

	doc := parseDoc(t, env.getOut(t, "speech.json"))
	require.Len(t, doc.Results.Transcripts, 1)

	text := doc.Results.Transcripts[0].Transcript
	t.Logf("transcript: %q", text)
	assert.NotEmpty(t, text)
	assert.Subset(t, strings.Fields(text), []string{"two", "three"})
	require.NotEmpty(t, doc.Results.Items)
	assert.Equal(t, "pronunciation", doc.Results.Items[0].Type)
	assert.NotEmpty(t, doc.Results.Items[0].StartTime)
}
