package polly_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

var errWrite = errors.New("write failed")

type fakeS3 struct {
	err     error
	objects map[string][]byte
	mu      sync.Mutex
}

func (f *fakeS3) PutObject(_ context.Context, bucket, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	if f.objects == nil {
		f.objects = map[string][]byte{}
	}

	f.objects[bucket+"/"+key] = data

	return nil
}

func TestSynthesisTaskWritesOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		writerErr  error
		name       string
		prefix     string
		ext        string
		wantStatus string
		opts       polly.SynthesisOptions
	}{
		{
			name: "mp3", opts: polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna", OutputFormat: "mp3"},
			prefix: "out/", ext: "mp3", wantStatus: "completed",
		},
		{
			name: "ogg", opts: polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna", OutputFormat: "ogg_vorbis"},
			ext: "ogg", wantStatus: "completed",
		},
		{
			name: "pcm", opts: polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna", OutputFormat: "pcm"},
			prefix: "a/b-", ext: "pcm", wantStatus: "completed",
		},
		{
			name: "json", opts: polly.SynthesisOptions{
				Text: "hello world", VoiceID: "Joanna", OutputFormat: "json", SpeechMarkTypes: []string{"word"},
			},
			ext: "json", wantStatus: "completed",
		},
		{
			name: "write error", opts: polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna", OutputFormat: "mp3"},
			ext: "mp3", writerErr: errWrite, wantStatus: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &fakeS3{err: tt.writerErr}
			b := polly.NewInMemoryBackend()
			b.SetS3Writer(w)

			task, err := b.StartSpeechSynthesisTask(tt.opts, "bkt", tt.prefix, "")
			require.NoError(t, err)

			var got *polly.SpeechSynthesisTask

			for range 2 {
				got, err = b.GetSpeechSynthesisTask(task.TaskID)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantStatus, got.TaskStatus)

			key := "bkt/" + tt.prefix + task.TaskID + "." + tt.ext
			assert.Equal(t, "s3://"+key, got.OutputURI)

			if tt.wantStatus == "failed" {
				assert.NotEmpty(t, got.TaskStatusReason)
				assert.Empty(t, w.objects)

				return
			}

			assert.NotEmpty(t, w.objects[key])
		})
	}
}

func TestSynthesisTaskNoWriter(t *testing.T) {
	t.Parallel()

	b := polly.NewInMemoryBackend()
	task, err := b.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: "hi", VoiceID: "Joanna"}, "bkt", "", "",
	)
	require.NoError(t, err)

	var got *polly.SpeechSynthesisTask

	for range 2 {
		got, err = b.GetSpeechSynthesisTask(task.TaskID)
		require.NoError(t, err)
	}

	assert.Equal(t, "completed", got.TaskStatus)
}
