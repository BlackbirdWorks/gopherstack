package transcribe_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

type signalWriter struct{ puts chan string }

func (w signalWriter) PutObject(_ context.Context, _, _ string, data []byte) error {
	w.puts <- string(data)

	return nil
}

type wordsEngine struct{ words []transcribe.Word }

func (wordsEngine) SupportsLanguage(string) bool { return true }

func (e wordsEngine) Recognize(context.Context, string, int, []byte) ([]transcribe.Word, error) {
	return append([]transcribe.Word(nil), e.words...), nil
}

func TestTranscriptionJobAppliesStoredVocabularyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		want   string
	}{
		{name: "mask", method: "mask", want: `"transcript":"say *** now"`},
		{name: "remove", method: "remove", want: `"transcript":"say now"`},
		{name: "tag", method: "tag", want: `"vocabularyFilterMatch":true`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			puts := make(chan string, 4)
			b := transcribe.NewInMemoryBackend()
			b.SetMediaReader(staticMedia{data: wavFile(1, 0.1)})
			b.SetS3Writer(signalWriter{puts: puts})
			b.SetEngine(wordsEngine{words: []transcribe.Word{
				{Text: "say", End: 0.1, Confidence: 1}, {Text: "darn", Start: 0.1, End: 0.2, Confidence: 1},
				{Text: "now", Start: 0.2, End: 0.3, Confidence: 1},
			}})
			t.Cleanup(func() { b.Shutdown(context.WithoutCancel(t.Context())) })

			b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{
				VocabularyFilterName: "f", LanguageCode: "en-US", Words: []string{"darn"},
			})

			_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName: "j", LanguageCode: "en-US", MediaFormat: "wav",
				Media:            transcribe.Media{MediaFileURI: "s3://in/a.wav"},
				OutputBucketName: "out",
				Settings: &transcribe.TranscriptionSettings{
					VocabularyFilterName: "f", VocabularyFilterMethod: tt.method,
				},
			})
			require.NoError(t, err)

			body := <-puts
			assert.Contains(t, body, tt.want)

			if tt.method != "tag" {
				assert.NotContains(t, body, "darn")
			}
		})
	}
}
