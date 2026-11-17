package transcribe_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

type limitedFilterMedia struct {
	wav    []byte
	filter []byte
	limits []int64
	mu     sync.Mutex
}

func (m *limitedFilterMedia) GetObject(_ context.Context, _, key string) ([]byte, error) {
	if strings.HasSuffix(key, ".wav") {
		return m.wav, nil
	}

	return m.filter, nil
}

func (m *limitedFilterMedia) GetObjectLimit(ctx context.Context, b, key string, limit int64) ([]byte, error) {
	m.mu.Lock()
	m.limits = append(m.limits, limit)
	m.mu.Unlock()

	return m.GetObject(ctx, b, key)
}

func TestCallAnalyticsAppliesVocabularyFilterFile(t *testing.T) {
	t.Parallel()

	puts := make(chan string, 4)
	media := &limitedFilterMedia{wav: wavFile(1, 0.1), filter: []byte("Darn\n")}
	b := transcribe.NewInMemoryBackend()
	b.SetMediaReader(media)
	b.SetS3Writer(signalWriter{puts: puts})
	b.SetEngine(wordsEngine{words: []transcribe.Word{
		{Text: "say", End: 0.1, Confidence: 1}, {Text: "darn", Start: 0.1, End: 0.2, Confidence: 1},
	}})
	t.Cleanup(func() { b.Shutdown(context.WithoutCancel(t.Context())) })

	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{
		VocabularyFilterName: "f", LanguageCode: "en-US", VocabularyFilterFileURI: "s3://in/words.txt",
	})

	_, err := b.StartCallAnalyticsJob(&transcribe.CallAnalyticsJob{
		CallAnalyticsJobName: "ca", LanguageCode: "en-US", MediaFormat: "wav",
		Media:          transcribe.Media{MediaFileURI: "s3://in/a.wav"},
		Settings:       &transcribe.CallAnalyticsSettings{VocabularyFilterName: "f", VocabularyFilterMethod: "mask"},
		OutputLocation: "s3://out/",
	})
	require.NoError(t, err)

	body := <-puts
	assert.Contains(t, body, "***")
	assert.NotContains(t, body, "darn")

	media.mu.Lock()
	defer media.mu.Unlock()
	assert.Equal(t, []int64{1 << 20}, media.limits, "the filter file is read with a streaming cap")
}
