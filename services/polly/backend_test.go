package polly_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestBackendConcurrentFriendlyCopies(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	require.NoError(t, backend.PutLexicon("copy", `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`))
	first, err := backend.GetLexicon("copy")
	require.NoError(t, err)
	first.Content = "changed"
	second, err := backend.GetLexicon("copy")
	require.NoError(t, err)
	assert.NotEqual(t, first.Content, second.Content)
}

func TestBackendLexiconAndTagsErrors(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "invalid_lexicon", run: func() error { return backend.PutLexicon("bad name", "xml") }},
		{name: "missing_delete", run: func() error { return backend.DeleteLexicon("absent") }},
		{
			name: "missing_tags",
			run:  func() error { return backend.TagResource("arn:none", []polly.Tag{{Key: "a", Value: "b"}}) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Error(t, test.run())
		})
	}
}

func TestBackendSynthesisDefaults(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	audio, err := backend.SynthesizeSpeech(polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna"})
	require.NoError(t, err)
	assert.Equal(t, "audio/mpeg", audio.ContentType)
	assert.Contains(t, string(audio.Data), "mp3:22050")
}
