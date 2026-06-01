package polly_test

import (
	"strings"
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
		run  func() error
		name string
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
			t.Parallel()

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

func TestLexiconNameValidation(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	content := `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`
	tests := []struct {
		name    string
		lexName string
		wantErr bool
	}{
		{name: "valid_alpha", lexName: "medical", wantErr: false},
		{name: "valid_alphanumeric", lexName: "lexicon1", wantErr: false},
		{name: "empty", lexName: "", wantErr: true},
		{name: "space", lexName: "bad name", wantErr: true},
		{name: "hyphen", lexName: "my-lex", wantErr: true},
		{name: "underscore", lexName: "my_lex", wantErr: true},
		{name: "too_long", lexName: strings.Repeat("a", 21), wantErr: true},
		{name: "exactly_20", lexName: strings.Repeat("a", 20), wantErr: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := backend.PutLexicon(test.lexName, content)
			if test.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, polly.ErrValidation)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSynthesisTextLimits(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	tests := []struct {
		name     string
		text     string
		textType string
		wantErr  bool
	}{
		{name: "text_at_limit", text: strings.Repeat("a", 3000), textType: "text", wantErr: false},
		{name: "text_over_limit", text: strings.Repeat("a", 3001), textType: "text", wantErr: true},
		{name: "ssml_at_limit", text: strings.Repeat("a", 6000), textType: "ssml", wantErr: false},
		{name: "ssml_over_limit", text: strings.Repeat("a", 6001), textType: "ssml", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := backend.SynthesizeSpeech(polly.SynthesisOptions{
				Text:     test.text,
				VoiceID:  "Joanna",
				TextType: test.textType,
			})
			if test.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, polly.ErrValidation)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStartTaskTextLimit(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	_, err := backend.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: strings.Repeat("a", 100001), VoiceID: "Joanna"},
		"bucket", "", "",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, polly.ErrValidation)
}

func TestLexiconNamesLimit(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	content := `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`
	names := []string{"lex1", "lex2", "lex3", "lex4", "lex5", "lex6"}
	for _, name := range names {
		require.NoError(t, backend.PutLexicon(name, content))
	}

	_, err := backend.SynthesizeSpeech(polly.SynthesisOptions{
		Text:         "hello",
		VoiceID:      "Joanna",
		LexiconNames: names,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, polly.ErrValidation)
}

func TestTagResourceNotFound(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	unknown := "arn:aws:polly:us-east-1:123456789012:synthesis-task/nope"
	tags := []polly.Tag{{Key: "k", Value: "v"}}

	err := backend.TagResource(unknown, tags)
	require.ErrorIs(t, err, polly.ErrResourceNotFound)

	err = backend.UntagResource(unknown, []string{"k"})
	require.ErrorIs(t, err, polly.ErrResourceNotFound)

	_, err = backend.ListTagsForResource(unknown)
	require.ErrorIs(t, err, polly.ErrResourceNotFound)
}

func TestTagValidationLimits(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	task, err := backend.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna"},
		"bucket", "", "",
	)
	require.NoError(t, err)
	arn := backend.TaskARN(task.TaskID)

	tests := []struct {
		name string
		tags []polly.Tag
	}{
		{name: "key_too_long", tags: []polly.Tag{{Key: strings.Repeat("k", 129), Value: "v"}}},
		{name: "value_too_long", tags: []polly.Tag{{Key: "k", Value: strings.Repeat("v", 257)}}},
		{name: "duplicate_key", tags: []polly.Tag{{Key: "k", Value: "v1"}, {Key: "k", Value: "v2"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			tagErr := backend.TagResource(arn, test.tags)
			require.Error(t, tagErr)
			assert.ErrorIs(t, tagErr, polly.ErrValidation)
		})
	}
}

func TestTagCountLimit(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	task, err := backend.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna"},
		"bucket", "", "",
	)
	require.NoError(t, err)
	arn := backend.TaskARN(task.TaskID)

	bulk := make([]polly.Tag, 50)
	for i := range bulk {
		bulk[i] = polly.Tag{Key: strings.Repeat("k", i+1), Value: "v"}
	}
	require.NoError(t, backend.TagResource(arn, bulk))

	err = backend.TagResource(arn, []polly.Tag{{Key: "extra", Value: "v"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, polly.ErrValidation)
}
