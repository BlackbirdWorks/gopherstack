package integration_test

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	pollytypes "github.com/aws/aws-sdk-go-v2/service/polly/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPollyClient returns a Polly client pointed at the shared test container.
func createPollyClient(t *testing.T) *pollysdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return pollysdk.NewFromConfig(cfg, func(o *pollysdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Polly_SynthesizeSpeech asserts that SynthesizeSpeech returns a non-empty
// audio stream with the requested content type.
func TestIntegration_Polly_SynthesizeSpeech(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name        string
		text        string
		voice       pollytypes.VoiceId
		format      pollytypes.OutputFormat
		contentType string
	}{
		{name: "mp3", text: "Hello from Polly", voice: pollytypes.VoiceIdJoanna, format: pollytypes.OutputFormatMp3, contentType: "audio/mpeg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createPollyClient(t)

			out, err := client.SynthesizeSpeech(t.Context(), &pollysdk.SynthesizeSpeechInput{
				Text:         aws.String(tt.text),
				VoiceId:      tt.voice,
				OutputFormat: tt.format,
			})
			require.NoError(t, err, "SynthesizeSpeech should succeed")
			require.NotNil(t, out.AudioStream)
			defer out.AudioStream.Close()

			data, err := io.ReadAll(out.AudioStream)
			require.NoError(t, err, "reading audio stream should succeed")
			assert.NotEmpty(t, data, "synthesized audio must be non-empty")
			assert.Equal(t, tt.contentType, aws.ToString(out.ContentType))
		})
	}
}

// TestIntegration_Polly_LexiconLifecycle drives put→get→list→delete of a pronunciation lexicon.
func TestIntegration_Polly_LexiconLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	const lexiconXML = `<?xml version="1.0" encoding="UTF-8"?>
<lexicon version="1.0" xmlns="http://www.w3.org/2005/01/pronunciation-lexicon" alphabet="ipa" xml:lang="en-US">
  <lexeme><grapheme>W3C</grapheme><alias>World Wide Web Consortium</alias></lexeme>
</lexicon>`

	tests := []struct {
		name        string
		lexiconName string
	}{
		{name: "full_lifecycle", lexiconName: "integLexicon"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createPollyClient(t)

			_, err := client.PutLexicon(ctx, &pollysdk.PutLexiconInput{
				Name:    aws.String(tt.lexiconName),
				Content: aws.String(lexiconXML),
			})
			require.NoError(t, err, "PutLexicon should succeed")

			t.Cleanup(func() {
				_, _ = client.DeleteLexicon(ctx, &pollysdk.DeleteLexiconInput{Name: aws.String(tt.lexiconName)})
			})

			getOut, err := client.GetLexicon(ctx, &pollysdk.GetLexiconInput{Name: aws.String(tt.lexiconName)})
			require.NoError(t, err, "GetLexicon should succeed")
			require.NotNil(t, getOut.Lexicon)
			assert.Equal(t, tt.lexiconName, aws.ToString(getOut.Lexicon.Name))

			listOut, err := client.ListLexicons(ctx, &pollysdk.ListLexiconsInput{})
			require.NoError(t, err, "ListLexicons should succeed")

			found := false
			for _, l := range listOut.Lexicons {
				if aws.ToString(l.Name) == tt.lexiconName {
					found = true

					break
				}
			}

			assert.True(t, found, "put lexicon should appear in list")

			_, err = client.DeleteLexicon(ctx, &pollysdk.DeleteLexiconInput{Name: aws.String(tt.lexiconName)})
			require.NoError(t, err, "DeleteLexicon should succeed")
		})
	}
}
