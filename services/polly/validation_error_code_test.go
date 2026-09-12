package polly_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

// TestErrValidation_WireCode_ValidationException proves ErrValidation's
// mapped wire code is the real modeled type "ValidationException", not the
// fabricated "InvalidParameterValueException" that names no type anywhere in
// polly's pinned SDK module (polly@v1.60.4 types/errors.go).
//
// None of these operations' own deserializeOpError declares ANY generic
// validation exception (confirmed by reading each switch in
// deserializers.go), so the real client can never resolve a specific typed
// exception here -- the strongest available proof is that the response
// decodes as *smithy.GenericAPIError with Code == "ValidationException",
// which is what this asserts. See errors.go's ErrValidation doc comment and
// PARITY.md for the UNCONFIRMED-per-operation disclosure.
func TestErrValidation_WireCode_ValidationException(t *testing.T) {
	t.Parallel()

	tests := map[string]func(t *testing.T, client *pollysdk.Client) error{
		"putlexicon invalid name": func(t *testing.T, client *pollysdk.Client) error {
			t.Helper()

			_, err := client.PutLexicon(t.Context(), &pollysdk.PutLexiconInput{
				Name:    aws.String("bad-name-with-hyphen"),
				Content: aws.String(`<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`),
			})

			return err
		},
		"describevoices invalid engine": func(t *testing.T, client *pollysdk.Client) error {
			t.Helper()

			_, err := client.DescribeVoices(t.Context(), &pollysdk.DescribeVoicesInput{
				Engine: "quantum",
			})

			return err
		},
		"synthesizespeech unknown voice": func(t *testing.T, client *pollysdk.Client) error {
			t.Helper()

			_, err := client.SynthesizeSpeech(t.Context(), &pollysdk.SynthesizeSpeechInput{
				OutputFormat: "mp3",
				Text:         aws.String("hello"),
				VoiceId:      "NotAVoice",
			})

			return err
		},
	}

	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := polly.NewHandler(polly.NewInMemoryBackend())
			client := newTestPollySDKClient(t, h)

			err := call(t, client)
			require.Error(t, err)

			var generic *smithy.GenericAPIError
			require.ErrorAsf(t, err, &generic, "expected a smithy.GenericAPIError, got %v", err)
			require.Equal(t, "ValidationException", generic.Code)
		})
	}
}
