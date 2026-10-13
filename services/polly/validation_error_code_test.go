package polly_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

// TestErrValidation_WireCode_ValidationException proves every op that raises
// ErrValidation answers with the generic frontend "ValidationException" code,
// which the SDK surfaces as *smithy.GenericAPIError (none of these ops
// declares it in deserializeOpError; polly@v1.60.4 types/errors.go has no
// "InvalidParameterValueException").
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
		"listspeechsynthesistasks invalid status": func(t *testing.T, client *pollysdk.Client) error {
			t.Helper()

			_, err := client.ListSpeechSynthesisTasks(t.Context(), &pollysdk.ListSpeechSynthesisTasksInput{
				Status: "bogus",
			})

			return err
		},
		"startspeechsynthesistask missing bucket": func(t *testing.T, client *pollysdk.Client) error {
			t.Helper()

			_, err := client.StartSpeechSynthesisTask(t.Context(), &pollysdk.StartSpeechSynthesisTaskInput{
				OutputFormat:       "mp3",
				OutputS3BucketName: aws.String(""),
				Text:               aws.String("hello"),
				VoiceId:            "Joanna",
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
