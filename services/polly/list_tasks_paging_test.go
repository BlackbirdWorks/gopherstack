package polly_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/aws/aws-sdk-go-v2/service/polly/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

// TestListSpeechSynthesisTasks_StatusPaging checks the token resumes after the last scanned task, not the last match.
func TestListSpeechSynthesisTasks_StatusPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status types.TaskStatus
		want   []int
		size   int32
	}{
		{name: "completed_two_then_one", status: types.TaskStatusCompleted, size: 2, want: []int{2, 1}},
		{name: "completed_exact_division", status: types.TaskStatusCompleted, size: 3, want: []int{3}},
		{name: "failed_single_page", status: types.TaskStatusFailed, size: 5, want: []int{2}},
		{name: "all_statuses", size: 2, want: []int{2, 2, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestPollySDKClient(t, polly.NewHandler(polly.NewInMemoryBackend()))

			for _, text := range []string{"a", "b [fail]", "c", "d [fail]", "e"} {
				_, err := client.StartSpeechSynthesisTask(t.Context(), &pollysdk.StartSpeechSynthesisTaskInput{
					OutputFormat: types.OutputFormatMp3, OutputS3BucketName: aws.String("bucket"),
					Text: aws.String(text), VoiceId: types.VoiceIdJoanna,
				})
				require.NoError(t, err)
			}

			for range 2 {
				_, err := client.ListSpeechSynthesisTasks(t.Context(), &pollysdk.ListSpeechSynthesisTasksInput{})
				require.NoError(t, err)
			}

			var (
				token *string
				got   []int
			)

			for range 6 {
				out, err := client.ListSpeechSynthesisTasks(t.Context(), &pollysdk.ListSpeechSynthesisTasksInput{
					Status: tt.status, MaxResults: aws.Int32(tt.size), NextToken: token,
				})
				require.NoError(t, err)

				got = append(got, len(out.SynthesisTasks))
				if token = out.NextToken; token == nil {
					break
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
