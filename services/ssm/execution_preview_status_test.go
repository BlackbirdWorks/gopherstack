package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutionPreview_SDKShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *ssmsdk.Client)
		name string
	}{
		{name: "started preview reports an SDK status", run: func(t *testing.T, client *ssmsdk.Client) {
			t.Helper()

			start, err := client.StartExecutionPreview(t.Context(), &ssmsdk.StartExecutionPreviewInput{
				DocumentName: aws.String("AWS-Test"),
			})
			require.NoError(t, err)

			got, err := client.GetExecutionPreview(t.Context(), &ssmsdk.GetExecutionPreviewInput{
				ExecutionPreviewId: start.ExecutionPreviewId,
			})
			require.NoError(t, err)
			assert.Equal(t, ssmtypes.ExecutionPreviewStatusInProgress, got.Status)
		}},
		{name: "unknown preview is a typed ResourceNotFoundException", run: func(t *testing.T, client *ssmsdk.Client) {
			t.Helper()

			_, err := client.GetExecutionPreview(t.Context(), &ssmsdk.GetExecutionPreviewInput{
				ExecutionPreviewId: aws.String("missing"),
			})
			var nf *ssmtypes.ResourceNotFoundException
			require.ErrorAs(t, err, &nf)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			tc.run(t, newTestSSMClient(t, h))
		})
	}
}
