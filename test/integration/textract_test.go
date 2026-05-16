package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Textract_DetectDocumentText exercises the synchronous
// DetectDocumentText operation via the AWS SDK v2 against the in-memory
// Textract backend.
func TestIntegration_Textract_DetectDocumentText(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createTextractClient(t)
	ctx := t.Context()

	out, err := client.DetectDocumentText(ctx, &textractsdk.DetectDocumentTextInput{
		Document: &textracttypes.Document{
			S3Object: &textracttypes.S3Object{
				Bucket: aws.String("it-textract-bucket"),
				Name:   aws.String("doc.png"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DocumentMetadata)
	assert.Equal(t, int32(1), aws.ToInt32(out.DocumentMetadata.Pages))
}
