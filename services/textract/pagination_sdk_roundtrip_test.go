package textract_test

import (
	"encoding/base64"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetDocumentTextDetection_SDKRoundTrip_CursorAtEndDoesNotRestart drives
// StartDocumentTextDetection -> GetDocumentTextDetection through the real
// aws-sdk-go-v2 textract client with a NextToken equal to the job's total
// block count. This pass found paginateBlocks (services/textract/synthetic_blocks.go)
// treating any decoded offset >= len(blocks) as invalid and silently
// resetting to offset 0 -- restarting pagination from the first block
// instead of returning empty, the same bug class independently found in
// dax's paginateParameters and DescribeEvents. Ties the unit-level
// reproduction in pagination_arithmetic_internal_test.go to observable
// behaviour through the typed SDK client and its own deserializer.
func TestGetDocumentTextDetection_SDKRoundTrip_CursorAtEndDoesNotRestart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestTextractClient(t, h)

	started, err := client.StartDocumentTextDetection(t.Context(), &textractsdk.StartDocumentTextDetectionInput{
		DocumentLocation: &textracttypes.DocumentLocation{
			S3Object: &textracttypes.S3Object{Bucket: aws.String("b"), Name: aws.String("doc.pdf")},
		},
	})
	require.NoError(t, err)

	first, err := client.GetDocumentTextDetection(t.Context(), &textractsdk.GetDocumentTextDetectionInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.Blocks, "synthetic job must produce at least one block")

	tokenAtEnd := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(len(first.Blocks))))

	page, err := client.GetDocumentTextDetection(t.Context(), &textractsdk.GetDocumentTextDetectionInput{
		JobId:     started.JobId,
		NextToken: aws.String(tokenAtEnd),
	})
	require.NoError(t, err)
	assert.Empty(t, page.Blocks, "a token at the end of the block list must not restart pagination from the beginning")
	assert.Nil(t, page.NextToken)
}
