package opensearch_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDataSourceAttachments_SDKPagination drives the real aws-sdk-go-v2
// client. ListDataSourceAttachmentsInput carries real MaxResults/NextToken
// members, sent as body fields "maxResults"/"nextToken" per this op's own
// awsRestjson1_serializeOpDocumentListDataSourceAttachmentsInput
// (opensearch@v1.75.4 serializers.go) -- unlike its ListMigrations sibling,
// which uses HTTP query params for the same concept
// (awsRestjson1_serializeOpHttpBindingsListMigrationsInput), confirming each
// op's own serializer, not a shared family convention. The handler
// previously never read either at all, so every call returned the full,
// unpaginated attachment set. b.dataSourceAttachmentsByApp is a
// pkgs/store.Index, whose Get() is insertion-ordered and stable across
// calls, so no additional sort/tiebreak is needed to paginate it safely.
func TestListDataSourceAttachments_SDKPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	appID := createTestApplication(t, h, "paginated-app")

	const numAttachments = 5

	wantArns := make(map[string]bool, numAttachments)

	for i := range numAttachments {
		arn := domainARN(t, h, fmt.Sprintf("ds-domain-%d", i))

		_, err := client.AttachDataSource(t.Context(), &opensearchsdk.AttachDataSourceInput{
			Id:            aws.String(appID),
			DataSourceArn: aws.String(arn),
		})
		require.NoError(t, err)
		wantArns[arn] = true
	}

	require.Len(t, wantArns, numAttachments, "data source ARNs must be unique")

	gotArns := make(map[string]bool)

	input := &opensearchsdk.ListDataSourceAttachmentsInput{
		Id:         aws.String(appID),
		MaxResults: 2,
	}

	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "pagination did not terminate")

		out, err := client.ListDataSourceAttachments(t.Context(), input)
		require.NoError(t, err)
		require.LessOrEqual(t, len(out.Attachments), 2, "must honor MaxResults")

		for _, a := range out.Attachments {
			require.NotNil(t, a.DataSourceArn)
			gotArns[*a.DataSourceArn] = true
		}

		if out.NextToken == nil || *out.NextToken == "" {
			break
		}

		input.NextToken = out.NextToken
	}

	assert.Equal(t, wantArns, gotArns, "paginated union must equal the seeded attachment set exactly")
}

// TestListDataSourceAttachments_UnknownApplication proves an unknown
// application ID is rejected (ResourceNotFoundException, matching
// AttachDataSource/DetachDataSource/DescribeDataSourceAttachment's existing
// writeAttachmentError convention for this same op family), not silently
// answered with an empty list -- the same "did not validate the resource its
// own siblings validate" bug shape as this family's siblings.
func TestListDataSourceAttachments_UnknownApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)

	_, err := client.ListDataSourceAttachments(t.Context(), &opensearchsdk.ListDataSourceAttachmentsInput{
		Id: aws.String("no-such-app"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())

	var nf *types.ResourceNotFoundException
	assert.ErrorAs(t, err, &nf, "must decode as the real typed ResourceNotFoundException")
}
