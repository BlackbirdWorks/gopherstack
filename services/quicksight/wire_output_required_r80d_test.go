package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestListSearchSpaces_SpaceIdSpaceArn_RealClient covers gopherstack-r80d
// (required-output-member sweep). ListSpacesOutput and SearchSpacesOutput
// both require SpaceId and SpaceArn (quicksight@v1.123.1
// api_op_ListSpaces.go:44-63, api_op_SearchSpaces.go:49-68) even though
// neither op is scoped to any single space -- ListSpacesInput/
// SearchSpacesInput carry only AwsAccountId (+Filters for Search), no
// SpaceId at all. The handlers never emitted spaceId/spaceArn (the real
// wire keys, confirmed camelCase against deserializers.go), so a real
// client's *string fields always decoded nil. Driven through the real SDK
// client since a hand-built response fixture would not surface a
// structurally-missing key the way the SDK's own deserializer does.
func TestListSearchSpaces_SpaceIdSpaceArn_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("listspaces", func(t *testing.T) {
		t.Parallel()

		backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
		h := quicksight.NewHandler(backend)
		client := newTestQuickSightClient(t, h)

		out, err := client.ListSpaces(t.Context(), &quicksightsdk.ListSpacesInput{
			AwsAccountId: aws.String("000000000000"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.SpaceId)
		assert.Empty(t, *out.SpaceId)
		require.NotNil(t, out.SpaceArn)
		assert.Empty(t, *out.SpaceArn)
	})

	t.Run("searchspaces", func(t *testing.T) {
		t.Parallel()

		backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
		h := quicksight.NewHandler(backend)
		client := newTestQuickSightClient(t, h)

		out, err := client.SearchSpaces(t.Context(), &quicksightsdk.SearchSpacesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.SpaceQuicksightSearchFilter{
				{
					Name:     types.SpaceQuickSightSearchFilterNameSpaceName,
					Operator: types.SpaceSearchOperatorStringLike,
					Value:    aws.String("Support"),
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, out.SpaceId)
		assert.Empty(t, *out.SpaceId)
		require.NotNil(t, out.SpaceArn)
		assert.Empty(t, *out.SpaceArn)
	})
}
