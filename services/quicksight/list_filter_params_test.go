package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestSearchGroups_Filters proves SearchGroups honors its (required) Filters
// member. handleSearchGroups (handler_group.go) read a "Query" field from
// the JSON body -- a field SearchGroupsInput doesn't have at all -- instead
// of "Filters" (GROUP_NAME/StartsWith, the only filter the real API
// supports; confirmed against SearchGroupsInput/GroupSearchFilter in
// api_op_SearchGroups.go and types/types.go). A real client's Filters were
// silently dropped and every group in the namespace came back regardless.
func TestSearchGroups_Filters(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	for _, name := range []string{"admins", "analysts", "auditors-readonly"} {
		_, err := client.CreateGroup(ctx, &quicksightsdk.CreateGroupInput{
			AwsAccountId: aws.String("000000000000"),
			Namespace:    aws.String("default"),
			GroupName:    aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.SearchGroups(ctx, &quicksightsdk.SearchGroupsInput{
		AwsAccountId: aws.String("000000000000"),
		Namespace:    aws.String("default"),
		Filters: []types.GroupSearchFilter{
			{
				Name:     types.GroupFilterAttributeGroupName,
				Operator: types.GroupFilterOperatorStartsWith,
				Value:    aws.String("admin"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.GroupList, 1)
	require.Equal(t, "admins", aws.ToString(out.GroupList[0].GroupName))
}

// TestSearchGroups_Pagination proves MaxResults/NextToken are honored.
// Both are query-string bound (max-results/next-token, confirmed against
// serializers.go's awsRestjson1_serializeOpHttpBindingsSearchGroupsInput),
// but the handler read them from the JSON body instead, where a real client
// never puts them.
func TestSearchGroups_Pagination(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	for _, name := range []string{"group-a", "group-b", "group-c"} {
		_, err := client.CreateGroup(ctx, &quicksightsdk.CreateGroupInput{
			AwsAccountId: aws.String("000000000000"),
			Namespace:    aws.String("default"),
			GroupName:    aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.SearchGroups(ctx, &quicksightsdk.SearchGroupsInput{
		AwsAccountId: aws.String("000000000000"),
		Namespace:    aws.String("default"),
		Filters: []types.GroupSearchFilter{
			{
				Name:     types.GroupFilterAttributeGroupName,
				Operator: types.GroupFilterOperatorStartsWith,
				Value:    aws.String("group-"),
			},
		},
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out.GroupList, 2)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}
