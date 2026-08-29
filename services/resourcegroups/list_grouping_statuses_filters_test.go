package resourcegroups_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resourcegroupssdk "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestListGroupingStatuses_FiltersRoundTrip proves the real SDK client's
// ListGroupingStatusesInput.Filters member now genuinely narrows the result
// set. The real serializer (awsRestjson1_serializeOpDocumentListGroupingStatusesInput,
// resourcegroups@v1.36.4 serializers.go:927) puts Filters on the wire as a
// top-level "Filters" array of {Name, Values} objects; gopherstack's
// listGroupingStatusesInput struct previously had no Filters field at all,
// so json.Unmarshal silently dropped it and every real client's Filters was
// a no-op regardless of the "status"/"resource-arn" values requested
// (types.ListGroupingStatusesFilterName's only two values). The test creates
// both a SUCCESS and a FAILED status entry to prove the "status" filter
// EXCLUDES the non-matching one, not just includes the matching one.
func TestListGroupingStatuses_FiltersRoundTrip(t *testing.T) {
	t.Parallel()

	const region = "us-east-1"
	backend := resourcegroups.NewInMemoryBackend("000000000000", region)
	client := newTestResourceGroupsClient(t, resourcegroups.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateGroup(ctx, &resourcegroupssdk.CreateGroupInput{
		Name: aws.String("filters-rt-group"),
	})
	require.NoError(t, err)

	const memberARN = "arn:aws:s3:::filters-rt-success-bucket"
	const nonMemberARN = "arn:aws:s3:::filters-rt-failed-bucket"

	_, err = client.GroupResources(ctx, &resourcegroupssdk.GroupResourcesInput{
		Group:        aws.String("filters-rt-group"),
		ResourceArns: []string{memberARN},
	})
	require.NoError(t, err)

	// Ungrouping an ARN that was never a member records a FAILED status entry,
	// giving the group both a SUCCESS and a FAILED entry to filter between.
	_, err = client.UngroupResources(ctx, &resourcegroupssdk.UngroupResourcesInput{
		Group:        aws.String("filters-rt-group"),
		ResourceArns: []string{nonMemberARN},
	})
	require.NoError(t, err)

	out, err := client.ListGroupingStatuses(ctx, &resourcegroupssdk.ListGroupingStatusesInput{
		Group: aws.String("filters-rt-group"),
		Filters: []types.ListGroupingStatusesFilter{
			{Name: types.ListGroupingStatusesFilterNameStatus, Values: []string{"SUCCESS"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.GroupingStatuses, 1, "status=SUCCESS filter must exclude the FAILED entry")
	require.Equal(t, memberARN, aws.ToString(out.GroupingStatuses[0].ResourceArn))
	require.Equal(t, "SUCCESS", string(out.GroupingStatuses[0].Status))

	out, err = client.ListGroupingStatuses(ctx, &resourcegroupssdk.ListGroupingStatusesInput{
		Group: aws.String("filters-rt-group"),
		Filters: []types.ListGroupingStatusesFilter{
			{Name: types.ListGroupingStatusesFilterNameResourceArn, Values: []string{nonMemberARN}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.GroupingStatuses, 1, "resource-arn filter must exclude the SUCCESS entry for the other ARN")
	require.Equal(t, nonMemberARN, aws.ToString(out.GroupingStatuses[0].ResourceArn))
	require.Equal(t, "FAILED", string(out.GroupingStatuses[0].Status))
}
