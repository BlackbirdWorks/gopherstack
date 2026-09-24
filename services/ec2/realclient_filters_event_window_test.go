package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_DescribeInstanceEventWindowsFilters covers
// DescribeInstanceEventWindows, which previously ignored Filters entirely.
func TestRealClient_DescribeInstanceEventWindowsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	ewA, err := client.CreateInstanceEventWindow(t.Context(), &ec2sdk.CreateInstanceEventWindowInput{
		Name:           aws.String("window-a"),
		CronExpression: aws.String("0-4 * * * 1,5"),
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeInstanceEventWindow,
			Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("infra")}},
		}},
	})
	require.NoError(t, err)
	ewB, err := client.CreateInstanceEventWindow(t.Context(), &ec2sdk.CreateInstanceEventWindowInput{
		Name:           aws.String("window-b"),
		CronExpression: aws.String("0-4 * * * 1,5"),
	})
	require.NoError(t, err)

	_, err = client.AssociateInstanceEventWindow(t.Context(), &ec2sdk.AssociateInstanceEventWindowInput{
		InstanceEventWindowId: ewA.InstanceEventWindow.InstanceEventWindowId,
		AssociationTarget:     &types.InstanceEventWindowAssociationRequest{InstanceIds: []string{"i-aaa111"}},
	})
	require.NoError(t, err)
	_, err = client.AssociateInstanceEventWindow(t.Context(), &ec2sdk.AssociateInstanceEventWindowInput{
		InstanceEventWindowId: ewB.InstanceEventWindow.InstanceEventWindowId,
		AssociationTarget:     &types.InstanceEventWindowAssociationRequest{DedicatedHostIds: []string{"h-bbb222"}},
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "event-window-name",
			filters: []types.Filter{{Name: aws.String("event-window-name"), Values: []string{"window-a"}}},
			want:    []string{aws.ToString(ewA.InstanceEventWindow.InstanceEventWindowId)},
		},
		{
			name:    "instance-id",
			filters: []types.Filter{{Name: aws.String("instance-id"), Values: []string{"i-aaa111"}}},
			want:    []string{aws.ToString(ewA.InstanceEventWindow.InstanceEventWindowId)},
		},
		{
			name:    "dedicated-host-id",
			filters: []types.Filter{{Name: aws.String("dedicated-host-id"), Values: []string{"h-bbb222"}}},
			want:    []string{aws.ToString(ewB.InstanceEventWindow.InstanceEventWindowId)},
		},
		{
			name:    "tag:Team",
			filters: []types.Filter{{Name: aws.String("tag:Team"), Values: []string{"infra"}}},
			want:    []string{aws.ToString(ewA.InstanceEventWindow.InstanceEventWindowId)},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Team"}}},
			want:    []string{aws.ToString(ewA.InstanceEventWindow.InstanceEventWindowId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("instance-id"), Values: []string{"i-missing"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeInstanceEventWindows(
				t.Context(), &ec2sdk.DescribeInstanceEventWindowsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.InstanceEventWindows))
			for _, ew := range out.InstanceEventWindows {
				got = append(got, aws.ToString(ew.InstanceEventWindowId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
