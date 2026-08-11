package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const quicksightAccountID = "000000000000"

// createQuickSightClient returns a QuickSight client pointed at the shared test container.
func createQuickSightClient(t *testing.T) *quicksightsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return quicksightsdk.NewFromConfig(cfg, func(o *quicksightsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_QuickSight_GroupLifecycle drives create→describe→list→delete of a group in
// the default namespace.
func TestIntegration_QuickSight_GroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		groupName string
	}{
		{name: "full_lifecycle", groupName: "integ-group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createQuickSightClient(t)

			createOut, err := client.CreateGroup(ctx, &quicksightsdk.CreateGroupInput{
				AwsAccountId: aws.String(quicksightAccountID),
				Namespace:    aws.String("default"),
				GroupName:    aws.String(tt.groupName),
				Description:  aws.String("integration test group"),
			})
			require.NoError(t, err, "CreateGroup should succeed")
			require.NotNil(t, createOut.Group)
			assert.Equal(t, tt.groupName, aws.ToString(createOut.Group.GroupName))

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteGroup(cleanupCtx, &quicksightsdk.DeleteGroupInput{
					AwsAccountId: aws.String(quicksightAccountID),
					Namespace:    aws.String("default"),
					GroupName:    aws.String(tt.groupName),
				})
			})

			descOut, err := client.DescribeGroup(ctx, &quicksightsdk.DescribeGroupInput{
				AwsAccountId: aws.String(quicksightAccountID),
				Namespace:    aws.String("default"),
				GroupName:    aws.String(tt.groupName),
			})
			require.NoError(t, err, "DescribeGroup should succeed")
			require.NotNil(t, descOut.Group)
			assert.Equal(t, tt.groupName, aws.ToString(descOut.Group.GroupName))

			listOut, err := client.ListGroups(ctx, &quicksightsdk.ListGroupsInput{
				AwsAccountId: aws.String(quicksightAccountID),
				Namespace:    aws.String("default"),
			})
			require.NoError(t, err, "ListGroups should succeed")

			found := false
			for _, g := range listOut.GroupList {
				if aws.ToString(g.GroupName) == tt.groupName {
					found = true

					break
				}
			}

			assert.True(t, found, "created group should appear in list")

			_, err = client.DeleteGroup(ctx, &quicksightsdk.DeleteGroupInput{
				AwsAccountId: aws.String(quicksightAccountID),
				Namespace:    aws.String("default"),
				GroupName:    aws.String(tt.groupName),
			})
			require.NoError(t, err, "DeleteGroup should succeed")
		})
	}
}
