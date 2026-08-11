package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDAXClient returns a DAX client pointed at the shared test container.
func createDAXClient(t *testing.T) *daxsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return daxsdk.NewFromConfig(cfg, func(o *daxsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_DAX_SubnetGroupLifecycle drives create→describe→delete of a subnet group.
func TestIntegration_DAX_SubnetGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		groupName string
		subnets   []string
	}{
		{
			name:      "full_lifecycle",
			groupName: "integ-subnet-group",
			subnets:   []string{"subnet-11111111", "subnet-22222222"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDAXClient(t)

			createOut, err := client.CreateSubnetGroup(ctx, &daxsdk.CreateSubnetGroupInput{
				SubnetGroupName: aws.String(tt.groupName),
				SubnetIds:       tt.subnets,
			})
			require.NoError(t, err, "CreateSubnetGroup should succeed")
			require.NotNil(t, createOut.SubnetGroup)
			assert.Equal(t, tt.groupName, aws.ToString(createOut.SubnetGroup.SubnetGroupName))

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteSubnetGroup(cleanupCtx, &daxsdk.DeleteSubnetGroupInput{
					SubnetGroupName: aws.String(tt.groupName),
				})
			})

			descOut, err := client.DescribeSubnetGroups(ctx, &daxsdk.DescribeSubnetGroupsInput{
				SubnetGroupNames: []string{tt.groupName},
			})
			require.NoError(t, err, "DescribeSubnetGroups should succeed")
			require.Len(t, descOut.SubnetGroups, 1)
			assert.Equal(t, tt.groupName, aws.ToString(descOut.SubnetGroups[0].SubnetGroupName))
			assert.Len(t, descOut.SubnetGroups[0].Subnets, len(tt.subnets))

			_, err = client.DeleteSubnetGroup(ctx, &daxsdk.DeleteSubnetGroupInput{
				SubnetGroupName: aws.String(tt.groupName),
			})
			require.NoError(t, err, "DeleteSubnetGroup should succeed")
		})
	}
}

// TestIntegration_DAX_ParameterGroupLifecycle drives create→describe→delete of a parameter group.
func TestIntegration_DAX_ParameterGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		groupName string
	}{
		{name: "full_lifecycle", groupName: "integ-param-group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDAXClient(t)

			createOut, err := client.CreateParameterGroup(ctx, &daxsdk.CreateParameterGroupInput{
				ParameterGroupName: aws.String(tt.groupName),
				Description:        aws.String("integration test parameter group"),
			})
			require.NoError(t, err, "CreateParameterGroup should succeed")
			require.NotNil(t, createOut.ParameterGroup)
			assert.Equal(t, tt.groupName, aws.ToString(createOut.ParameterGroup.ParameterGroupName))

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteParameterGroup(cleanupCtx, &daxsdk.DeleteParameterGroupInput{
					ParameterGroupName: aws.String(tt.groupName),
				})
			})

			descOut, err := client.DescribeParameterGroups(ctx, &daxsdk.DescribeParameterGroupsInput{
				ParameterGroupNames: []string{tt.groupName},
			})
			require.NoError(t, err, "DescribeParameterGroups should succeed")
			require.Len(t, descOut.ParameterGroups, 1)
			assert.Equal(t, tt.groupName, aws.ToString(descOut.ParameterGroups[0].ParameterGroupName))

			_, err = client.DeleteParameterGroup(ctx, &daxsdk.DeleteParameterGroupInput{
				ParameterGroupName: aws.String(tt.groupName),
			})
			require.NoError(t, err, "DeleteParameterGroup should succeed")
		})
	}
}
