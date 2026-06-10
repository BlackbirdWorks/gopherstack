package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	appstreamtypes "github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAppStreamClient returns an AppStream client pointed at the shared test container.
func createAppStreamClient(t *testing.T) *appstreamsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return appstreamsdk.NewFromConfig(cfg, func(o *appstreamsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_AppStream_StackLifecycle drives create→describe→delete of a stack.
func TestIntegration_AppStream_StackLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		stackName string
	}{
		{name: "full_lifecycle", stackName: "integ-stack"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppStreamClient(t)

			createOut, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{
				Name:        aws.String(tt.stackName),
				Description: aws.String("integration test stack"),
			})
			require.NoError(t, err, "CreateStack should succeed")
			require.NotNil(t, createOut.Stack)
			assert.Equal(t, tt.stackName, aws.ToString(createOut.Stack.Name))

			t.Cleanup(func() {
				_, _ = client.DeleteStack(ctx, &appstreamsdk.DeleteStackInput{Name: aws.String(tt.stackName)})
			})

			descOut, err := client.DescribeStacks(ctx, &appstreamsdk.DescribeStacksInput{
				Names: []string{tt.stackName},
			})
			require.NoError(t, err, "DescribeStacks should succeed")
			require.Len(t, descOut.Stacks, 1)
			assert.Equal(t, tt.stackName, aws.ToString(descOut.Stacks[0].Name))

			_, err = client.DeleteStack(ctx, &appstreamsdk.DeleteStackInput{Name: aws.String(tt.stackName)})
			require.NoError(t, err, "DeleteStack should succeed")
		})
	}
}

// TestIntegration_AppStream_FleetLifecycle drives create→describe→delete of a fleet.
func TestIntegration_AppStream_FleetLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		fleetName    string
		instanceType string
	}{
		{name: "on_demand", fleetName: "integ-fleet", instanceType: "stream.standard.medium"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppStreamClient(t)

			createOut, err := client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
				Name:         aws.String(tt.fleetName),
				InstanceType: aws.String(tt.instanceType),
				FleetType:    appstreamtypes.FleetTypeOnDemand,
				ComputeCapacity: &appstreamtypes.ComputeCapacity{
					DesiredInstances: aws.Int32(1),
				},
				ImageName: aws.String("AppStream-WinServer2019-integ"),
			})
			require.NoError(t, err, "CreateFleet should succeed")
			require.NotNil(t, createOut.Fleet)
			assert.Equal(t, tt.fleetName, aws.ToString(createOut.Fleet.Name))

			t.Cleanup(func() {
				_, _ = client.DeleteFleet(ctx, &appstreamsdk.DeleteFleetInput{Name: aws.String(tt.fleetName)})
			})

			descOut, err := client.DescribeFleets(ctx, &appstreamsdk.DescribeFleetsInput{
				Names: []string{tt.fleetName},
			})
			require.NoError(t, err, "DescribeFleets should succeed")
			require.Len(t, descOut.Fleets, 1)
			assert.Equal(t, tt.instanceType, aws.ToString(descOut.Fleets[0].InstanceType))

			_, err = client.DeleteFleet(ctx, &appstreamsdk.DeleteFleetInput{Name: aws.String(tt.fleetName)})
			require.NoError(t, err, "DeleteFleet should succeed")
		})
	}
}
