package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createLightsailClient returns a Lightsail client pointed at the shared
// test container.
func createLightsailClient(t *testing.T) *lightsailsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return lightsailsdk.NewFromConfig(cfg, func(o *lightsailsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Lightsail_InstanceLifecycle drives Lightsail's core VPS
// lifecycle (create -> read -> state transitions -> destroy) through the
// real aws-sdk-go-v2 client: CreateInstances, GetInstances, GetInstance,
// GetInstanceState, StopInstance, StartInstance, RebootInstance,
// DeleteInstance. None of these had ever been called from a typed client
// before (gopherstack-n3zi).
func TestIntegration_Lightsail_InstanceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		instanceName string
	}{
		{name: "full_lifecycle", instanceName: "integ-lightsail-instance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createLightsailClient(t)

			createOut, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
				InstanceNames:    []string{tt.instanceName},
				AvailabilityZone: aws.String("us-east-1a"),
				BlueprintId:      aws.String("amazon_linux_2023"),
				BundleId:         aws.String("nano_3_0"),
			})
			require.NoError(t, err, "CreateInstances should succeed")
			require.Len(t, createOut.Operations, 1)
			assert.Equal(t, tt.instanceName, aws.ToString(createOut.Operations[0].ResourceName))
			assert.Equal(t, types.OperationTypeCreateInstance, createOut.Operations[0].OperationType)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteInstance(cleanupCtx, &lightsailsdk.DeleteInstanceInput{
					InstanceName: aws.String(tt.instanceName),
				})
			})

			getOut, err := client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "GetInstance should succeed")
			require.NotNil(t, getOut.Instance)
			assert.Equal(t, tt.instanceName, aws.ToString(getOut.Instance.Name))
			assert.Equal(t, "amazon_linux_2023", aws.ToString(getOut.Instance.BlueprintId))
			assert.Equal(t, "nano_3_0", aws.ToString(getOut.Instance.BundleId))
			require.NotNil(t, getOut.Instance.Hardware,
				"Instance.Hardware must decode -- real CpuCount/RamSizeInGb live nested under it, not flat on Instance")
			assert.Equal(t, int32(1), aws.ToInt32(getOut.Instance.Hardware.CpuCount))
			assert.InDelta(t, float32(0.5), aws.ToFloat32(getOut.Instance.Hardware.RamSizeInGb), 0.001)
			require.NotNil(t, getOut.Instance.State)
			assert.NotEmpty(t, aws.ToString(getOut.Instance.State.Name))

			listOut, err := client.GetInstances(ctx, &lightsailsdk.GetInstancesInput{})
			require.NoError(t, err, "GetInstances should succeed")
			var found bool
			for _, inst := range listOut.Instances {
				if aws.ToString(inst.Name) == tt.instanceName {
					found = true
				}
			}
			assert.True(t, found, "GetInstances should list the created instance")

			require.Eventually(t, func() bool {
				stateOut, stateErr := client.GetInstanceState(ctx, &lightsailsdk.GetInstanceStateInput{
					InstanceName: aws.String(tt.instanceName),
				})

				return stateErr == nil && stateOut.State != nil && aws.ToString(stateOut.State.Name) == "running"
			}, 5*time.Second, 20*time.Millisecond, "instance should transition pending -> running")

			stopOut, err := client.StopInstance(ctx, &lightsailsdk.StopInstanceInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "StopInstance should succeed")
			require.Len(t, stopOut.Operations, 1)
			assert.Equal(t, types.OperationTypeStopInstance, stopOut.Operations[0].OperationType)

			stateOut, err := client.GetInstanceState(ctx, &lightsailsdk.GetInstanceStateInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "GetInstanceState should succeed")
			require.NotNil(t, stateOut.State)
			assert.Equal(t, "stopped", aws.ToString(stateOut.State.Name))

			startOut, err := client.StartInstance(ctx, &lightsailsdk.StartInstanceInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "StartInstance should succeed")
			require.Len(t, startOut.Operations, 1)
			assert.Equal(t, types.OperationTypeStartInstance, startOut.Operations[0].OperationType)

			stateOut, err = client.GetInstanceState(ctx, &lightsailsdk.GetInstanceStateInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "GetInstanceState should succeed")
			require.NotNil(t, stateOut.State)
			assert.Equal(t, "running", aws.ToString(stateOut.State.Name))

			rebootOut, err := client.RebootInstance(ctx, &lightsailsdk.RebootInstanceInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "RebootInstance should succeed")
			require.Len(t, rebootOut.Operations, 1)
			assert.Equal(t, types.OperationTypeRebootInstance, rebootOut.Operations[0].OperationType)

			deleteOut, err := client.DeleteInstance(ctx, &lightsailsdk.DeleteInstanceInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.NoError(t, err, "DeleteInstance should succeed")
			require.Len(t, deleteOut.Operations, 1)
			assert.Equal(t, tt.instanceName, aws.ToString(deleteOut.Operations[0].ResourceName))

			_, err = client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{
				InstanceName: aws.String(tt.instanceName),
			})
			require.Error(t, err, "GetInstance on a deleted instance should fail")

			var nf *types.NotFoundException
			require.ErrorAs(t, err, &nf, "expected a real NotFoundException from the SDK deserializer")
		})
	}
}
