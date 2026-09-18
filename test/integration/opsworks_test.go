package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	opsworkstypes "github.com/aws/aws-sdk-go-v2/service/opsworks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createOpsWorksClient returns an OpsWorks client pointed at the shared test container.
func createOpsWorksClient(t *testing.T) *opsworkssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return opsworkssdk.NewFromConfig(cfg, func(o *opsworkssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_OpsWorks_StackLayerInstanceLifecycle drives the standard
// OpsWorks Stacks workflow through the real AWS SDK v2 client: create a
// stack, layer, instance and app, verify each Describe echoes real state,
// update the stack, tear the resources down in dependency order (OpsWorks
// enforces delete preconditions -- instance/layer/app must be gone before
// their parent), and confirm the stack is really gone afterward.
func TestIntegration_OpsWorks_StackLayerInstanceLifecycle(t *testing.T) { //nolint:tparallel // sequential subtests
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createOpsWorksClient(t)
	ctx := t.Context()

	const (
		stackName          = "it-opsworks-stack"
		stackRegion        = "us-east-1"
		serviceRoleArn     = "arn:aws:iam::123456789012:role/aws-opsworks-service-role"
		instanceProfileArn = "arn:aws:iam::123456789012:instance-profile/aws-opsworks-ec2-role"
		updatedName        = "it-opsworks-stack-renamed"
		layerName          = "it-opsworks-layer"
		layerShortname     = "it-layer"
		instanceType       = "m5.large"
		appName            = "it-opsworks-app"
		appShortname       = "it-app"
	)

	t.Run("create_stack", func(t *testing.T) { //nolint:paralleltest // sequential by design
		createOut, err := client.CreateStack(ctx, &opsworkssdk.CreateStackInput{
			Name:                      aws.String(stackName),
			Region:                    aws.String(stackRegion),
			ServiceRoleArn:            aws.String(serviceRoleArn),
			DefaultInstanceProfileArn: aws.String(instanceProfileArn),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.StackId))
	})

	// Re-fetch the stack ID via ListStacks-equivalent (DescribeStacks with no
	// filter) since subtests each declare their own scope.
	descAllOut, descAllErr := client.DescribeStacks(ctx, &opsworkssdk.DescribeStacksInput{})
	require.NoError(t, descAllErr)

	var stackID string

	for _, s := range descAllOut.Stacks {
		if aws.ToString(s.Name) == stackName {
			stackID = aws.ToString(s.StackId)

			break
		}
	}

	require.NotEmpty(t, stackID, "created stack must appear in DescribeStacks")

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStack(cleanupCtx, &opsworkssdk.DeleteStackInput{StackId: aws.String(stackID)})
	})

	t.Run("describe_stack_fields", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.DescribeStacks(ctx, &opsworkssdk.DescribeStacksInput{
			StackIds: []string{stackID},
		})
		require.NoError(t, err)
		require.Len(t, out.Stacks, 1)

		s := out.Stacks[0]
		assert.Equal(t, stackName, aws.ToString(s.Name))
		assert.Equal(t, stackRegion, aws.ToString(s.Region))
		assert.Equal(t, serviceRoleArn, aws.ToString(s.ServiceRoleArn))
		assert.Equal(t, instanceProfileArn, aws.ToString(s.DefaultInstanceProfileArn))
	})

	var layerID string

	t.Run("create_layer", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.CreateLayer(ctx, &opsworkssdk.CreateLayerInput{
			StackId:   aws.String(stackID),
			Type:      opsworkstypes.LayerTypeCustom,
			Name:      aws.String(layerName),
			Shortname: aws.String(layerShortname),
		})
		require.NoError(t, err)
		layerID = aws.ToString(out.LayerId)
		require.NotEmpty(t, layerID)
	})

	t.Cleanup(func() {
		if layerID == "" {
			return
		}

		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteLayer(cleanupCtx, &opsworkssdk.DeleteLayerInput{LayerId: aws.String(layerID)})
	})

	t.Run("describe_layer_fields", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.DescribeLayers(ctx, &opsworkssdk.DescribeLayersInput{
			LayerIds: []string{layerID},
		})
		require.NoError(t, err)
		require.Len(t, out.Layers, 1)

		l := out.Layers[0]
		assert.Equal(t, layerName, aws.ToString(l.Name))
		assert.Equal(t, layerShortname, aws.ToString(l.Shortname))
		assert.Equal(t, stackID, aws.ToString(l.StackId))
		assert.Equal(t, opsworkstypes.LayerTypeCustom, l.Type)
	})

	var instanceID string

	t.Run("create_instance", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.CreateInstance(ctx, &opsworkssdk.CreateInstanceInput{
			StackId:      aws.String(stackID),
			LayerIds:     []string{layerID},
			InstanceType: aws.String(instanceType),
		})
		require.NoError(t, err)
		instanceID = aws.ToString(out.InstanceId)
		require.NotEmpty(t, instanceID)
	})

	t.Cleanup(func() {
		if instanceID == "" {
			return
		}

		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteInstance(cleanupCtx, &opsworkssdk.DeleteInstanceInput{InstanceId: aws.String(instanceID)})
	})

	t.Run("describe_instance_fields", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.DescribeInstances(ctx, &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		require.Len(t, out.Instances, 1)

		i := out.Instances[0]
		assert.Equal(t, stackID, aws.ToString(i.StackId))
		assert.Equal(t, instanceType, aws.ToString(i.InstanceType))
		assert.Contains(t, i.LayerIds, layerID)
	})

	var appID string

	t.Run("create_app", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.CreateApp(ctx, &opsworkssdk.CreateAppInput{
			StackId:   aws.String(stackID),
			Name:      aws.String(appName),
			Type:      opsworkstypes.AppTypeOther,
			Shortname: aws.String(appShortname),
		})
		require.NoError(t, err)
		appID = aws.ToString(out.AppId)
		require.NotEmpty(t, appID)
	})

	t.Cleanup(func() {
		if appID == "" {
			return
		}

		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApp(cleanupCtx, &opsworkssdk.DeleteAppInput{AppId: aws.String(appID)})
	})

	t.Run("describe_app_fields", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := client.DescribeApps(ctx, &opsworkssdk.DescribeAppsInput{
			AppIds: []string{appID},
		})
		require.NoError(t, err)
		require.Len(t, out.Apps, 1)

		a := out.Apps[0]
		assert.Equal(t, appName, aws.ToString(a.Name))
		assert.Equal(t, stackID, aws.ToString(a.StackId))
		assert.Equal(t, opsworkstypes.AppTypeOther, a.Type)
	})

	t.Run("update_stack", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := client.UpdateStack(ctx, &opsworkssdk.UpdateStackInput{
			StackId: aws.String(stackID),
			Name:    aws.String(updatedName),
		})
		require.NoError(t, err)

		out, err := client.DescribeStacks(ctx, &opsworkssdk.DescribeStacksInput{
			StackIds: []string{stackID},
		})
		require.NoError(t, err)
		require.Len(t, out.Stacks, 1)
		assert.Equal(t, updatedName, aws.ToString(out.Stacks[0].Name))
	})

	// Deletes run in dependency order: instance, then layer/app, then stack.
	// OpsWorks enforces delete preconditions on both stack and layer
	// (services/opsworks/PARITY.md: DeleteStack/DeleteLayer reject while
	// dependents remain), so a real client cannot delete the stack first.
	t.Run("delete_instance", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := client.DeleteInstance(ctx, &opsworkssdk.DeleteInstanceInput{InstanceId: aws.String(instanceID)})
		require.NoError(t, err)
		instanceID = ""
	})

	t.Run("delete_layer", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := client.DeleteLayer(ctx, &opsworkssdk.DeleteLayerInput{LayerId: aws.String(layerID)})
		require.NoError(t, err)
		layerID = ""
	})

	t.Run("delete_app", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := client.DeleteApp(ctx, &opsworkssdk.DeleteAppInput{AppId: aws.String(appID)})
		require.NoError(t, err)
		appID = ""
	})

	t.Run("delete_stack_then_not_found", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := client.DeleteStack(ctx, &opsworkssdk.DeleteStackInput{StackId: aws.String(stackID)})
		require.NoError(t, err)

		_, err = client.DescribeStacks(ctx, &opsworkssdk.DescribeStacksInput{
			StackIds: []string{stackID},
		})
		require.Error(t, err)

		var rnf *opsworkstypes.ResourceNotFoundException
		require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFoundException from the SDK deserializer")

		stackID = ""
	})
}
