package opsworks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/aws/aws-sdk-go-v2/service/opsworks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice13OpsworksRealClient drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage slice 13). Each subtest builds its own fresh client/backend and
// full precondition chain so subtests can run in parallel with each other.
func TestTypedSlice13OpsworksRealClient(t *testing.T) {
	t.Parallel()

	t.Run("stack_lifecycle_extras", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		src, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("source-stack"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)
		srcID := aws.ToString(src.StackId)

		cloned, err := client.CloneStack(t.Context(), &opsworkssdk.CloneStackInput{
			SourceStackId:  aws.String(srcID),
			ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/opsworks"),
			Name:           aws.String("cloned-stack"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(cloned.StackId))
		assert.NotEqual(t, srcID, aws.ToString(cloned.StackId))

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   aws.String(srcID),
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l"),
			Shortname: aws.String("l"),
		})
		require.NoError(t, err)

		inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
			StackId:      aws.String(srcID),
			LayerIds:     []string{aws.ToString(layer.LayerId)},
			InstanceType: aws.String("t2.micro"),
		})
		require.NoError(t, err)

		_, err = client.StartStack(t.Context(), &opsworkssdk.StartStackInput{StackId: aws.String(srcID)})
		require.NoError(t, err)

		described, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{aws.ToString(inst.InstanceId)},
		})
		require.NoError(t, err)
		require.Len(t, described.Instances, 1)
		assert.Equal(t, "online", aws.ToString(described.Instances[0].Status))

		_, err = client.StopStack(t.Context(), &opsworkssdk.StopStackInput{StackId: aws.String(srcID)})
		require.NoError(t, err)

		described, err = client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{aws.ToString(inst.InstanceId)},
		})
		require.NoError(t, err)
		require.Len(t, described.Instances, 1)
		assert.Equal(t, "stopped", aws.ToString(described.Instances[0].Status))

		summary, err := client.DescribeStackSummary(t.Context(), &opsworkssdk.DescribeStackSummaryInput{
			StackId: aws.String(srcID),
		})
		require.NoError(t, err)
		require.NotNil(t, summary.StackSummary)
		assert.Equal(t, srcID, aws.ToString(summary.StackSummary.StackId))
		assert.Equal(t, int32(1), aws.ToInt32(summary.StackSummary.LayersCount))
		require.NotNil(t, summary.StackSummary.InstancesCount)
		assert.Equal(t, int32(1), aws.ToInt32(summary.StackSummary.InstancesCount.Stopped))

		provisioning, err := client.DescribeStackProvisioningParameters(
			t.Context(),
			&opsworkssdk.DescribeStackProvisioningParametersInput{StackId: aws.String(srcID)},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(provisioning.AgentInstallerUrl))
	})

	t.Run("layer_update", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("original"),
			Shortname: aws.String("orig"),
		})
		require.NoError(t, err)

		_, err = client.UpdateLayer(t.Context(), &opsworkssdk.UpdateLayerInput{
			LayerId: layer.LayerId,
			Name:    aws.String("renamed"),
		})
		require.NoError(t, err)

		described, err := client.DescribeLayers(t.Context(), &opsworkssdk.DescribeLayersInput{
			LayerIds: []string{aws.ToString(layer.LayerId)},
		})
		require.NoError(t, err)
		require.Len(t, described.Layers, 1)
		assert.Equal(t, "renamed", aws.ToString(described.Layers[0].Name))
	})

	t.Run("instance_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer1, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l1"),
			Shortname: aws.String("l1"),
		})
		require.NoError(t, err)

		layer2, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l2"),
			Shortname: aws.String("l2"),
		})
		require.NoError(t, err)

		hostnameOut, err := client.GetHostnameSuggestion(t.Context(), &opsworkssdk.GetHostnameSuggestionInput{
			LayerId: layer1.LayerId,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(hostnameOut.Hostname))

		// CreateInstance's LayerIds is genuinely plural on the real wire
		// (api_op_CreateInstance.go: "An array that contains the instance's
		// layer IDs") -- assert both requested layers are actually stored,
		// not silently truncated to the first.
		inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
			StackId:      stack.StackId,
			LayerIds:     []string{aws.ToString(layer1.LayerId), aws.ToString(layer2.LayerId)},
			InstanceType: aws.String("t2.micro"),
		})
		require.NoError(t, err)
		instanceID := aws.ToString(inst.InstanceId)

		described, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		require.Len(t, described.Instances, 1)
		assert.ElementsMatch(
			t,
			[]string{aws.ToString(layer1.LayerId), aws.ToString(layer2.LayerId)},
			described.Instances[0].LayerIds,
		)

		_, err = client.UpdateInstance(t.Context(), &opsworkssdk.UpdateInstanceInput{
			InstanceId: aws.String(instanceID),
			Hostname:   aws.String("renamed-host"),
		})
		require.NoError(t, err)

		_, err = client.StartInstance(t.Context(), &opsworkssdk.StartInstanceInput{InstanceId: aws.String(instanceID)})
		require.NoError(t, err)

		described, err = client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		assert.Equal(t, "renamed-host", aws.ToString(described.Instances[0].Hostname))
		assert.Equal(t, "online", aws.ToString(described.Instances[0].Status))

		_, err = client.RebootInstance(
			t.Context(),
			&opsworkssdk.RebootInstanceInput{InstanceId: aws.String(instanceID)},
		)
		require.NoError(t, err)

		_, err = client.StopInstance(t.Context(), &opsworkssdk.StopInstanceInput{InstanceId: aws.String(instanceID)})
		require.NoError(t, err)

		described, err = client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		assert.Equal(t, "stopped", aws.ToString(described.Instances[0].Status))

		_, err = client.DeleteInstance(
			t.Context(),
			&opsworkssdk.DeleteInstanceInput{InstanceId: aws.String(instanceID)},
		)
		require.NoError(t, err)

		described, err = client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, described.Instances)
	})

	t.Run("instance_assign_register", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer1, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l1"),
			Shortname: aws.String("l1"),
		})
		require.NoError(t, err)

		layer2, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l2"),
			Shortname: aws.String("l2"),
		})
		require.NoError(t, err)

		registered, err := client.RegisterInstance(t.Context(), &opsworkssdk.RegisterInstanceInput{
			StackId:  stack.StackId,
			Hostname: aws.String("onprem"),
		})
		require.NoError(t, err)
		instanceID := aws.ToString(registered.InstanceId)

		// AssignInstance's LayerIds is also a plural real wire field --
		// assert both are actually recorded, not truncated to the first.
		_, err = client.AssignInstance(t.Context(), &opsworkssdk.AssignInstanceInput{
			InstanceId: aws.String(instanceID),
			LayerIds:   []string{aws.ToString(layer1.LayerId), aws.ToString(layer2.LayerId)},
		})
		require.NoError(t, err)

		described, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		require.Len(t, described.Instances, 1)
		assert.ElementsMatch(
			t,
			[]string{aws.ToString(layer1.LayerId), aws.ToString(layer2.LayerId)},
			described.Instances[0].LayerIds,
		)

		byLayer, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			LayerId: layer2.LayerId,
		})
		require.NoError(t, err)
		require.Len(t, byLayer.Instances, 1)
		assert.Equal(t, instanceID, aws.ToString(byLayer.Instances[0].InstanceId))

		_, err = client.UnassignInstance(t.Context(), &opsworkssdk.UnassignInstanceInput{
			InstanceId: aws.String(instanceID),
		})
		require.NoError(t, err)

		described, err = client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		assert.Empty(t, described.Instances[0].LayerIds)

		_, err = client.DeregisterInstance(t.Context(), &opsworkssdk.DeregisterInstanceInput{
			InstanceId: aws.String(instanceID),
		})
		require.NoError(t, err)

		described, err = client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, described.Instances)
	})

	t.Run("app_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		app, err := client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
			StackId: stack.StackId,
			Name:    aws.String("myapp"),
			Type:    types.AppTypeNodejs,
		})
		require.NoError(t, err)
		appID := aws.ToString(app.AppId)

		described, err := client.DescribeApps(t.Context(), &opsworkssdk.DescribeAppsInput{AppIds: []string{appID}})
		require.NoError(t, err)
		require.Len(t, described.Apps, 1)
		assert.Equal(t, "myapp", aws.ToString(described.Apps[0].Name))
		assert.Equal(t, types.AppTypeNodejs, described.Apps[0].Type)

		_, err = client.UpdateApp(t.Context(), &opsworkssdk.UpdateAppInput{
			AppId: aws.String(appID),
			Name:  aws.String("renamed-app"),
		})
		require.NoError(t, err)

		described, err = client.DescribeApps(t.Context(), &opsworkssdk.DescribeAppsInput{AppIds: []string{appID}})
		require.NoError(t, err)
		assert.Equal(t, "renamed-app", aws.ToString(described.Apps[0].Name))

		_, err = client.DeleteApp(t.Context(), &opsworkssdk.DeleteAppInput{AppId: aws.String(appID)})
		require.NoError(t, err)

		described, err = client.DescribeApps(t.Context(), &opsworkssdk.DescribeAppsInput{StackId: stack.StackId})
		require.NoError(t, err)
		assert.Empty(t, described.Apps)
	})

	t.Run("deployment_commands", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		app, err := client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
			StackId: stack.StackId,
			Name:    aws.String("myapp"),
			Type:    types.AppTypeOther,
		})
		require.NoError(t, err)

		deployment, err := client.CreateDeployment(t.Context(), &opsworkssdk.CreateDeploymentInput{
			StackId: stack.StackId,
			AppId:   app.AppId,
			Command: &types.DeploymentCommand{Name: types.DeploymentCommandNameDeploy},
		})
		require.NoError(t, err)
		deploymentID := aws.ToString(deployment.DeploymentId)

		described, err := client.DescribeDeployments(t.Context(), &opsworkssdk.DescribeDeploymentsInput{
			DeploymentIds: []string{deploymentID},
		})
		require.NoError(t, err)
		require.Len(t, described.Deployments, 1)
		assert.Equal(t, aws.ToString(app.AppId), aws.ToString(described.Deployments[0].AppId))
		assert.Equal(t, "successful", aws.ToString(described.Deployments[0].Status))

		commands, err := client.DescribeCommands(t.Context(), &opsworkssdk.DescribeCommandsInput{
			DeploymentId: aws.String(deploymentID),
		})
		require.NoError(t, err)
		require.Len(t, commands.Commands, 1)
		assert.Equal(t, deploymentID, aws.ToString(commands.Commands[0].DeploymentId))
		assert.Equal(t, "successful", aws.ToString(commands.Commands[0].Status))
	})

	t.Run("elastic_ip", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l"),
			Shortname: aws.String("l"),
		})
		require.NoError(t, err)

		inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
			StackId:      stack.StackId,
			LayerIds:     []string{aws.ToString(layer.LayerId)},
			InstanceType: aws.String("t2.micro"),
		})
		require.NoError(t, err)

		const eip = "203.0.113.9"

		_, err = client.RegisterElasticIp(t.Context(), &opsworkssdk.RegisterElasticIpInput{
			ElasticIp: aws.String(eip),
			StackId:   stack.StackId,
		})
		require.NoError(t, err)

		_, err = client.AssociateElasticIp(t.Context(), &opsworkssdk.AssociateElasticIpInput{
			ElasticIp:  aws.String(eip),
			InstanceId: inst.InstanceId,
		})
		require.NoError(t, err)

		described, err := client.DescribeElasticIps(t.Context(), &opsworkssdk.DescribeElasticIpsInput{
			Ips: []string{eip},
		})
		require.NoError(t, err)
		require.Len(t, described.ElasticIps, 1)
		assert.Equal(t, aws.ToString(inst.InstanceId), aws.ToString(described.ElasticIps[0].InstanceId))

		_, err = client.UpdateElasticIp(t.Context(), &opsworkssdk.UpdateElasticIpInput{
			ElasticIp: aws.String(eip),
			Name:      aws.String("front-door"),
		})
		require.NoError(t, err)

		described, err = client.DescribeElasticIps(t.Context(), &opsworkssdk.DescribeElasticIpsInput{
			Ips: []string{eip},
		})
		require.NoError(t, err)
		assert.Equal(t, "front-door", aws.ToString(described.ElasticIps[0].Name))

		_, err = client.DisassociateElasticIp(t.Context(), &opsworkssdk.DisassociateElasticIpInput{
			ElasticIp: aws.String(eip),
		})
		require.NoError(t, err)

		described, err = client.DescribeElasticIps(t.Context(), &opsworkssdk.DescribeElasticIpsInput{
			Ips: []string{eip},
		})
		require.NoError(t, err)
		assert.Empty(t, aws.ToString(described.ElasticIps[0].InstanceId))

		_, err = client.DeregisterElasticIp(t.Context(), &opsworkssdk.DeregisterElasticIpInput{
			ElasticIp: aws.String(eip),
		})
		require.NoError(t, err)

		described, err = client.DescribeElasticIps(t.Context(), &opsworkssdk.DescribeElasticIpsInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, described.ElasticIps)
	})

	t.Run("elb", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l"),
			Shortname: aws.String("l"),
		})
		require.NoError(t, err)

		_, err = client.AttachElasticLoadBalancer(t.Context(), &opsworkssdk.AttachElasticLoadBalancerInput{
			ElasticLoadBalancerName: aws.String("my-elb"),
			LayerId:                 layer.LayerId,
		})
		require.NoError(t, err)

		described, err := client.DescribeElasticLoadBalancers(
			t.Context(),
			&opsworkssdk.DescribeElasticLoadBalancersInput{LayerIds: []string{aws.ToString(layer.LayerId)}},
		)
		require.NoError(t, err)
		require.Len(t, described.ElasticLoadBalancers, 1)
		assert.Equal(t, "my-elb", aws.ToString(described.ElasticLoadBalancers[0].ElasticLoadBalancerName))
		assert.NotEmpty(t, aws.ToString(described.ElasticLoadBalancers[0].DnsName))

		_, err = client.DetachElasticLoadBalancer(t.Context(), &opsworkssdk.DetachElasticLoadBalancerInput{
			ElasticLoadBalancerName: aws.String("my-elb"),
			LayerId:                 layer.LayerId,
		})
		require.NoError(t, err)

		described, err = client.DescribeElasticLoadBalancers(
			t.Context(),
			&opsworkssdk.DescribeElasticLoadBalancersInput{StackId: stack.StackId},
		)
		require.NoError(t, err)
		assert.Empty(t, described.ElasticLoadBalancers)
	})

	t.Run("volume", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l"),
			Shortname: aws.String("l"),
		})
		require.NoError(t, err)

		inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
			StackId:      stack.StackId,
			LayerIds:     []string{aws.ToString(layer.LayerId)},
			InstanceType: aws.String("t2.micro"),
		})
		require.NoError(t, err)

		registered, err := client.RegisterVolume(t.Context(), &opsworkssdk.RegisterVolumeInput{
			StackId:     stack.StackId,
			Ec2VolumeId: aws.String("vol-0abc123"),
		})
		require.NoError(t, err)
		volumeID := aws.ToString(registered.VolumeId)

		_, err = client.AssignVolume(t.Context(), &opsworkssdk.AssignVolumeInput{
			VolumeId:   aws.String(volumeID),
			InstanceId: inst.InstanceId,
		})
		require.NoError(t, err)

		described, err := client.DescribeVolumes(t.Context(), &opsworkssdk.DescribeVolumesInput{
			VolumeIds: []string{volumeID},
		})
		require.NoError(t, err)
		require.Len(t, described.Volumes, 1)
		assert.Equal(t, aws.ToString(inst.InstanceId), aws.ToString(described.Volumes[0].InstanceId))

		_, err = client.UpdateVolume(t.Context(), &opsworkssdk.UpdateVolumeInput{
			VolumeId:   aws.String(volumeID),
			Name:       aws.String("data-volume"),
			MountPoint: aws.String("/data"),
		})
		require.NoError(t, err)

		described, err = client.DescribeVolumes(t.Context(), &opsworkssdk.DescribeVolumesInput{
			VolumeIds: []string{volumeID},
		})
		require.NoError(t, err)
		assert.Equal(t, "data-volume", aws.ToString(described.Volumes[0].Name))
		assert.Equal(t, "/data", aws.ToString(described.Volumes[0].MountPoint))

		_, err = client.UnassignVolume(t.Context(), &opsworkssdk.UnassignVolumeInput{
			VolumeId: aws.String(volumeID),
		})
		require.NoError(t, err)

		described, err = client.DescribeVolumes(t.Context(), &opsworkssdk.DescribeVolumesInput{
			VolumeIds: []string{volumeID},
		})
		require.NoError(t, err)
		assert.Empty(t, aws.ToString(described.Volumes[0].InstanceId))

		_, err = client.DeregisterVolume(t.Context(), &opsworkssdk.DeregisterVolumeInput{
			VolumeId: aws.String(volumeID),
		})
		require.NoError(t, err)

		described, err = client.DescribeVolumes(t.Context(), &opsworkssdk.DescribeVolumesInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, described.Volumes)
	})

	t.Run("rds_db_instance", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		const rdsArn = "arn:aws:rds:us-east-1:000000000000:db:mydb"

		_, err = client.RegisterRdsDbInstance(t.Context(), &opsworkssdk.RegisterRdsDbInstanceInput{
			StackId:          stack.StackId,
			RdsDbInstanceArn: aws.String(rdsArn),
			DbUser:           aws.String("admin"),
			DbPassword:       aws.String("hunter2"),
		})
		require.NoError(t, err)

		described, err := client.DescribeRdsDbInstances(t.Context(), &opsworkssdk.DescribeRdsDbInstancesInput{
			StackId:           stack.StackId,
			RdsDbInstanceArns: []string{rdsArn},
		})
		require.NoError(t, err)
		require.Len(t, described.RdsDbInstances, 1)
		assert.Equal(t, "admin", aws.ToString(described.RdsDbInstances[0].DbUser))

		_, err = client.UpdateRdsDbInstance(t.Context(), &opsworkssdk.UpdateRdsDbInstanceInput{
			RdsDbInstanceArn: aws.String(rdsArn),
			DbUser:           aws.String("root"),
		})
		require.NoError(t, err)

		described, err = client.DescribeRdsDbInstances(t.Context(), &opsworkssdk.DescribeRdsDbInstancesInput{
			StackId:           stack.StackId,
			RdsDbInstanceArns: []string{rdsArn},
		})
		require.NoError(t, err)
		assert.Equal(t, "root", aws.ToString(described.RdsDbInstances[0].DbUser))

		_, err = client.DeregisterRdsDbInstance(t.Context(), &opsworkssdk.DeregisterRdsDbInstanceInput{
			RdsDbInstanceArn: aws.String(rdsArn),
		})
		require.NoError(t, err)

		described, err = client.DescribeRdsDbInstances(t.Context(), &opsworkssdk.DescribeRdsDbInstancesInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, described.RdsDbInstances)
	})

	t.Run("ecs_cluster_deregister", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		const ecsArn = "arn:aws:ecs:us-east-1:000000000000:cluster/mycluster"

		_, err = client.RegisterEcsCluster(t.Context(), &opsworkssdk.RegisterEcsClusterInput{
			StackId:       stack.StackId,
			EcsClusterArn: aws.String(ecsArn),
		})
		require.NoError(t, err)

		_, err = client.DeregisterEcsCluster(t.Context(), &opsworkssdk.DeregisterEcsClusterInput{
			EcsClusterArn: aws.String(ecsArn),
		})
		require.NoError(t, err)

		described, err := client.DescribeEcsClusters(t.Context(), &opsworkssdk.DescribeEcsClustersInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, described.EcsClusters)
	})

	t.Run("user_profile", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		const iamArn = "arn:aws:iam::000000000000:user/alice"

		created, err := client.CreateUserProfile(t.Context(), &opsworkssdk.CreateUserProfileInput{
			IamUserArn:  aws.String(iamArn),
			SshUsername: aws.String("alice"),
		})
		require.NoError(t, err)
		assert.Equal(t, iamArn, aws.ToString(created.IamUserArn))

		described, err := client.DescribeUserProfiles(t.Context(), &opsworkssdk.DescribeUserProfilesInput{
			IamUserArns: []string{iamArn},
		})
		require.NoError(t, err)
		require.Len(t, described.UserProfiles, 1)
		assert.Equal(t, "alice", aws.ToString(described.UserProfiles[0].SshUsername))

		_, err = client.UpdateUserProfile(t.Context(), &opsworkssdk.UpdateUserProfileInput{
			IamUserArn:  aws.String(iamArn),
			SshUsername: aws.String("alice2"),
		})
		require.NoError(t, err)

		described, err = client.DescribeUserProfiles(t.Context(), &opsworkssdk.DescribeUserProfilesInput{
			IamUserArns: []string{iamArn},
		})
		require.NoError(t, err)
		assert.Equal(t, "alice2", aws.ToString(described.UserProfiles[0].SshUsername))

		mine, err := client.DescribeMyUserProfile(t.Context(), &opsworkssdk.DescribeMyUserProfileInput{})
		require.NoError(t, err)
		require.NotNil(t, mine.UserProfile)
		assert.Empty(t, aws.ToString(mine.UserProfile.SshPublicKey))

		_, err = client.UpdateMyUserProfile(t.Context(), &opsworkssdk.UpdateMyUserProfileInput{
			SshPublicKey: aws.String("ssh-rsa AAAAB3NzaC1yc2E"),
		})
		require.NoError(t, err)

		mine, err = client.DescribeMyUserProfile(t.Context(), &opsworkssdk.DescribeMyUserProfileInput{})
		require.NoError(t, err)
		assert.Equal(t, "ssh-rsa AAAAB3NzaC1yc2E", aws.ToString(mine.UserProfile.SshPublicKey))

		_, err = client.DeleteUserProfile(t.Context(), &opsworkssdk.DeleteUserProfileInput{
			IamUserArn: aws.String(iamArn),
		})
		require.NoError(t, err)

		described, err = client.DescribeUserProfiles(t.Context(), &opsworkssdk.DescribeUserProfilesInput{})
		require.NoError(t, err)
		assert.Empty(t, described.UserProfiles)
	})

	t.Run("permission", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l"),
			Shortname: aws.String("l"),
		})
		require.NoError(t, err)

		inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
			StackId:      stack.StackId,
			LayerIds:     []string{aws.ToString(layer.LayerId)},
			InstanceType: aws.String("t2.micro"),
		})
		require.NoError(t, err)

		const iamArn = "arn:aws:iam::000000000000:user/bob"

		_, err = client.SetPermission(t.Context(), &opsworkssdk.SetPermissionInput{
			StackId:    stack.StackId,
			IamUserArn: aws.String(iamArn),
			Level:      aws.String("deploy"),
			AllowSsh:   aws.Bool(true),
		})
		require.NoError(t, err)

		described, err := client.DescribePermissions(t.Context(), &opsworkssdk.DescribePermissionsInput{
			StackId:    stack.StackId,
			IamUserArn: aws.String(iamArn),
		})
		require.NoError(t, err)
		require.Len(t, described.Permissions, 1)
		assert.Equal(t, "deploy", aws.ToString(described.Permissions[0].Level))
		assert.True(t, aws.ToBool(described.Permissions[0].AllowSsh))

		granted, err := client.GrantAccess(t.Context(), &opsworkssdk.GrantAccessInput{
			InstanceId: inst.InstanceId,
		})
		require.NoError(t, err)
		require.NotNil(t, granted.TemporaryCredential)
		assert.Equal(t, aws.ToString(inst.InstanceId), aws.ToString(granted.TemporaryCredential.InstanceId))
		assert.NotEmpty(t, aws.ToString(granted.TemporaryCredential.Password))
	})

	t.Run("autoscaling", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   stack.StackId,
			Type:      types.LayerTypeCustom,
			Name:      aws.String("l"),
			Shortname: aws.String("l"),
		})
		require.NoError(t, err)

		inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
			StackId:      stack.StackId,
			LayerIds:     []string{aws.ToString(layer.LayerId)},
			InstanceType: aws.String("t2.micro"),
		})
		require.NoError(t, err)

		_, err = client.SetTimeBasedAutoScaling(t.Context(), &opsworkssdk.SetTimeBasedAutoScalingInput{
			InstanceId: inst.InstanceId,
			AutoScalingSchedule: &types.WeeklyAutoScalingSchedule{
				Monday: map[string]string{"00": "on"},
			},
		})
		require.NoError(t, err)

		described, err := client.DescribeTimeBasedAutoScaling(
			t.Context(),
			&opsworkssdk.DescribeTimeBasedAutoScalingInput{InstanceIds: []string{aws.ToString(inst.InstanceId)}},
		)
		require.NoError(t, err)
		require.Len(t, described.TimeBasedAutoScalingConfigurations, 1)
		require.NotNil(t, described.TimeBasedAutoScalingConfigurations[0].AutoScalingSchedule)
		assert.Equal(t, "on", described.TimeBasedAutoScalingConfigurations[0].AutoScalingSchedule.Monday["00"])

		_, err = client.SetLoadBasedAutoScaling(t.Context(), &opsworkssdk.SetLoadBasedAutoScalingInput{
			LayerId: layer.LayerId,
			Enable:  aws.Bool(true),
			UpScaling: &types.AutoScalingThresholds{
				CpuThreshold: aws.Float64(80),
			},
		})
		require.NoError(t, err)

		lbDescribed, err := client.DescribeLoadBasedAutoScaling(
			t.Context(),
			&opsworkssdk.DescribeLoadBasedAutoScalingInput{LayerIds: []string{aws.ToString(layer.LayerId)}},
		)
		require.NoError(t, err)
		require.Len(t, lbDescribed.LoadBasedAutoScalingConfigurations, 1)
		assert.True(t, aws.ToBool(lbDescribed.LoadBasedAutoScalingConfigurations[0].Enable))
		require.NotNil(t, lbDescribed.LoadBasedAutoScalingConfigurations[0].UpScaling)
		assert.InDelta(
			t, 80,
			aws.ToFloat64(lbDescribed.LoadBasedAutoScalingConfigurations[0].UpScaling.CpuThreshold),
			0.001,
		)
	})

	t.Run("misc_static", func(t *testing.T) {
		t.Parallel()

		client := newTestClient(t)

		stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("s"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)

		errsOut, err := client.DescribeServiceErrors(t.Context(), &opsworkssdk.DescribeServiceErrorsInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, errsOut.ServiceErrors)

		raidOut, err := client.DescribeRaidArrays(t.Context(), &opsworkssdk.DescribeRaidArraysInput{
			StackId: stack.StackId,
		})
		require.NoError(t, err)
		assert.Empty(t, raidOut.RaidArrays)

		osOut, err := client.DescribeOperatingSystems(t.Context(), &opsworkssdk.DescribeOperatingSystemsInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, osOut.OperatingSystems)
	})
}
