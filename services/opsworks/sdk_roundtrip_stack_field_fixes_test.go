package opsworks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/aws/aws-sdk-go-v2/service/opsworks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testInstanceProfileArn = "arn:aws:iam::000000000000:instance-profile/opsworks"
	testServiceRoleArn     = "arn:aws:iam::000000000000:role/opsworks"
)

// TestRealClient_StackAttributes drives CreateStack/UpdateStack
// through the real SDK client, asserting every tier-1 reqfielddiff finding
// on those two ops (gopherstack-xhu2t) is now stored and described
// back by DescribeStacks.
func TestRealClient_StackAttributes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_stack_describes_back_optional_attributes",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)

				created, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
					Name:                      aws.String("s"),
					Region:                    aws.String(rtTestRegion),
					DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
					ServiceRoleArn:            aws.String(testServiceRoleArn),
					AgentVersion:              aws.String("LATEST"),
					CustomJson:                aws.String(`{"key":"value"}`),
					DefaultAvailabilityZone:   aws.String("us-east-1a"),
					DefaultOs:                 aws.String("Amazon Linux 2"),
					DefaultRootDeviceType:     types.RootDeviceTypeEbs,
					DefaultSshKeyName:         aws.String("my-key"),
					DefaultSubnetId:           aws.String("subnet-abc"),
					HostnameTheme:             aws.String("Legendary_creatures_from_Japan"),
					UseOpsworksSecurityGroups: aws.Bool(false),
				})
				require.NoError(t, err)

				described, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
					StackIds: []string{aws.ToString(created.StackId)},
				})
				require.NoError(t, err)
				require.Len(t, described.Stacks, 1)

				got := described.Stacks[0]
				assert.Equal(t, "LATEST", aws.ToString(got.AgentVersion))
				assert.JSONEq(t, `{"key":"value"}`, aws.ToString(got.CustomJson))
				assert.Equal(t, "us-east-1a", aws.ToString(got.DefaultAvailabilityZone))
				assert.Equal(t, "Amazon Linux 2", aws.ToString(got.DefaultOs))
				assert.Equal(t, types.RootDeviceTypeEbs, got.DefaultRootDeviceType)
				assert.Equal(t, "my-key", aws.ToString(got.DefaultSshKeyName))
				assert.Equal(t, "subnet-abc", aws.ToString(got.DefaultSubnetId))
				assert.Equal(t, "Legendary_creatures_from_Japan", aws.ToString(got.HostnameTheme))
				require.NotNil(t, got.UseOpsworksSecurityGroups)
				assert.False(t, aws.ToBool(got.UseOpsworksSecurityGroups))
			},
		},
		{
			name: "update_stack_applies_optional_attributes",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)

				created, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
					Name:                      aws.String("s"),
					Region:                    aws.String(rtTestRegion),
					DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
					ServiceRoleArn:            aws.String(testServiceRoleArn),
				})
				require.NoError(t, err)
				stackID := aws.ToString(created.StackId)

				_, err = client.UpdateStack(t.Context(), &opsworkssdk.UpdateStackInput{
					StackId:      aws.String(stackID),
					AgentVersion: aws.String("12.14"),
					ConfigurationManager: &types.StackConfigurationManager{
						Name: aws.String("Chef"), Version: aws.String("12"),
					},
					CustomJson:                aws.String(`{"a":"b"}`),
					DefaultAvailabilityZone:   aws.String("us-east-1b"),
					DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/updated"),
					DefaultOs:                 aws.String("Ubuntu 18.04 LTS"),
					DefaultRootDeviceType:     types.RootDeviceTypeInstanceStore,
					DefaultSshKeyName:         aws.String("updated-key"),
					DefaultSubnetId:           aws.String("subnet-def"),
					HostnameTheme:             aws.String("Wild_Cats"),
					ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/updated"),
					UseOpsworksSecurityGroups: aws.Bool(true),
				})
				require.NoError(t, err)

				described, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
					StackIds: []string{stackID},
				})
				require.NoError(t, err)
				require.Len(t, described.Stacks, 1)

				got := described.Stacks[0]
				assert.Equal(t, "12.14", aws.ToString(got.AgentVersion))
				require.NotNil(t, got.ConfigurationManager)
				assert.Equal(t, "Chef", aws.ToString(got.ConfigurationManager.Name))
				assert.JSONEq(t, `{"a":"b"}`, aws.ToString(got.CustomJson))
				assert.Equal(t, "us-east-1b", aws.ToString(got.DefaultAvailabilityZone))
				assert.Equal(t,
					"arn:aws:iam::000000000000:instance-profile/updated", aws.ToString(got.DefaultInstanceProfileArn),
				)
				assert.Equal(t, "Ubuntu 18.04 LTS", aws.ToString(got.DefaultOs))
				assert.Equal(t, types.RootDeviceTypeInstanceStore, got.DefaultRootDeviceType)
				assert.Equal(t, "updated-key", aws.ToString(got.DefaultSshKeyName))
				assert.Equal(t, "subnet-def", aws.ToString(got.DefaultSubnetId))
				assert.Equal(t, "Wild_Cats", aws.ToString(got.HostnameTheme))
				assert.Equal(t, "arn:aws:iam::000000000000:role/updated", aws.ToString(got.ServiceRoleArn))
				require.NotNil(t, got.UseOpsworksSecurityGroups)
				assert.True(t, aws.ToBool(got.UseOpsworksSecurityGroups))
			},
		},
		{
			name: "update_stack_zero_values_leave_attributes_unchanged",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)

				created, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
					Name:                      aws.String("s"),
					Region:                    aws.String(rtTestRegion),
					DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
					ServiceRoleArn:            aws.String(testServiceRoleArn),
					DefaultOs:                 aws.String("Amazon Linux 2"),
				})
				require.NoError(t, err)
				stackID := aws.ToString(created.StackId)

				_, err = client.UpdateStack(t.Context(), &opsworkssdk.UpdateStackInput{
					StackId: aws.String(stackID),
					Name:    aws.String("renamed"),
				})
				require.NoError(t, err)

				described, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
					StackIds: []string{stackID},
				})
				require.NoError(t, err)
				require.Len(t, described.Stacks, 1)
				assert.Equal(t, "Amazon Linux 2", aws.ToString(described.Stacks[0].DefaultOs))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_CloneStack drives CloneStack through the real
// SDK client: ServiceRoleArn is required (unlike the other overrides) and
// every other attribute either takes an explicit override or inherits the
// source stack's value when omitted (both confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_CloneStack.go doc
// comments).
func TestRealClient_CloneStack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "missing_required_service_role_arn_is_rejected",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)

				src, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
					Name:                      aws.String("src"),
					Region:                    aws.String(rtTestRegion),
					DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
					ServiceRoleArn:            aws.String(testServiceRoleArn),
				})
				require.NoError(t, err)

				_, err = client.CloneStack(t.Context(), &opsworkssdk.CloneStackInput{
					SourceStackId: src.StackId,
					Name:          aws.String("clone"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "explicit_overrides_win_over_source",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)

				src, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
					Name:                      aws.String("src"),
					Region:                    aws.String(rtTestRegion),
					DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
					ServiceRoleArn:            aws.String(testServiceRoleArn),
					VpcId:                     aws.String("vpc-src"),
					AgentVersion:              aws.String("LATEST"),
					DefaultOs:                 aws.String("Amazon Linux 2"),
				})
				require.NoError(t, err)

				cloned, err := client.CloneStack(t.Context(), &opsworkssdk.CloneStackInput{
					SourceStackId:             src.StackId,
					Name:                      aws.String("clone"),
					ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/clone-role"),
					DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/clone"),
					VpcId:                     aws.String("vpc-clone"),
					AgentVersion:              aws.String("12.14"),
					DefaultOs:                 aws.String("Ubuntu 18.04 LTS"),
					UseOpsworksSecurityGroups: aws.Bool(false),
					HostnameTheme:             aws.String("Fruits"),
				})
				require.NoError(t, err)

				described, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
					StackIds: []string{aws.ToString(cloned.StackId)},
				})
				require.NoError(t, err)
				require.Len(t, described.Stacks, 1)

				got := described.Stacks[0]
				assert.Equal(t, "arn:aws:iam::000000000000:role/clone-role", aws.ToString(got.ServiceRoleArn))
				assert.Equal(
					t,
					"arn:aws:iam::000000000000:instance-profile/clone",
					aws.ToString(got.DefaultInstanceProfileArn),
				)
				assert.Equal(t, "vpc-clone", aws.ToString(got.VpcId))
				assert.Equal(t, "12.14", aws.ToString(got.AgentVersion))
				assert.Equal(t, "Ubuntu 18.04 LTS", aws.ToString(got.DefaultOs))
				require.NotNil(t, got.UseOpsworksSecurityGroups)
				assert.False(t, aws.ToBool(got.UseOpsworksSecurityGroups))
				assert.Equal(t, "Fruits", aws.ToString(got.HostnameTheme))
			},
		},
		{
			name: "omitted_attributes_inherit_from_source",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)

				src, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
					Name:                      aws.String("src"),
					Region:                    aws.String(rtTestRegion),
					DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
					ServiceRoleArn:            aws.String(testServiceRoleArn),
					VpcId:                     aws.String("vpc-src"),
					AgentVersion:              aws.String("LATEST"),
					DefaultOs:                 aws.String("Amazon Linux 2"),
					DefaultAvailabilityZone:   aws.String("us-east-1a"),
					DefaultSshKeyName:         aws.String("src-key"),
					DefaultSubnetId:           aws.String("subnet-src"),
					HostnameTheme:             aws.String("Planets_and_Moons"),
					CustomJson:                aws.String(`{"src":"true"}`),
				})
				require.NoError(t, err)

				cloned, err := client.CloneStack(t.Context(), &opsworkssdk.CloneStackInput{
					SourceStackId:  src.StackId,
					Name:           aws.String("clone"),
					ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/clone-role"),
				})
				require.NoError(t, err)

				described, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
					StackIds: []string{aws.ToString(cloned.StackId)},
				})
				require.NoError(t, err)
				require.Len(t, described.Stacks, 1)

				got := described.Stacks[0]
				assert.Equal(t, testInstanceProfileArn, aws.ToString(got.DefaultInstanceProfileArn))
				assert.Equal(t, "vpc-src", aws.ToString(got.VpcId))
				assert.Equal(t, "LATEST", aws.ToString(got.AgentVersion))
				assert.Equal(t, "Amazon Linux 2", aws.ToString(got.DefaultOs))
				assert.Equal(t, "us-east-1a", aws.ToString(got.DefaultAvailabilityZone))
				assert.Equal(t, "src-key", aws.ToString(got.DefaultSshKeyName))
				assert.Equal(t, "subnet-src", aws.ToString(got.DefaultSubnetId))
				assert.Equal(t, "Planets_and_Moons", aws.ToString(got.HostnameTheme))
				assert.JSONEq(t, `{"src":"true"}`, aws.ToString(got.CustomJson))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_InstanceAttributes drives CreateInstance and
// UpdateInstance's tier-1 optional-surface findings through the real SDK
// client.
func TestRealClient_InstanceAttributes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_instance_describes_back_optional_attributes",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)
				stackID, layerID := newTestStackWithLayer(t, client)

				created, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
					StackId:              aws.String(stackID),
					LayerIds:             []string{layerID},
					InstanceType:         aws.String("t2.micro"),
					AgentVersion:         aws.String("LATEST"),
					Architecture:         types.ArchitectureX8664,
					InstallUpdatesOnBoot: aws.Bool(true),
					Os:                   aws.String("Amazon Linux 2"),
					SubnetId:             aws.String("subnet-abc"),
					Tenancy:              aws.String("dedicated"),
				})
				require.NoError(t, err)

				described, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
					InstanceIds: []string{aws.ToString(created.InstanceId)},
				})
				require.NoError(t, err)
				require.Len(t, described.Instances, 1)

				got := described.Instances[0]
				assert.Equal(t, "LATEST", aws.ToString(got.AgentVersion))
				assert.Equal(t, types.ArchitectureX8664, got.Architecture)
				require.NotNil(t, got.InstallUpdatesOnBoot)
				assert.True(t, aws.ToBool(got.InstallUpdatesOnBoot))
				assert.Equal(t, "Amazon Linux 2", aws.ToString(got.Os))
				assert.Equal(t, "subnet-abc", aws.ToString(got.SubnetId))
				assert.Equal(t, "dedicated", aws.ToString(got.Tenancy))
			},
		},
		{
			name: "update_instance_applies_optional_attributes",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)
				stackID, layerID := newTestStackWithLayer(t, client)

				created, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
					StackId:      aws.String(stackID),
					LayerIds:     []string{layerID},
					InstanceType: aws.String("t2.micro"),
				})
				require.NoError(t, err)
				instanceID := aws.ToString(created.InstanceId)

				_, err = client.UpdateInstance(t.Context(), &opsworkssdk.UpdateInstanceInput{
					InstanceId:           aws.String(instanceID),
					AgentVersion:         aws.String("12.14"),
					InstallUpdatesOnBoot: aws.Bool(true),
					Os:                   aws.String("Ubuntu 18.04 LTS"),
				})
				require.NoError(t, err)

				described, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
					InstanceIds: []string{instanceID},
				})
				require.NoError(t, err)
				require.Len(t, described.Instances, 1)

				got := described.Instances[0]
				assert.Equal(t, "12.14", aws.ToString(got.AgentVersion))
				require.NotNil(t, got.InstallUpdatesOnBoot)
				assert.True(t, aws.ToBool(got.InstallUpdatesOnBoot))
				assert.Equal(t, "Ubuntu 18.04 LTS", aws.ToString(got.Os))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_LayerAttributes drives CreateLayer and
// UpdateLayer's tier-1 InstallUpdatesOnBoot finding through the real SDK
// client.
func TestRealClient_LayerAttributes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_layer_describes_back_install_updates_on_boot",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)
				stackID := newTestStack(t, client)

				created, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
					StackId:              aws.String(stackID),
					Type:                 types.LayerTypeCustom,
					Name:                 aws.String("l"),
					Shortname:            aws.String("l"),
					InstallUpdatesOnBoot: aws.Bool(false),
				})
				require.NoError(t, err)

				described, err := client.DescribeLayers(t.Context(), &opsworkssdk.DescribeLayersInput{
					LayerIds: []string{aws.ToString(created.LayerId)},
				})
				require.NoError(t, err)
				require.Len(t, described.Layers, 1)
				require.NotNil(t, described.Layers[0].InstallUpdatesOnBoot)
				assert.False(t, aws.ToBool(described.Layers[0].InstallUpdatesOnBoot))
			},
		},
		{
			name: "update_layer_applies_install_updates_on_boot",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestClient(t)
				stackID := newTestStack(t, client)

				created, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
					StackId:   aws.String(stackID),
					Type:      types.LayerTypeCustom,
					Name:      aws.String("l"),
					Shortname: aws.String("l"),
				})
				require.NoError(t, err)

				_, err = client.UpdateLayer(t.Context(), &opsworkssdk.UpdateLayerInput{
					LayerId:              created.LayerId,
					InstallUpdatesOnBoot: aws.Bool(true),
				})
				require.NoError(t, err)

				described, err := client.DescribeLayers(t.Context(), &opsworkssdk.DescribeLayersInput{
					LayerIds: []string{aws.ToString(created.LayerId)},
				})
				require.NoError(t, err)
				require.Len(t, described.Layers, 1)
				require.NotNil(t, described.Layers[0].InstallUpdatesOnBoot)
				assert.True(t, aws.ToBool(described.Layers[0].InstallUpdatesOnBoot))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_DeploymentCustomJSON drives CreateDeployment's
// tier-1 CustomJson finding through the real SDK client.
func TestRealClient_DeploymentCustomJSON(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID := newTestStack(t, client)

	app, err := client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
		StackId: aws.String(stackID),
		Name:    aws.String("app1"),
		Type:    types.AppTypeOther,
	})
	require.NoError(t, err)

	deployment, err := client.CreateDeployment(t.Context(), &opsworkssdk.CreateDeploymentInput{
		StackId:    aws.String(stackID),
		AppId:      app.AppId,
		Command:    &types.DeploymentCommand{Name: types.DeploymentCommandNameDeploy},
		CustomJson: aws.String(`{"key":"value"}`),
	})
	require.NoError(t, err)

	described, err := client.DescribeDeployments(t.Context(), &opsworkssdk.DescribeDeploymentsInput{
		DeploymentIds: []string{aws.ToString(deployment.DeploymentId)},
	})
	require.NoError(t, err)
	require.Len(t, described.Deployments, 1)
	assert.JSONEq(t, `{"key":"value"}`, aws.ToString(described.Deployments[0].CustomJson))
}

func newTestStack(t *testing.T, client *opsworkssdk.Client) string {
	t.Helper()

	stack, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
		Name:                      aws.String("s"),
		Region:                    aws.String(rtTestRegion),
		DefaultInstanceProfileArn: aws.String(testInstanceProfileArn),
		ServiceRoleArn:            aws.String(testServiceRoleArn),
	})
	require.NoError(t, err)

	return aws.ToString(stack.StackId)
}

func newTestStackWithLayer(t *testing.T, client *opsworkssdk.Client) (string, string) {
	t.Helper()

	stackID := newTestStack(t, client)

	layer, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
		StackId:   aws.String(stackID),
		Type:      types.LayerTypeCustom,
		Name:      aws.String("l"),
		Shortname: aws.String("l"),
	})
	require.NoError(t, err)

	return stackID, aws.ToString(layer.LayerId)
}
