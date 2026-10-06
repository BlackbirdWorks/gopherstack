package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func registerEC2Instances(t *testing.T, client *ecssdk.Client, cluster string, n int) []string {
	t.Helper()

	arns := make([]string, 0, n)

	for range n {
		out, err := client.RegisterContainerInstance(t.Context(), &ecssdk.RegisterContainerInstanceInput{
			Cluster: aws.String(cluster),
		})
		require.NoError(t, err)

		arns = append(arns, aws.ToString(out.ContainerInstance.ContainerInstanceArn))
	}

	return arns
}

func TestECS_RunTask_PlacementStrategyApplied(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		strategyType ecstypes.PlacementStrategyType
		wantSpread   bool
	}{
		{name: "binpack", strategyType: ecstypes.PlacementStrategyTypeBinpack, wantSpread: false},
		{name: "spread", strategyType: ecstypes.PlacementStrategyTypeSpread, wantSpread: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestECSClient(t, h)
			tdArn := registerTestTaskDef(t, h, "placement-td")
			registerEC2Instances(t, client, "default", 2)

			perInstance := map[string]int{}

			for range 4 {
				out, err := client.RunTask(t.Context(), &ecssdk.RunTaskInput{
					TaskDefinition:    aws.String(tdArn),
					LaunchType:        ecstypes.LaunchTypeEc2,
					PlacementStrategy: []ecstypes.PlacementStrategy{{Type: tt.strategyType}},
					PlacementConstraints: []ecstypes.PlacementConstraint{
						{Type: ecstypes.PlacementConstraintTypeDistinctInstance},
					},
				})
				require.NoError(t, err)
				require.Len(t, out.Tasks, 1)

				perInstance[aws.ToString(out.Tasks[0].ContainerInstanceArn)]++
			}

			if tt.wantSpread {
				assert.Len(t, perInstance, 2)

				for _, n := range perInstance {
					assert.Equal(t, 2, n)
				}

				return
			}

			assert.Len(t, perInstance, 1)
		})
	}
}

func TestECS_StartTask_AppliesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		propagate   ecstypes.PropagateTags
		include     []ecstypes.TaskField
		wantKeys    []string
		managedTags bool
		wantNone    bool
	}{
		{
			name:     "explicit with include",
			include:  []ecstypes.TaskField{ecstypes.TaskFieldTags},
			wantKeys: []string{"env"},
		},
		{name: "no include hides tags", wantNone: true},
		{
			name: "managed tags", include: []ecstypes.TaskField{ecstypes.TaskFieldTags}, managedTags: true,
			wantKeys: []string{
				"env", "aws:ecs:clusterName", "aws:ecs:taskDefinitionFamily", "aws:ecs:taskDefinitionRevision",
			},
		},
		{
			name: "propagate task definition", include: []ecstypes.TaskField{ecstypes.TaskFieldTags},
			propagate: ecstypes.PropagateTagsTaskDefinition, wantKeys: []string{"env"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestECSClient(t, h)
			tdArn := registerTestTaskDef(t, h, "start-task-tags-td")
			instances := registerEC2Instances(t, client, "default", 1)

			started, err := client.StartTask(t.Context(), &ecssdk.StartTaskInput{
				TaskDefinition:       aws.String(tdArn),
				ContainerInstances:   instances,
				Tags:                 []ecstypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				PropagateTags:        tt.propagate,
				EnableECSManagedTags: tt.managedTags,
			})
			require.NoError(t, err)
			require.Len(t, started.Tasks, 1)

			desc, err := client.DescribeTasks(t.Context(), &ecssdk.DescribeTasksInput{
				Tasks:   []string{aws.ToString(started.Tasks[0].TaskArn)},
				Include: tt.include,
			})
			require.NoError(t, err)
			require.Len(t, desc.Tasks, 1)

			if tt.wantNone {
				assert.Empty(t, desc.Tasks[0].Tags)

				return
			}

			keys := make([]string, 0, len(desc.Tasks[0].Tags))
			for _, tag := range desc.Tasks[0].Tags {
				keys = append(keys, aws.ToString(tag.Key))
			}

			assert.ElementsMatch(t, tt.wantKeys, keys)
		})
	}
}

func TestECS_Service_PlatformVersionManagedTagsAndRegistries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	tdArn := registerTestTaskDef(t, h, "svc-pv-td")

	created, err := client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
		ServiceName:          aws.String("svc-pv"),
		TaskDefinition:       aws.String(tdArn),
		PlatformVersion:      aws.String("1.4.0"),
		EnableECSManagedTags: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", aws.ToString(created.Service.PlatformVersion))
	assert.True(t, created.Service.EnableECSManagedTags)
	require.NotEmpty(t, created.Service.Deployments)
	assert.Equal(t, "1.4.0", aws.ToString(created.Service.Deployments[0].PlatformVersion))

	registryArn := "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-abc"

	updated, err := client.UpdateService(t.Context(), &ecssdk.UpdateServiceInput{
		Service:              aws.String("svc-pv"),
		PlatformVersion:      aws.String("LATEST"),
		EnableECSManagedTags: aws.Bool(false),
		ServiceRegistries:    []ecstypes.ServiceRegistry{{RegistryArn: aws.String(registryArn)}},
	})
	require.NoError(t, err)
	assert.Equal(t, "LATEST", aws.ToString(updated.Service.PlatformVersion))
	assert.False(t, updated.Service.EnableECSManagedTags)
	require.Len(t, updated.Service.ServiceRegistries, 1)
	assert.Equal(t, registryArn, aws.ToString(updated.Service.ServiceRegistries[0].RegistryArn))

	var primary *ecstypes.Deployment

	for i := range updated.Service.Deployments {
		if aws.ToString(updated.Service.Deployments[i].Status) == "PRIMARY" {
			primary = &updated.Service.Deployments[i]
		}
	}

	require.NotNil(t, primary)
	assert.Equal(t, "LATEST", aws.ToString(primary.PlatformVersion))

	_, err = client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
		ServiceName:     aws.String("svc-bad-pv"),
		TaskDefinition:  aws.String(tdArn),
		PlatformVersion: aws.String("9.9.9"),
	})

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
}

func TestECS_CreateService_ClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   *string
		name    string
		wantErr bool
	}{
		{name: "same token is idempotent", token: aws.String("tok-1")},
		{name: "different token conflicts", token: aws.String("tok-2"), wantErr: true},
		{name: "no token conflicts", token: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestECSClient(t, h)
			tdArn := registerTestTaskDef(t, h, "token-td")

			first, err := client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
				ServiceName:    aws.String("token-svc"),
				TaskDefinition: aws.String(tdArn),
				ClientToken:    aws.String("tok-1"),
			})
			require.NoError(t, err)

			second, err := client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
				ServiceName:    aws.String("token-svc"),
				TaskDefinition: aws.String(tdArn),
				ClientToken:    tt.token,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.Service.ServiceArn), aws.ToString(second.Service.ServiceArn))
		})
	}
}

func TestECS_ClusterConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	cfg := &ecstypes.ClusterConfiguration{
		ExecuteCommandConfiguration: &ecstypes.ExecuteCommandConfiguration{
			KmsKeyId: aws.String("arn:aws:kms:us-east-1:123456789012:key/abc"),
			Logging:  ecstypes.ExecuteCommandLoggingOverride,
			LogConfiguration: &ecstypes.ExecuteCommandLogConfiguration{
				CloudWatchLogGroupName: aws.String("/ecs/exec"),
				S3BucketName:           aws.String("exec-bucket"),
				S3EncryptionEnabled:    true,
			},
		},
		ManagedStorageConfiguration: &ecstypes.ManagedStorageConfiguration{
			KmsKeyId: aws.String("arn:aws:kms:us-east-1:123456789012:key/def"),
		},
	}

	created, err := client.CreateCluster(t.Context(), &ecssdk.CreateClusterInput{
		ClusterName:   aws.String("cfg-cluster"),
		Configuration: cfg,
	})
	require.NoError(t, err)
	assert.Equal(t, cfg, created.Cluster.Configuration)

	described, err := client.DescribeClusters(t.Context(), &ecssdk.DescribeClustersInput{
		Clusters: []string{"cfg-cluster"},
	})
	require.NoError(t, err)
	require.Len(t, described.Clusters, 1)
	assert.Equal(t, cfg, described.Clusters[0].Configuration)

	next := &ecstypes.ClusterConfiguration{
		ExecuteCommandConfiguration: &ecstypes.ExecuteCommandConfiguration{
			Logging: ecstypes.ExecuteCommandLoggingDefault,
		},
	}

	updated, err := client.UpdateCluster(t.Context(), &ecssdk.UpdateClusterInput{
		Cluster:       aws.String("cfg-cluster"),
		Configuration: next,
	})
	require.NoError(t, err)
	assert.Equal(t, next, updated.Cluster.Configuration)

	_, err = client.UpdateCluster(t.Context(), &ecssdk.UpdateClusterInput{
		Cluster: aws.String("cfg-cluster"),
		Configuration: &ecstypes.ClusterConfiguration{
			ExecuteCommandConfiguration: &ecstypes.ExecuteCommandConfiguration{Logging: "BOGUS"},
		},
	})

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
}

func TestECS_RegisterContainerInstance_AttributesTagsVersionInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	_, err := client.CreateCluster(t.Context(), &ecssdk.CreateClusterInput{ClusterName: aws.String("ci-cluster")})
	require.NoError(t, err)

	out, err := client.RegisterContainerInstance(t.Context(), &ecssdk.RegisterContainerInstanceInput{
		Cluster: aws.String("ci-cluster"),
		Attributes: []ecstypes.Attribute{
			{Name: aws.String("stack"), Value: aws.String("blue")},
		},
		Tags:        []ecstypes.Tag{{Key: aws.String("team"), Value: aws.String("core")}},
		VersionInfo: &ecstypes.VersionInfo{AgentVersion: aws.String("1.80.0"), DockerVersion: aws.String("25.0.0")},
	})
	require.NoError(t, err)

	ciArn := aws.ToString(out.ContainerInstance.ContainerInstanceArn)
	require.Len(t, out.ContainerInstance.Attributes, 1)
	assert.Equal(t, "stack", aws.ToString(out.ContainerInstance.Attributes[0].Name))

	desc, err := client.DescribeContainerInstances(t.Context(), &ecssdk.DescribeContainerInstancesInput{
		Cluster:            aws.String("ci-cluster"),
		ContainerInstances: []string{ciArn},
		Include:            []ecstypes.ContainerInstanceField{ecstypes.ContainerInstanceFieldTags},
	})
	require.NoError(t, err)
	require.Len(t, desc.ContainerInstances, 1)

	ci := desc.ContainerInstances[0]
	require.Len(t, ci.Attributes, 1)
	assert.Equal(t, "blue", aws.ToString(ci.Attributes[0].Value))
	assert.Equal(t, ciArn, aws.ToString(ci.Attributes[0].TargetId))
	require.Len(t, ci.Tags, 1)
	assert.Equal(t, "team", aws.ToString(ci.Tags[0].Key))
	require.NotNil(t, ci.VersionInfo)
	assert.Equal(t, "1.80.0", aws.ToString(ci.VersionInfo.AgentVersion))

	listed, err := client.ListAttributes(t.Context(), &ecssdk.ListAttributesInput{
		Cluster:    aws.String("ci-cluster"),
		TargetType: ecstypes.TargetTypeContainerInstance,
	})
	require.NoError(t, err)
	require.Len(t, listed.Attributes, 1)
}

func TestECS_RegisterTaskDefinition_ProxyConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		container string
		wantCode  string
	}{
		{name: "matching container", container: "envoy"},
		{name: "unknown container", container: "missing", wantCode: "ClientException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))

			out, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
				Family: aws.String("proxy-td"),
				ContainerDefinitions: []ecstypes.ContainerDefinition{
					{Name: aws.String("envoy"), Image: aws.String("envoy:v1"), Memory: aws.Int32(128)},
				},
				ProxyConfiguration: &ecstypes.ProxyConfiguration{
					ContainerName: aws.String(tt.container),
					Type:          ecstypes.ProxyConfigurationTypeAppmesh,
					Properties:    []ecstypes.KeyValuePair{{Name: aws.String("AppPorts"), Value: aws.String("8080")}},
				},
			})

			if tt.wantCode != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.TaskDefinition.ProxyConfiguration)
			assert.Equal(t, "envoy", aws.ToString(out.TaskDefinition.ProxyConfiguration.ContainerName))
			assert.Equal(t, ecstypes.ProxyConfigurationTypeAppmesh, out.TaskDefinition.ProxyConfiguration.Type)
			require.Len(t, out.TaskDefinition.ProxyConfiguration.Properties, 1)

			desc, err := client.DescribeTaskDefinition(t.Context(), &ecssdk.DescribeTaskDefinitionInput{
				TaskDefinition: out.TaskDefinition.TaskDefinitionArn,
			})
			require.NoError(t, err)
			assert.Equal(t, out.TaskDefinition.ProxyConfiguration, desc.TaskDefinition.ProxyConfiguration)
		})
	}
}
