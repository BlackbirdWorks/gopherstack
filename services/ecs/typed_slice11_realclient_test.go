package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice11RealClient drives ecs's typed-coverage-blind ops
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("clusters and capacity providers", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestECSClient(t, h)
		ctx := t.Context()

		_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
			ClusterName: aws.String("s11-cluster"),
		})
		require.NoError(t, err)

		listOut, err := client.ListClusters(ctx, &ecssdk.ListClustersInput{})
		require.NoError(t, err)
		found := false
		for _, arn := range listOut.ClusterArns {
			if arn != "" {
				found = true
			}
		}
		assert.True(t, found, "ListClusters must return at least one cluster ARN")

		cpArn := createCapacityProviderForDaemon(t, h, "s11-cp")
		descOut, err := client.DescribeCapacityProviders(ctx, &ecssdk.DescribeCapacityProvidersInput{
			CapacityProviders: []string{cpArn},
		})
		require.NoError(t, err)
		require.Len(t, descOut.CapacityProviders, 1)
		assert.Equal(t, cpArn, aws.ToString(descOut.CapacityProviders[0].CapacityProviderArn))
	})

	t.Run("task definitions delete and deregister", func(t *testing.T) {
		t.Parallel()

		client := newTestECSClient(t, newTestHandler(t))
		ctx := t.Context()

		regOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
			Family: aws.String("s11-taskdef-family"),
			ContainerDefinitions: []ecstypes.ContainerDefinition{
				{Name: aws.String("app"), Image: aws.String("example/app:latest")},
			},
		})
		require.NoError(t, err)
		tdArn := aws.ToString(regOut.TaskDefinition.TaskDefinitionArn)

		deregOut, err := client.DeregisterTaskDefinition(ctx, &ecssdk.DeregisterTaskDefinitionInput{
			TaskDefinition: aws.String(tdArn),
		})
		require.NoError(t, err)
		assert.Equal(t, ecstypes.TaskDefinitionStatusInactive, deregOut.TaskDefinition.Status)

		delOut, err := client.DeleteTaskDefinitions(ctx, &ecssdk.DeleteTaskDefinitionsInput{
			TaskDefinitions: []string{tdArn},
		})
		require.NoError(t, err)
		require.Len(t, delOut.TaskDefinitions, 1)
		assert.Equal(t, ecstypes.TaskDefinitionStatusDeleteInProgress, delOut.TaskDefinitions[0].Status)
		assert.Empty(t, delOut.Failures)
	})

	t.Run("discover poll endpoint", func(t *testing.T) {
		t.Parallel()

		client := newTestECSClient(t, newTestHandler(t))
		ctx := t.Context()

		out, err := client.DiscoverPollEndpoint(ctx, &ecssdk.DiscoverPollEndpointInput{})
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(out.Endpoint), "https://")
		assert.Contains(t, aws.ToString(out.ServiceConnectEndpoint), "https://")
		assert.Contains(t, aws.ToString(out.TelemetryEndpoint), "https://")
	})

	t.Run("list services by namespace", func(t *testing.T) {
		t.Parallel()

		client := newTestECSClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
			ClusterName: aws.String("s11-ns-cluster"),
		})
		require.NoError(t, err)

		regOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
			Family: aws.String("s11-ns-taskdef"),
			ContainerDefinitions: []ecstypes.ContainerDefinition{
				{Name: aws.String("app"), Image: aws.String("example/app:latest")},
			},
		})
		require.NoError(t, err)
		tdArn := aws.ToString(regOut.TaskDefinition.TaskDefinitionArn)

		svcOut, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
			Cluster:        aws.String("s11-ns-cluster"),
			ServiceName:    aws.String("s11-namespace-svc"),
			TaskDefinition: aws.String(tdArn),
			DesiredCount:   aws.Int32(0),
			ServiceConnectConfiguration: &ecstypes.ServiceConnectConfiguration{
				Enabled:   true,
				Namespace: aws.String("s11-namespace"),
			},
		})
		require.NoError(t, err)
		svcArn := aws.ToString(svcOut.Service.ServiceArn)

		// Real ListServicesByNamespaceInput has no Cluster member at all --
		// this op spans every cluster in the namespace, matching a service's
		// own ServiceConnectConfiguration.Namespace, not a substring of the
		// service's name.
		listOut, err := client.ListServicesByNamespace(ctx, &ecssdk.ListServicesByNamespaceInput{
			Namespace: aws.String("s11-namespace"),
		})
		require.NoError(t, err)
		assert.Contains(t, listOut.ServiceArns, svcArn)
	})

	t.Run("submit agent state changes", func(t *testing.T) {
		t.Parallel()

		client := newTestECSClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
			ClusterName: aws.String("s11-agent-cluster"),
		})
		require.NoError(t, err)

		// Unknown cluster/task/attachment ARNs are tolerated as no-ops by this
		// backend's agent-internal API contract -- asserting no error is the
		// correct, honest assertion here.
		_, err = client.SubmitTaskStateChange(ctx, &ecssdk.SubmitTaskStateChangeInput{
			Cluster: aws.String("s11-agent-cluster"),
			Task:    aws.String("s11-unknown-task"),
			Status:  aws.String("RUNNING"),
		})
		require.NoError(t, err)

		_, err = client.SubmitContainerStateChange(ctx, &ecssdk.SubmitContainerStateChangeInput{
			Cluster:       aws.String("s11-agent-cluster"),
			Task:          aws.String("s11-unknown-task"),
			ContainerName: aws.String("app"),
			Status:        aws.String("RUNNING"),
		})
		require.NoError(t, err)

		_, err = client.SubmitAttachmentStateChanges(ctx, &ecssdk.SubmitAttachmentStateChangesInput{
			Cluster: aws.String("s11-agent-cluster"),
			Attachments: []ecstypes.AttachmentStateChange{
				{AttachmentArn: aws.String("s11-unknown-attachment"), Status: aws.String("ATTACHED")},
			},
		})
		require.NoError(t, err)
	})

	t.Run("continue and describe service deployments", func(t *testing.T) {
		t.Parallel()

		client := newTestECSClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
			ClusterName: aws.String("s11-svcdep-cluster"),
		})
		require.NoError(t, err)

		regOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
			Family: aws.String("s11-svcdep-taskdef"),
			ContainerDefinitions: []ecstypes.ContainerDefinition{
				{Name: aws.String("app"), Image: aws.String("example/app:latest")},
			},
		})
		require.NoError(t, err)
		tdArn := aws.ToString(regOut.TaskDefinition.TaskDefinitionArn)

		svcOut, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
			Cluster:        aws.String("s11-svcdep-cluster"),
			ServiceName:    aws.String("s11-svcdep-svc"),
			TaskDefinition: aws.String(tdArn),
			DesiredCount:   aws.Int32(0),
		})
		require.NoError(t, err)

		listOut, err := client.ListServiceDeployments(ctx, &ecssdk.ListServiceDeploymentsInput{
			Cluster: aws.String("s11-svcdep-cluster"),
			Service: svcOut.Service.ServiceName,
		})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.ServiceDeployments)
		sdArn := aws.ToString(listOut.ServiceDeployments[0].ServiceDeploymentArn)

		descOut, err := client.DescribeServiceDeployments(ctx, &ecssdk.DescribeServiceDeploymentsInput{
			ServiceDeploymentArns: []string{sdArn, "arn:aws:ecs:us-east-1:000000000000:service-deployment/missing"},
		})
		require.NoError(t, err)
		require.Len(t, descOut.ServiceDeployments, 1)
		require.Len(t, descOut.Failures, 1)
		assert.Equal(t, sdArn, aws.ToString(descOut.ServiceDeployments[0].ServiceDeploymentArn))

		// gopherstack doesn't model lifecycle hooks, so a deployment is never
		// actually paused -- ContinueServiceDeployment honestly reports no
		// paused hook rather than fabricating a successful continue.
		_, err = client.ContinueServiceDeployment(ctx, &ecssdk.ContinueServiceDeploymentInput{
			ServiceDeploymentArn: aws.String(sdArn),
			HookId:               aws.String("s11-hook"),
		})
		require.Error(t, err, "no lifecycle hook is ever actually paused in this backend")
	})

	t.Run("daemon lifecycle", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestECSClient(t, h)
		ctx := t.Context()

		tdArn := registerDaemonTaskDef(t, h, "s11-daemon-family")
		cpArn := createCapacityProviderForDaemon(t, h, "s11-daemon-cp")

		createOut, err := client.CreateDaemon(ctx, &ecssdk.CreateDaemonInput{
			DaemonName:              aws.String("s11-daemon"),
			DaemonTaskDefinitionArn: aws.String(tdArn),
			CapacityProviderArns:    []string{cpArn},
		})
		require.NoError(t, err)
		daemonArn := aws.ToString(createOut.DaemonArn)

		listOut, err := client.ListDaemons(ctx, &ecssdk.ListDaemonsInput{})
		require.NoError(t, err)
		listedArns := make([]string, 0, len(listOut.DaemonSummariesList))
		for _, d := range listOut.DaemonSummariesList {
			listedArns = append(listedArns, aws.ToString(d.DaemonArn))
		}
		assert.Contains(t, listedArns, daemonArn)

		tdArn2 := registerDaemonTaskDef(t, h, "s11-daemon-family-2")

		updOut, err := client.UpdateDaemon(ctx, &ecssdk.UpdateDaemonInput{
			DaemonArn:               aws.String(daemonArn),
			DaemonTaskDefinitionArn: aws.String(tdArn2),
			CapacityProviderArns:    []string{cpArn},
		})
		require.NoError(t, err)
		assert.Equal(t, daemonArn, aws.ToString(updOut.DaemonArn))
		assert.Equal(t, ecstypes.DaemonStatusActive, updOut.Status)
		deploymentArn := aws.ToString(updOut.DeploymentArn)
		require.NotEmpty(t, deploymentArn)

		descOut, err := client.DescribeDaemon(ctx, &ecssdk.DescribeDaemonInput{
			DaemonArn: aws.String(daemonArn),
		})
		require.NoError(t, err)
		require.NotNil(t, descOut.Daemon)
		// DaemonDetail (the real DescribeDaemonOutput.Daemon type) exposes no
		// DaemonTaskDefinitionArn field at all -- only per-revision snapshots
		// under CurrentRevisions, each carrying its own capacity providers.
		require.Len(t, descOut.Daemon.CurrentRevisions, 1)
		revisionArn := aws.ToString(descOut.Daemon.CurrentRevisions[0].Arn)
		require.NotEmpty(t, revisionArn)
		require.Len(t, descOut.Daemon.CurrentRevisions[0].CapacityProviders, 1)
		assert.Equal(t, cpArn, aws.ToString(descOut.Daemon.CurrentRevisions[0].CapacityProviders[0].Arn))

		depListOut, err := client.ListDaemonDeployments(ctx, &ecssdk.ListDaemonDeploymentsInput{
			DaemonArn: aws.String(daemonArn),
		})
		require.NoError(t, err)
		found := false
		for _, d := range depListOut.DaemonDeployments {
			if aws.ToString(d.DaemonDeploymentArn) == deploymentArn {
				found = true
			}
		}
		assert.True(t, found, "ListDaemonDeployments must include the deployment just created")

		descDepOut, err := client.DescribeDaemonDeployments(ctx, &ecssdk.DescribeDaemonDeploymentsInput{
			DaemonDeploymentArns: []string{
				deploymentArn,
				"arn:aws:ecs:us-east-1:000000000000:daemon-deployment/missing",
			},
		})
		require.NoError(t, err)
		require.Len(t, descDepOut.DaemonDeployments, 1)
		require.Len(t, descDepOut.Failures, 1)

		descRevOut, err := client.DescribeDaemonRevisions(ctx, &ecssdk.DescribeDaemonRevisionsInput{
			DaemonRevisionArns: []string{revisionArn, "arn:aws:ecs:us-east-1:000000000000:daemon-revision/missing"},
		})
		require.NoError(t, err)
		require.Len(t, descRevOut.DaemonRevisions, 1)
		require.Len(t, descRevOut.Failures, 1)
		assert.Equal(t, revisionArn, aws.ToString(descRevOut.DaemonRevisions[0].DaemonRevisionArn))

		delOut, err := client.DeleteDaemon(ctx, &ecssdk.DeleteDaemonInput{
			DaemonArn: aws.String(daemonArn),
		})
		require.NoError(t, err)
		assert.Equal(t, daemonArn, aws.ToString(delOut.DaemonArn))
		assert.Equal(t, ecstypes.DaemonStatusDeleteInProgress, delOut.Status)

		// DeleteDaemonTaskDefinitionOutput carries only the ARN on the real
		// wire -- no Status field exists to assert against.
		delTdOut, err := client.DeleteDaemonTaskDefinition(ctx, &ecssdk.DeleteDaemonTaskDefinitionInput{
			DaemonTaskDefinition: aws.String(tdArn),
		})
		require.NoError(t, err)
		assert.Equal(t, tdArn, aws.ToString(delTdOut.DaemonTaskDefinitionArn))
	})
}
