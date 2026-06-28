package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ECSAudit_ListServicesFiltersByCluster verifies that
// ListServices scopes its results to the requested cluster and does not leak
// service ARNs that belong to a different cluster.
func TestIntegration_ECSAudit_ListServicesFiltersByCluster(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterA := "audit-svc-a-" + suffix
	clusterB := "audit-svc-b-" + suffix
	family := "audit-svc-task-" + suffix
	serviceA := "audit-service-a-" + suffix
	serviceB := "audit-service-b-" + suffix

	for _, cluster := range []string{clusterA, clusterB} {
		_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
			ClusterName: aws.String(cluster),
		})
		require.NoError(t, err)
	}

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	svcA, err := client.CreateService(ctx, &ecs.CreateServiceInput{
		ServiceName:    aws.String(serviceA),
		Cluster:        aws.String(clusterA),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotNil(t, svcA.Service)
	arnA := aws.ToString(svcA.Service.ServiceArn)
	require.NotEmpty(t, arnA)

	svcB, err := client.CreateService(ctx, &ecs.CreateServiceInput{
		ServiceName:    aws.String(serviceB),
		Cluster:        aws.String(clusterB),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotNil(t, svcB.Service)
	arnB := aws.ToString(svcB.Service.ServiceArn)
	require.NotEmpty(t, arnB)

	listA, err := client.ListServices(ctx, &ecs.ListServicesInput{
		Cluster: aws.String(clusterA),
	})
	require.NoError(t, err)

	assert.Contains(t, listA.ServiceArns, arnA,
		"ListServices for cluster A must include cluster A's service")
	assert.NotContains(t, listA.ServiceArns, arnB,
		"ListServices for cluster A must not include cluster B's service")
	assert.Len(t, listA.ServiceArns, 1,
		"ListServices for cluster A must return exactly cluster A's single service")
}

// TestIntegration_ECSAudit_ListServicesPagination verifies that ListServices
// honors MaxResults and NextToken so clients can page through results.
func TestIntegration_ECSAudit_ListServicesPagination(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	cluster := "audit-svc-page-" + suffix
	family := "audit-svc-page-task-" + suffix

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(cluster),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	const total = 3
	want := make(map[string]bool, total)
	for range total {
		out, createErr := client.CreateService(ctx, &ecs.CreateServiceInput{
			ServiceName:    aws.String("audit-page-svc-" + uuid.NewString()[:8]),
			Cluster:        aws.String(cluster),
			TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
			DesiredCount:   aws.Int32(1),
		})
		require.NoError(t, createErr)
		want[aws.ToString(out.Service.ServiceArn)] = true
	}

	got := make(map[string]bool, total)
	var nextToken *string
	pages := 0
	for {
		out, listErr := client.ListServices(ctx, &ecs.ListServicesInput{
			Cluster:    aws.String(cluster),
			MaxResults: aws.Int32(1),
			NextToken:  nextToken,
		})
		require.NoError(t, listErr)
		assert.LessOrEqual(t, len(out.ServiceArns), 1,
			"each page must contain at most MaxResults entries")

		for _, arn := range out.ServiceArns {
			got[arn] = true
		}

		pages++
		require.LessOrEqual(t, pages, total+1, "pagination must terminate")

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}
		nextToken = out.NextToken
	}

	assert.Equal(t, want, got,
		"paging through ListServices must yield exactly the created services")
	assert.Greater(t, pages, 1, "MaxResults=1 over multiple services must span multiple pages")
}
