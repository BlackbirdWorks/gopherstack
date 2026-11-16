package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestFirewallReferenceResolver(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portalloctest.New(t, 100),
	})
	require.NoError(t, err)

	byName := serviceByName(services)
	res := firewallReferences(
		ec2SubnetAdapter{regions: ec2Regions{handler: byName["EC2"].(*ec2backend.Handler)}},
		byName,
	)

	ec2b := byName["EC2"].(*ec2backend.Handler).Backend.(*ec2backend.InMemoryBackend)
	insts, err := ec2b.RunInstances("ami-12345678", "t3.small", "", 2)
	require.NoError(t, err)

	rg := byName["ResourceGroups"].(*resourcegroupsbackend.Handler).Backend
	_, err = rg.CreateGroup(t.Context(), "web", "", nil, nil, nil)
	require.NoError(t, err)

	arns := make([]string, 0, len(insts))
	for _, in := range insts {
		arns = append(arns, "arn:aws:ec2:us-east-1:000000000000:instance/"+in.ID)
	}

	_, err = rg.GroupResources(t.Context(), "web", arns)
	require.NoError(t, err)

	ecsb := byName["ECS"].(*ecsbackend.Handler).Backend.(*ecsbackend.InMemoryBackend)
	_, err = ecsb.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	td, err := ecsb.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family: "f", ContainerDefinitions: []ecsbackend.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, _, err = ecsb.RunTask(ecsbackend.RunTaskInput{
		Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: 2, LaunchType: "FARGATE",
	})
	require.NoError(t, err)

	tests := []struct {
		count func() (int, bool)
		name  string
		want  int
	}{
		{name: "group", count: func() (int, bool) {
			return res.ResourceGroupCIDRCount("arn:aws:resource-groups:us-east-1:000000000000:group/web")
		}, want: 2},
		{name: "cluster", count: func() (int, bool) {
			return res.ECSClusterCIDRCount("arn:aws:ecs:us-east-1:000000000000:cluster/c", nil)
		}, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n, ok := tt.count()
			assert.True(t, ok)
			assert.GreaterOrEqual(t, n, tt.want)
		})
	}
}
