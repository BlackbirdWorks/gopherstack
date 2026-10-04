package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

const euRegion = "eu-west-1"

func TestArnRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "ecs-cluster", arn: "arn:aws:ecs:eu-west-1:000000000000:cluster/c", want: "eu-west-1"},
		{name: "global", arn: "arn:aws:iam::000000000000:role/r", want: ""},
		{name: "name", arn: "my-cluster", want: ""},
		{name: "empty", arn: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, arnRegion(tc.arn))
		})
	}
}

func TestInitializeServices_EC2ECSRegionWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19500, 19600)
	require.NoError(t, err)

	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(&service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portAlloc,
	})
	require.NoError(t, err)

	byName := serviceByName(services)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	euEC2, ok := ec2H.BackendFor(euRegion).(*ec2backend.InMemoryBackend)
	require.True(t, ok)

	vpc, err := euEC2.CreateVpc("10.20.0.0/16", "")
	require.NoError(t, err)

	subnet, err := euEC2.CreateSubnet(vpc.ID, "10.20.1.0/24", euRegion+"b")
	require.NoError(t, err)

	vgw, err := euEC2.CreateVpnGateway("ipsec.1", 0)
	require.NoError(t, err)

	regions := ec2Regions{handler: ec2H}
	subnetArn := "arn:aws:ec2:" + euRegion + ":000000000000:subnet/" + subnet.ID

	t.Run("resolvers-find-other-region-resources", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			check func() bool
			name  string
			want  bool
		}{
			{
				name:  "elb-subnet",
				check: func() bool { return (&elbEC2ResolverAdapter{regions: regions}).SubnetExists(subnet.ID) },
				want:  true,
			},
			{
				name:  "elb-unknown",
				check: func() bool { return (&elbEC2ResolverAdapter{regions: regions}).SubnetExists("subnet-none") },
			},
			{
				name:  "efs-az",
				check: func() bool { return (&efsEC2ResolverAdapter{regions: regions}).SubnetAZ(subnet.ID) == euRegion+"b" },
				want:  true,
			},
			{
				name: "directconnect-vgw",
				check: func() bool {
					return (&directConnectEC2ResolverAdapter{regions: regions}).ResolveVpnGateway(vgw.VpnGatewayID)
				},
				want: true,
			},
			{
				name:  "networkmanager-arn-region",
				check: func() bool { return (&networkManagerEC2ResolverAdapter{regions: regions}).ResolveSubnet(subnetArn) },
				want:  true,
			},
			{
				name: "networkmanager-wrong-region",
				check: func() bool {
					return (&networkManagerEC2ResolverAdapter{regions: regions}).
						ResolveSubnet("arn:aws:ec2:us-east-1:000000000000:subnet/" + subnet.ID)
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				assert.Equal(t, tc.want, tc.check())
			})
		}
	})

	t.Run("cloudwatch-alarm-stops-instance-in-its-region", func(t *testing.T) {
		t.Parallel()

		cwH, isCW := byName["CloudWatch"].(*cwbackend.Handler)
		require.True(t, isCW)

		homeEC2, isMem := ec2H.Backend.(*ec2backend.InMemoryBackend)
		require.True(t, isMem)

		euInst, runErr := euEC2.RunInstances("ami-12345678", "t3.micro", "", 1)
		require.NoError(t, runErr)

		homeInst, runErr := homeEC2.RunInstances("ami-12345678", "t3.micro", "", 1)
		require.NoError(t, runErr)

		cwBk, isCWBk := cwH.BackendFor(euRegion).(*cwbackend.InMemoryBackend)
		require.True(t, isCWBk)

		require.NoError(t, cwBk.PutMetricAlarm(&cwbackend.MetricAlarm{
			AlarmName:      "eu-cpu",
			StateValue:     "OK",
			ActionsEnabled: true,
			AlarmActions:   []string{"arn:aws:automate:" + euRegion + ":ec2:stop"},
			Dimensions:     []cwbackend.Dimension{{Name: "InstanceId", Value: euInst[0].ID}},
		}))
		require.NoError(t, cwBk.SetAlarmState(t.Context(), "eu-cpu", "ALARM", "high", ""))

		status := euEC2.DescribeInstanceStatus([]string{euInst[0].ID})
		require.Len(t, status, 1)
		// The stop transition is timed, so a loaded runner may already report "stopped".
		assert.Contains(t, []string{"stopping", "stopped"}, status[0].State.Name)

		homeStatus := homeEC2.DescribeInstanceStatus([]string{homeInst[0].ID})
		require.Len(t, homeStatus, 1)
		assert.NotContains(t, []string{"stopping", "stopped"}, homeStatus[0].State.Name)
	})

	t.Run("ecs-awslogs-and-eventbridge-use-task-region", func(t *testing.T) {
		t.Parallel()

		ecsH, isECS := byName["ECS"].(*ecsbackend.Handler)
		require.True(t, isECS)

		euECS, isMem := ecsH.BackendFor(euRegion).(*ecsbackend.InMemoryBackend)
		require.True(t, isMem)

		_, err = euECS.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "c"})
		require.NoError(t, err)

		td, tdErr := euECS.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
			Family: "logger",
			ContainerDefinitions: []ecsbackend.ContainerDefinition{{
				Name: "app", Image: "nginx",
				LogConfiguration: &ecsbackend.LogConfiguration{
					LogDriver: "awslogs", Options: map[string]string{"awslogs-group": "/ecs/eu-app"},
				},
			}},
		})
		require.NoError(t, tdErr)

		adapter := &ebECSTaskRunnerAdapter{handler: ecsH}
		require.NoError(t, adapter.RunTaskWithParams(t.Context(),
			"arn:aws:ecs:"+euRegion+":000000000000:cluster/c",
			&ebbackend.EcsParameters{TaskDefinitionArn: td.TaskDefinitionArn, TaskCount: 1}, nil))

		tasks, listErr := euECS.ListTasks("c")
		require.NoError(t, listErr)
		assert.Len(t, tasks, 1)

		cwlogsH, isLogs := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
		require.True(t, isLogs)

		logsBk, isLogsBk := cwlogsH.Backend.(*cwlogsbackend.InMemoryBackend)
		require.True(t, isLogsBk)

		euCtx := cwlogsbackend.WithRegion(t.Context(), euRegion)
		euGroups, _, descErr := logsBk.DescribeLogGroups(euCtx, "/ecs/", "", "", 0)
		require.NoError(t, descErr)
		assert.Len(t, euGroups, 1)

		homeGroups, _, descErr := logsBk.DescribeLogGroups(t.Context(), "/ecs/", "", "", 0)
		require.NoError(t, descErr)
		assert.Empty(t, homeGroups)
	})
}
