package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTask_PortCollisionRetriesOtherInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantReason string
		count      int
		wantTasks  int
	}{
		{name: "both instances used", count: 2, wantTasks: 2},
		{name: "third has no instance", count: 3, wantTasks: 2, wantReason: failureReasonResourcePorts},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, id := range []string{"i-aaa0001", "i-bbb0002"} {
				_, err := b.RegisterContainerInstance("default", id)
				require.NoError(t, err)
			}

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:      "fixed-port",
				NetworkMode: networkModeBridge,
				ContainerDefinitions: []ContainerDefinition{
					{Name: "app", Image: "nginx", PortMappings: []PortMapping{{ContainerPort: 8080, HostPort: 8080}}},
				},
			})
			require.NoError(t, err)

			tasks, failures, err := b.RunTask(RunTaskInput{
				TaskDefinition: td.TaskDefinitionArn, LaunchType: "EC2", Count: tt.count,
			})
			require.NoError(t, err)
			assert.Len(t, tasks, tt.wantTasks)

			if tt.wantReason == "" {
				assert.Empty(t, failures)

				return
			}

			require.Len(t, failures, 1)
			assert.Equal(t, tt.wantReason, failures[0].Reason)
		})
	}
}

func TestPortRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		networkMode string
		ranges      []string
		wantErr     bool
	}{
		{name: "bridge ok", networkMode: networkModeBridge, ranges: []string{"8080-8090"}},
		{name: "awsvpc ok", networkMode: networkModeAwsvpc, ranges: []string{"8080-8090", "9000-9001"}},
		{name: "host rejected", networkMode: networkModeHost, ranges: []string{"8080-8090"}, wantErr: true},
		{name: "reversed", networkMode: networkModeBridge, ranges: []string{"8090-8080"}, wantErr: true},
		{name: "equal ends", networkMode: networkModeBridge, ranges: []string{"8080-8080"}, wantErr: true},
		{name: "zero start", networkMode: networkModeBridge, ranges: []string{"0-10"}, wantErr: true},
		{name: "too high", networkMode: networkModeBridge, ranges: []string{"65530-65536"}, wantErr: true},
		{name: "garbage", networkMode: networkModeBridge, ranges: []string{"abc"}, wantErr: true},
		{name: "overlap", networkMode: networkModeBridge, ranges: []string{"8080-8090", "8085-8095"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pms := make([]PortMapping, 0, len(tt.ranges))
			for _, r := range tt.ranges {
				pms = append(pms, PortMapping{ContainerPortRange: r})
			}

			_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:               "ranges",
				NetworkMode:          tt.networkMode,
				ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx", PortMappings: pms}},
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrClient)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPortRanges_BridgeAllocatesAndReleasesHostRange(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	ci, err := b.RegisterContainerInstance("default", "i-range001")
	require.NoError(t, err)

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:      "bridge-range",
		NetworkMode: networkModeBridge,
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx", PortMappings: []PortMapping{{ContainerPortRange: "8080-8084"}}},
		},
	})
	require.NoError(t, err)

	tasks, failures, err := b.RunTask(RunTaskInput{TaskDefinition: td.TaskDefinitionArn, LaunchType: "EC2", Count: 1})
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].Containers[0].NetworkBindings, 1)

	nb := tasks[0].Containers[0].NetworkBindings[0]
	assert.Equal(t, "8080-8084", nb.ContainerPortRange)

	hr, ok := parsePortRange(nb.HostPortRange)
	require.True(t, ok)
	assert.Equal(t, 5, hr.size())
	assert.GreaterOrEqual(t, hr.start, ephemeralPortRangeMin)
	assert.LessOrEqual(t, hr.end, ephemeralPortRangeMax)

	stored, found := b.containerInstances.Get(scopedKey("default", ci.ContainerInstanceArn))
	require.True(t, found)
	assert.Len(t, stored.AllocatedPorts, 5)

	_, err = b.StopTask("default", tasks[0].TaskArn, "done")
	require.NoError(t, err)
	assert.Empty(t, stored.AllocatedPorts)
}

func TestPortRanges_AwsvpcHostRangeEqualsContainerRange(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "awsvpc-range",
		NetworkMode:             networkModeAwsvpc,
		RequiresCompatibilities: []string{"FARGATE"},
		CPU:                     "256",
		Memory:                  "512",
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx", PortMappings: []PortMapping{{ContainerPortRange: "9000-9010"}}},
		},
	})
	require.NoError(t, err)

	tasks, failures, err := b.RunTask(
		RunTaskInput{TaskDefinition: td.TaskDefinitionArn, LaunchType: "FARGATE", Count: 1},
	)
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].Containers[0].NetworkBindings, 1)

	nb := tasks[0].Containers[0].NetworkBindings[0]
	assert.Equal(t, "9000-9010", nb.ContainerPortRange)
	assert.Equal(t, "9000-9010", nb.HostPortRange)
}

func TestRunTask_OccupiedInstanceNeverBlocksPlacement(t *testing.T) {
	t.Parallel()

	const attempts = 24

	for range attempts {
		b := newTestBackend()

		busy, err := b.RegisterContainerInstance("default", "i-busy0001")
		require.NoError(t, err)

		free, err := b.RegisterContainerInstance("default", "i-free0002")
		require.NoError(t, err)

		stored, found := b.containerInstances.Get(scopedKey("default", busy.ContainerInstanceArn))
		require.True(t, found)
		stored.AllocatedPorts = map[string]bool{hostPortKey(transportTCP, 8080): true}

		td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
			Family:      "fixed-port",
			NetworkMode: networkModeBridge,
			ContainerDefinitions: []ContainerDefinition{
				{Name: "app", Image: "nginx", PortMappings: []PortMapping{{ContainerPort: 8080, HostPort: 8080}}},
			},
		})
		require.NoError(t, err)

		tasks, failures, err := b.RunTask(
			RunTaskInput{TaskDefinition: td.TaskDefinitionArn, LaunchType: "EC2", Count: 1},
		)
		require.NoError(t, err)
		require.Empty(t, failures)
		require.Len(t, tasks, 1)
		assert.Equal(t, free.ContainerInstanceArn, tasks[0].ContainerInstanceArn)
	}
}
