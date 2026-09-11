package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunTask_EC2Placement_HostPorts covers the placement outcomes
// gopherstack-fpro adds for EC2-launch-type tasks: bridge dynamic and fixed
// host-port assignment, host-mode hostPort==containerPort, the RESOURCE:*
// failure when no container instance exists, the RESOURCE:PORTS failure on
// a port collision, and that EC2+awsvpc placement (pre-existing behavior)
// is unaffected.
func TestRunTask_EC2Placement_HostPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		networkMode   string
		wantReason    string
		occupyPort    int
		containerPort int
		hostPort      int
		wantHostPort  int
		registerCI    bool
		wantOK        bool
		wantAllocated bool
	}{
		{
			name:          "bridge dynamic port",
			networkMode:   networkModeBridge,
			containerPort: 8080,
			registerCI:    true,
			wantOK:        true,
			wantAllocated: true,
		},
		{
			name:          "bridge fixed port free",
			networkMode:   networkModeBridge,
			containerPort: 8080,
			hostPort:      8080,
			registerCI:    true,
			wantOK:        true,
			wantHostPort:  8080,
			wantAllocated: true,
		},
		{
			name:          "bridge fixed port collision",
			networkMode:   networkModeBridge,
			containerPort: 8080,
			hostPort:      8080,
			occupyPort:    8080,
			registerCI:    true,
			wantOK:        false,
			wantReason:    failureReasonResourcePorts,
		},
		{
			name:          "host mode hostPort equals containerPort",
			networkMode:   networkModeHost,
			containerPort: 9090,
			registerCI:    true,
			wantOK:        true,
			wantHostPort:  9090,
			wantAllocated: true,
		},
		{
			name:          "host mode collision",
			networkMode:   networkModeHost,
			containerPort: 9090,
			occupyPort:    9090,
			registerCI:    true,
			wantOK:        false,
			wantReason:    failureReasonResourcePorts,
		},
		{
			name:          "no container instance registered",
			networkMode:   networkModeBridge,
			containerPort: 8080,
			registerCI:    false,
			wantOK:        false,
			wantReason:    failureReasonResource,
		},
		{
			name:          "awsvpc unaffected: no host port consumed",
			networkMode:   networkModeAwsvpc,
			containerPort: 80,
			registerCI:    true,
			wantOK:        true,
			wantHostPort:  80, // historical hostPort==containerPort default, unchanged
			wantAllocated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var ci *ContainerInstance

			if tt.registerCI {
				var err error

				ci, err = b.RegisterContainerInstance("default", "i-"+tt.name)
				require.NoError(t, err)
			}

			if tt.occupyPort != 0 {
				stored, found := b.containerInstances.Get(scopedKey("default", ci.ContainerInstanceArn))
				require.True(t, found)
				stored.AllocatedPorts = map[string]bool{hostPortKey(transportTCP, tt.occupyPort): true}
			}

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:      "placement-" + tt.name,
				NetworkMode: tt.networkMode,
				ContainerDefinitions: []ContainerDefinition{
					{Name: "app", Image: "nginx", PortMappings: []PortMapping{
						{ContainerPort: tt.containerPort, HostPort: tt.hostPort},
					}},
				},
			})
			require.NoError(t, err)

			tasks, failures, err := b.RunTask(RunTaskInput{
				TaskDefinition: td.TaskDefinitionArn,
				LaunchType:     "EC2",
				Count:          1,
			})
			require.NoError(t, err)

			if !tt.wantOK {
				require.Empty(t, tasks)
				require.Len(t, failures, 1)
				assert.Equal(t, tt.wantReason, failures[0].Reason)

				return
			}

			require.Empty(t, failures)
			require.Len(t, tasks, 1)
			require.Len(t, tasks[0].Containers[0].NetworkBindings, 1)

			binding := tasks[0].Containers[0].NetworkBindings[0]
			assert.Equal(t, tt.containerPort, binding.ContainerPort)

			if tt.wantHostPort != 0 {
				assert.Equal(t, tt.wantHostPort, binding.HostPort)
			} else {
				assert.GreaterOrEqual(t, binding.HostPort, ephemeralPortRangeMin)
				assert.LessOrEqual(t, binding.HostPort, ephemeralPortRangeMax)
			}

			stored, found := b.containerInstances.Get(scopedKey("default", ci.ContainerInstanceArn))
			require.True(t, found)

			if tt.wantAllocated {
				assert.True(t, stored.AllocatedPorts[hostPortKey(transportTCP, binding.HostPort)])
			} else {
				assert.Empty(t, stored.AllocatedPorts)
			}
		})
	}
}

// TestRunTask_BridgeMode_DynamicPortsUniqueAndReleasedOnStop proves two
// tasks placed on the same container instance in one RunTask call never
// collide on a dynamically assigned port, and that stopping one task frees
// only its own reservation, not its sibling's.
func TestRunTask_BridgeMode_DynamicPortsUniqueAndReleasedOnStop(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	ci, err := b.RegisterContainerInstance("default", "i-dyn0001")
	require.NoError(t, err)

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:      "bridge-dyn",
		NetworkMode: networkModeBridge,
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx", PortMappings: []PortMapping{{ContainerPort: 8080}}},
		},
	})
	require.NoError(t, err)

	tasks, failures, err := b.RunTask(RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		LaunchType:     "EC2",
		Count:          2,
	})
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, tasks, 2)

	port1 := tasks[0].Containers[0].NetworkBindings[0].HostPort
	port2 := tasks[1].Containers[0].NetworkBindings[0].HostPort
	assert.NotEqual(t, port1, port2, "two tasks on the same instance must not share a dynamic host port")

	stored, found := b.containerInstances.Get(scopedKey("default", ci.ContainerInstanceArn))
	require.True(t, found)
	assert.True(t, stored.AllocatedPorts[hostPortKey(transportTCP, port1)])
	assert.True(t, stored.AllocatedPorts[hostPortKey(transportTCP, port2)])

	_, err = b.StopTask("default", tasks[0].TaskArn, "test stop")
	require.NoError(t, err)

	assert.False(t, stored.AllocatedPorts[hostPortKey(transportTCP, port1)], "stopped task's port must be released")
	assert.True(
		t,
		stored.AllocatedPorts[hostPortKey(transportTCP, port2)],
		"still-running task's port must stay reserved",
	)
}
