package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func netTestDef(mode string) *TaskDefinition {
	return &TaskDefinition{
		NetworkMode: mode,
		ContainerDefinitions: []ContainerDefinition{
			{
				Name: "web", Image: "busybox", Essential: true, Hostname: "h",
				PortMappings: []PortMapping{{ContainerPort: 80, HostPort: 8080}},
			},
			{Name: "side", Image: "busybox", Essential: true, DNSServers: []string{"1.1.1.1"}},
		},
	}
}

func TestRunTask_NetworkModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mode        string
		wantNetwork string
		wantPause   bool
		wantPorts   bool
	}{
		{name: "bridge default", mode: "", wantNetwork: "", wantPorts: true},
		{name: "bridge explicit", mode: networkModeBridge, wantNetwork: "", wantPorts: true},
		{name: "host", mode: networkModeHost, wantNetwork: "host"},
		{name: "none", mode: networkModeNone, wantNetwork: "none"},
		{name: "awsvpc", mode: networkModeAwsvpc, wantNetwork: "container:", wantPause: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			r := newLaunchRunner(t, fake)

			require.NoError(t, r.RunTask(launchTask(), netTestDef(tt.mode)))

			fake.mu.Lock()
			creates := append([]createCall(nil), fake.creates...)
			networks := append([]string(nil), fake.networks...)
			fake.mu.Unlock()

			apps := creates

			if tt.wantPause {
				require.Len(t, networks, 1)
				require.Len(t, creates, 3)
				pause := creates[0]
				assert.Equal(t, pauseImage(), pause.cfg.Image)
				assert.Equal(t, networks[0], string(pause.host.NetworkMode))
				assert.Len(t, pause.host.PortBindings, 1, "published ports live on the pause container")

				apps = creates[1:]
				assert.Equal(t, "172.30.0.2", r.TaskAddress(launchTaskArn))
			} else {
				assert.Empty(t, networks)
			}

			require.Len(t, apps, 2)

			for _, c := range apps {
				assert.Contains(t, string(c.host.NetworkMode), tt.wantNetwork)

				if tt.wantPause {
					assert.Equal(t, "container:"+creates[0].id, string(c.host.NetworkMode))
					assert.Empty(t, c.host.DNS)
					assert.Empty(t, c.host.PortBindings)
					assert.Empty(t, c.cfg.Hostname)
				}
			}

			assert.Equal(t, tt.wantPorts, len(apps[0].host.PortBindings) > 0)
		})
	}
}

func TestRunTask_AwsvpcNetworkTeardown(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	r := newLaunchRunner(t, fake)

	task := launchTask()
	require.NoError(t, r.RunTask(task, netTestDef(networkModeAwsvpc)))
	require.NoError(t, r.StopTask(task))

	fake.mu.Lock()
	defer fake.mu.Unlock()

	assert.Equal(t, []string{"net-" + taskNetworkName(task)}, fake.networksRemoved)
	assert.Len(t, fake.removed, 3, "app containers and the pause container are removed")
	assert.Empty(t, r.TaskAddress(task.TaskArn))
}

func TestBridgeMappings(t *testing.T) {
	t.Parallel()

	task := &Task{Containers: []Container{{
		Name:            "web",
		NetworkBindings: []NetworkBinding{{ContainerPort: 80, HostPort: 50001, Protocol: "tcp"}},
	}}}
	cd := ContainerDefinition{Name: "web", PortMappings: []PortMapping{
		{ContainerPort: 80},
		{ContainerPort: 81, HostPort: 9000},
		{ContainerPort: 82},
	}}

	got := bridgeMappings(task, cd)

	assert.Equal(t, 50001, got[0].HostPort)
	assert.Equal(t, 9000, got[1].HostPort)
	assert.Zero(t, got[2].HostPort, "no allocation leaves Docker to pick")

	pb, _ := buildPortMappings(got)
	addDynamicBindings(pb, got)
	assert.Len(t, pb, 3)
}
