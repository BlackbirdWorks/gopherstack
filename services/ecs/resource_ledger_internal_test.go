package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ledgerTD(t *testing.T, b *InMemoryBackend, family string, cpu, mem int) string {
	t.Helper()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:      family,
		NetworkMode: networkModeBridge,
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx", CPU: cpu, Memory: mem},
		},
	})
	require.NoError(t, err)

	return td.TaskDefinitionArn
}

func remainingInt(t *testing.T, cis []ContainerInstance, name string) int32 {
	t.Helper()

	require.Len(t, cis, 1)

	r := findResource(cis[0].RemainingResources, name)
	require.NotNil(t, r, name)

	return r.IntegerValue
}

func TestResourceLedger_Placement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantReason string
		runs       []struct{ cpu, mem int }
	}{
		{name: "fits", runs: []struct{ cpu, mem int }{{512, 256}, {512, 256}}},
		{name: "cpu_exhausted", wantReason: failureReasonCPU, runs: []struct{ cpu, mem int }{{768, 128}, {512, 128}}},
		{
			name: "memory_exhausted", wantReason: failureReasonMemory,
			runs: []struct{ cpu, mem int }{{128, 768}, {128, 512}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.RegisterContainerInstanceWithDetails("default", "i-led", ContainerInstanceDetails{
				Resources: []Resource{intResource(resourceCPU, 1024), intResource(resourceMemory, 1024)},
			})
			require.NoError(t, err)

			var lastFailures []Failure

			for i, r := range tt.runs {
				arn := ledgerTD(t, b, "led-"+tt.name+string(rune('a'+i)), r.cpu, r.mem)
				_, lastFailures, err = b.RunTask(RunTaskInput{TaskDefinition: arn, LaunchType: "EC2", Count: 1})
				require.NoError(t, err)
			}

			if tt.wantReason == "" {
				assert.Empty(t, lastFailures)

				return
			}

			require.Len(t, lastFailures, 1)
			assert.Equal(t, tt.wantReason, lastFailures[0].Reason)
		})
	}
}

func TestResourceLedger_RemainingTracksTasks(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.RegisterContainerInstanceWithDetails("default", "i-rem", ContainerInstanceDetails{
		Resources: []Resource{intResource(resourceCPU, 2048), intResource(resourceMemory, 4096)},
	})
	require.NoError(t, err)

	arn := ledgerTD(t, b, "rem", 1024, 1024)
	tasks, failures, err := b.RunTask(RunTaskInput{TaskDefinition: arn, LaunchType: "EC2", Count: 1})
	require.NoError(t, err)
	require.Empty(t, failures)

	cis, _, err := b.DescribeContainerInstances("default", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1024), remainingInt(t, cis, resourceCPU))
	assert.Equal(t, int32(3072), remainingInt(t, cis, resourceMemory))

	ports := findResource(cis[0].RemainingResources, resourcePorts)
	require.NotNil(t, ports)
	assert.Len(t, ports.StringSetValue, len(reservedAgentPorts))

	_, err = b.StopTask("default", tasks[0].TaskArn, "done")
	require.NoError(t, err)

	cis, _, err = b.DescribeContainerInstances("default", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2048), remainingInt(t, cis, resourceCPU))
	assert.Equal(t, int32(4096), remainingInt(t, cis, resourceMemory))
}

func TestResourceLedger_Defaults(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	b.SetInstanceResourceResolver(sizedResolver{cpu: 2048, mem: 8192})

	ci, err := b.RegisterContainerInstanceWithDetails("default", "i-sized", ContainerInstanceDetails{})
	require.NoError(t, err)

	assert.Equal(t, int32(2048), findResource(ci.RegisteredResources, resourceCPU).IntegerValue)
	assert.Equal(t, int32(8192), findResource(ci.RegisteredResources, resourceMemory).IntegerValue)
	assert.Equal(t, reservedAgentPorts, findResource(ci.RegisteredResources, resourcePorts).StringSetValue)
	require.NotNil(t, findResource(ci.RegisteredResources, resourcePortsUDP))
}

func TestResourceLedger_BinpackAndSpread(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy PlacementStrategy
		wantBusy bool
	}{
		{
			name:     "binpack_cpu_prefers_fuller",
			strategy: PlacementStrategy{Type: "binpack", Field: "cpu"},
			wantBusy: true,
		},
		{
			name: "binpack_memory_prefers_fuller", wantBusy: true,
			strategy: PlacementStrategy{Type: "binpack", Field: "memory"},
		},
		{name: "spread_prefers_emptier", strategy: PlacementStrategy{Type: "spread", Field: "instanceId"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			res := []Resource{intResource(resourceCPU, 2048), intResource(resourceMemory, 2048)}
			_, err := b.RegisterContainerInstanceWithDetails(
				"default",
				"i-busy",
				ContainerInstanceDetails{Resources: res},
			)
			require.NoError(t, err)
			_, err = b.RegisterContainerInstanceWithDetails(
				"default",
				"i-idle",
				ContainerInstanceDetails{Resources: res},
			)
			require.NoError(t, err)

			seed := ledgerTD(t, b, "seed-"+tt.name, 512, 512)
			tasks, _, err := b.RunTask(RunTaskInput{
				TaskDefinition: seed, LaunchType: "EC2", Count: 1,
				PlacementStrategy: []PlacementStrategy{{Type: "binpack", Field: "cpu"}},
			})
			require.NoError(t, err)

			seededOn := tasks[0].ContainerInstanceArn
			require.NotEmpty(t, seededOn)

			next := ledgerTD(t, b, "next-"+tt.name, 128, 128)
			placed, failures, err := b.RunTask(RunTaskInput{
				TaskDefinition: next, LaunchType: "EC2", Count: 1, PlacementStrategy: []PlacementStrategy{tt.strategy},
			})
			require.NoError(t, err)
			require.Empty(t, failures)

			if tt.wantBusy {
				assert.Equal(t, seededOn, placed[0].ContainerInstanceArn)
			} else {
				assert.NotEqual(t, seededOn, placed[0].ContainerInstanceArn)
			}
		})
	}
}

type sizedResolver struct{ cpu, mem int }

func (s sizedResolver) ContainerInstanceResources(string) (int, int, bool) { return s.cpu, s.mem, true }

func (s sizedResolver) InstanceTypeResources(string) (int, int, bool) { return s.cpu, s.mem, true }
