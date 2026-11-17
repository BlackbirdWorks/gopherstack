package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTaskSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		comment string
		cpu     int
		mem     int
		cpuOK   bool
		memOK   bool
	}{
		{in: "1024", cpu: 1024, cpuOK: true, mem: 1024, memOK: true},
		{in: "1 vCPU", cpu: 1024, cpuOK: true},
		{in: "0.5 vcpu", cpu: 512, cpuOK: true},
		{in: "2 vCPUs", cpu: 2048, cpuOK: true},
		{in: "2GB", mem: 2048, memOK: true},
		{in: "0.5 GB", mem: 512, memOK: true},
		{in: "abc"},
		{in: "-5"},
		{in: ""},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			cpu, ok := parseTaskCPU(tt.in)
			assert.Equal(t, tt.cpuOK, ok)

			if ok {
				assert.Equal(t, tt.cpu, cpu)
			}

			mem, ok := parseTaskMemory(tt.in)
			assert.Equal(t, tt.memOK, ok)

			if ok {
				assert.Equal(t, tt.mem, mem)
			}
		})
	}
}

func TestFargateTaskSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cpu     string
		memory  string
		wantErr string
	}{
		{name: "valid numeric", cpu: "256", memory: "512"},
		{name: "valid vcpu gb", cpu: "1 vCPU", memory: "2 GB"},
		{name: "valid largest", cpu: "16384", memory: "122880"},
		{name: "memory not allowed for cpu", cpu: "256", memory: "4096", wantErr: "invalid Fargate memory"},
		{name: "cpu not allowed", cpu: "300", memory: "512", wantErr: "invalid Fargate CPU"},
		{name: "vcpu form wrong memory", cpu: "0.25 vCPU", memory: "8 GB", wantErr: "invalid Fargate memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
			_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "f", NetworkMode: networkModeAwsvpc, RequiresCompatibilities: []string{launchTypeFargate},
				CPU: tt.cpu, Memory: tt.memory,
				ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true}},
			})

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, ErrInvalidParameter)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestEphemeralStorageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "min", size: 21},
		{name: "max", size: 200},
		{name: "below", size: 20, wantErr: true},
		{name: "above", size: 201, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
			_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "f", EphemeralStorage: &EphemeralStorage{SizeInGiB: tt.size},
				ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true}},
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrClient)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestRunTask_TaskSizeReflectedAndOverridden(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "f", NetworkMode: networkModeAwsvpc, RequiresCompatibilities: []string{launchTypeFargate},
		CPU: "256", Memory: "512", EphemeralStorage: &EphemeralStorage{SizeInGiB: 30},
		ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true, Memory: 600}},
	})
	require.ErrorIs(t, err, ErrClient, "container memory above the task's")

	_, err = b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "f", NetworkMode: networkModeAwsvpc, RequiresCompatibilities: []string{launchTypeFargate},
		CPU: "256", Memory: "512", EphemeralStorage: &EphemeralStorage{SizeInGiB: 30},
		ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true, Memory: 256}},
	})
	require.NoError(t, err)

	tasks, _, err := b.RunTask(RunTaskInput{TaskDefinition: "f", LaunchType: launchTypeFargate})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "256", tasks[0].CPU)
	assert.Equal(t, "512", tasks[0].Memory)
	assert.Equal(t, 30, tasks[0].EphemeralStorage.SizeInGiB)

	over, _, err := b.RunTask(RunTaskInput{
		TaskDefinition: "f", LaunchType: launchTypeFargate,
		Overrides: &TaskOverride{CPU: "512", Memory: "1024", EphemeralStorage: &EphemeralStorage{SizeInGiB: 50}},
	})
	require.NoError(t, err)
	assert.Equal(t, "1024", over[0].Memory)
	assert.Equal(t, 50, over[0].EphemeralStorage.SizeInGiB)

	_, _, err = b.RunTask(RunTaskInput{
		TaskDefinition: "f", LaunchType: launchTypeFargate,
		Overrides: &TaskOverride{CPU: "256", Memory: "8192"},
	})
	require.ErrorIs(t, err, ErrInvalidParameter)

	_, _, err = b.RunTask(RunTaskInput{
		TaskDefinition: "f", LaunchType: launchTypeFargate,
		Overrides: &TaskOverride{EphemeralStorage: &EphemeralStorage{SizeInGiB: 5}},
	})
	require.ErrorIs(t, err, ErrInvalidParameter)
}

func TestRunTask_DockerTaskLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		task       Task
		cd         ContainerDefinition
		wantMemory int64
		wantNano   int64
	}{
		{
			name: "container without limits inherits task size",
			task: Task{CPU: "512", Memory: "1024"},
			cd:   ContainerDefinition{Name: "a", Image: "i"}, wantMemory: 1024 << 20, wantNano: 500_000_000,
		},
		{
			name: "container hard limit wins",
			task: Task{CPU: "1 vCPU", Memory: "2 GB"},
			cd: ContainerDefinition{
				Name:   "a",
				Image:  "i",
				Memory: 256,
			},
			wantMemory: 256 << 20,
			wantNano:   1_000_000_000,
		},
		{
			name: "no task size",
			task: Task{},
			cd:   ContainerDefinition{Name: "a", Image: "i"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := tt.task
			task.TaskArn = launchTaskArn

			fake := &fakeDockerClient{}
			r := newLaunchRunner(t, fake)
			require.NoError(t, r.RunTask(&task, &TaskDefinition{ContainerDefinitions: []ContainerDefinition{tt.cd}}))

			hc := fake.createAt(0).host
			assert.Equal(t, tt.wantMemory, hc.Memory)
			assert.Equal(t, tt.wantNano, hc.NanoCPUs)
		})
	}
}

func TestContainerTotalsWithinTask(t *testing.T) {
	t.Parallel()

	two := func(cpuA, cpuB, memA, memB int) []ContainerDefinition {
		return []ContainerDefinition{
			{Name: "a", Image: "i", Essential: true, CPU: cpuA, Memory: memA},
			{Name: "b", Image: "i", CPU: cpuB, Memory: memB},
		}
	}

	tests := []struct {
		name    string
		cpu     string
		memory  string
		wantErr string
		defs    []ContainerDefinition
	}{
		{name: "totals fit", cpu: "512", memory: "1024", defs: two(256, 256, 512, 512)},
		{
			name: "cpu sum over task", cpu: "512", memory: "1024", defs: two(256, 512, 256, 256),
			wantErr: "'cpu' settings (768)",
		},
		{
			name: "memory sum over task", cpu: "512", memory: "1024", defs: two(128, 128, 600, 600),
			wantErr: "'memory' settings (1200)",
		},
		{
			name: "vcpu spelling", cpu: "0.5 vCPU", memory: "1 GB", defs: two(300, 300, 1, 1),
			wantErr: "'cpu' settings (600)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "f", NetworkMode: networkModeAwsvpc, RequiresCompatibilities: []string{launchTypeFargate},
				CPU: tt.cpu, Memory: tt.memory, ContainerDefinitions: tt.defs,
			})

			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrClient)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			_, err = b.CreateCluster(CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			_, _, err = b.RunTask(RunTaskInput{
				Cluster: "c", TaskDefinition: td.TaskDefinitionArn, LaunchType: launchTypeFargate,
				Overrides: &TaskOverride{CPU: "256", Memory: "512"},
			})
			require.ErrorIs(t, err, ErrInvalidParameter)
			assert.Contains(t, err.Error(), "is greater than the task")
		})
	}
}
