package ecs

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

type usageRunner struct {
	TaskRunner
	cpu, mem float64
}

func (u usageRunner) TaskUsage(context.Context, string) (float64, float64, bool) {
	return u.cpu, u.mem, true
}

type recordingEmitter struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (r *recordingEmitter) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = append(r.points, p)

	return nil
}

func TestEmitUtilizationMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cpu     float64
		memMiB  float64
		wantCPU float64
		wantMem float64
	}{
		{name: "half_cpu_quarter_mem", cpu: 128, memMiB: 128, wantCPU: 50, wantMem: 25},
		{name: "idle", wantCPU: 0, wantMem: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(
				"123456789012",
				"us-east-1",
				usageRunner{TaskRunner: NewNoopRunner(), cpu: tt.cpu, mem: tt.memMiB * bytesPerMiB},
			)
			rec := &recordingEmitter{}
			b.SetMetricEmitter(rec)

			s := newFargateServiceWithLB(t, b, "svc-usage")
			require.NoError(t, b.StartTaskForService(s.cluster, s.service, s.tdArn))

			b.emitUtilizationMetrics(t.Context())

			got := map[string]float64{}

			for _, p := range rec.points {
				assert.Equal(t, "AWS/ECS", p.Namespace)
				assert.Equal(t, "Percent", p.Unit)
				assert.Equal(t, []cwmetric.Dimension{
					{Name: "ClusterName", Value: s.cluster}, {Name: "ServiceName", Value: s.service},
				}, p.Dimensions)

				got[p.Name] = p.Value
			}

			assert.InDelta(t, tt.wantCPU, got["CPUUtilization"], 0.001)
			assert.InDelta(t, tt.wantMem, got["MemoryUtilization"], 0.001)
			assert.Len(t, got, 2)
		})
	}
}

func TestDockerRunner_TaskUsage(t *testing.T) {
	t.Parallel()

	const sample = `{
		"cpu_stats": {"cpu_usage": {"total_usage": 300}, "system_cpu_usage": 2000, "online_cpus": 2},
		"precpu_stats": {"cpu_usage": {"total_usage": 100}, "system_cpu_usage": 1000},
		"memory_stats": {"usage": 314572800, "stats": {"inactive_file": 46137344}}
	}`

	fake := &fakeDockerClient{stats: map[string]string{"aaaaaaaaaaaa01": sample}}
	runner := newDockerRunnerWithClient(t.Context(), fake)
	b := NewInMemoryBackend("123456789012", "us-east-1", runner)

	s := newFargateServiceWithLB(t, b, "svc-docker-usage")
	require.NoError(t, b.StartTaskForService(s.cluster, s.service, s.tdArn))

	tasks, _, err := b.DescribeTasks(s.cluster, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	cpu, mem, ok := runner.TaskUsage(t.Context(), tasks[0].TaskArn)
	require.True(t, ok)
	assert.InDelta(t, 409.6, cpu, 0.001)
	assert.InDelta(t, 256*bytesPerMiB, mem, 1)
}
