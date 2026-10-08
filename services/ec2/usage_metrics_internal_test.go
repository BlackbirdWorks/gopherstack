package ec2

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// usageCompute is a stubCompute that also reports cumulative usage.
type usageCompute struct {
	stubCompute
	usage InstanceUsage
}

func (u *usageCompute) Usage(context.Context, string) (InstanceUsage, error) { return u.usage, nil }

type recordedMetrics struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (r *recordedMetrics) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = append(r.points, p)

	return nil
}

func (r *recordedMetrics) byName() map[string]cwmetric.Point {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := map[string]cwmetric.Point{}
	for _, p := range r.points {
		out[p.Name] = p
	}

	return out
}

func TestEmitUsageMetrics(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	c := &usageCompute{}
	c.result = LaunchResult{ProviderID: "ctr-1"}
	rec := &recordedMetrics{}

	b.WithCompute(c)
	b.SetMetricEmitter(rec)

	h := NewHandler(b)
	_, err := h.handleRunInstances(map[string][]string{"ImageId": {"ami-1"}, "MinCount": {"1"}}, "req")
	require.NoError(t, err)
	b.reconcileInstanceLifecycle()

	c.usage = InstanceUsage{
		CPUNanos: 100, SystemNanos: 1000, NetRxBytes: 10, NetTxBytes: 20,
		NetRxPackets: 1, NetTxPackets: 2, DiskReadBytes: 5, DiskWriteBytes: 6, DiskReadOps: 1, DiskWriteOps: 2,
	}

	b.EmitUsageMetrics(t.Context())
	assert.Empty(t, rec.byName(), "the first sample only records the baseline")

	c.usage = InstanceUsage{
		CPUNanos: 150, SystemNanos: 1200, NetRxBytes: 110, NetTxBytes: 70,
		NetRxPackets: 4, NetTxPackets: 5, DiskReadBytes: 105, DiskWriteBytes: 56, DiskReadOps: 3, DiskWriteOps: 7,
	}

	b.EmitUsageMetrics(t.Context())

	got := rec.byName()
	assert.InDelta(t, 25.0, got["CPUUtilization"].Value, 1e-9)
	assert.Equal(t, "Percent", got["CPUUtilization"].Unit)
	assert.InDelta(t, 100, got["NetworkIn"].Value, 0)
	assert.InDelta(t, 50, got["NetworkOut"].Value, 0)
	assert.InDelta(t, 3, got["NetworkPacketsIn"].Value, 0)
	assert.InDelta(t, 3, got["NetworkPacketsOut"].Value, 0)
	assert.InDelta(t, 100, got["DiskReadBytes"].Value, 0)
	assert.InDelta(t, 50, got["DiskWriteBytes"].Value, 0)
	assert.InDelta(t, 2, got["DiskReadOps"].Value, 0)
	assert.InDelta(t, 5, got["DiskWriteOps"].Value, 0)
	assert.Equal(t, "InstanceId", got["NetworkIn"].Dimensions[0].Name)
}

func TestDockerComputeUsage(t *testing.T) {
	t.Parallel()

	api := &fakeDockerAPI{statsJSON: `{
		"cpu_stats": {"cpu_usage": {"total_usage": 500}, "system_cpu_usage": 9000},
		"networks": {"eth0": {"rx_bytes": 7, "tx_bytes": 8, "rx_packets": 1, "tx_packets": 2}},
		"blkio_stats": {
			"io_service_bytes_recursive": [
				{"op": "Read", "value": 11}, {"op": "Write", "value": 12}, {"op": "Total", "value": 23}
			],
			"io_serviced_recursive": [{"op": "Read", "value": 3}, {"op": "Write", "value": 4}]
		}
	}`}
	dc := newDockerComputeWithAPI(api, DockerComputeConfig{})

	got, err := dc.Usage(t.Context(), "ctr")
	require.NoError(t, err)
	assert.Equal(t, InstanceUsage{
		CPUNanos: 500, SystemNanos: 9000, NetRxBytes: 7, NetTxBytes: 8, NetRxPackets: 1, NetTxPackets: 2,
		DiskReadBytes: 11, DiskWriteBytes: 12, DiskReadOps: 3, DiskWriteOps: 4,
	}, got)

	api.statsErr = errFakeBoom
	_, err = dc.Usage(t.Context(), "ctr")
	require.ErrorIs(t, err, errFakeBoom)
}
