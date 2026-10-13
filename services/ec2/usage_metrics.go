package ec2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const (
	ec2UnitPercent = "Percent"
	ec2UnitBytes   = "Bytes"
	percentScale   = 100
)

// InstanceUsage holds the cumulative resource counters of a running instance.
type InstanceUsage struct {
	CPUNanos       uint64
	SystemNanos    uint64
	NetRxBytes     uint64
	NetTxBytes     uint64
	NetRxPackets   uint64
	NetTxPackets   uint64
	DiskReadBytes  uint64
	DiskWriteBytes uint64
	DiskReadOps    uint64
	DiskWriteOps   uint64
}

// UsageReporter is the optional Compute capability that reports cumulative counters for AWS/EC2 utilisation metrics.
type UsageReporter interface {
	Usage(ctx context.Context, providerID string) (InstanceUsage, error)
}

// EmitUsageMetrics publishes CPUUtilization, NetworkIn/Out, NetworkPacketsIn/Out and Disk* for every running
// instance whose Compute reports usage. Each value is the change since the previous call; the first call for an
// instance only records the baseline.
func (b *InMemoryBackend) EmitUsageMetrics(ctx context.Context) {
	reporter, ok := b.compute.(UsageReporter)
	if !ok || !b.metrics.Enabled() {
		return
	}

	b.mu.RLock("EmitUsageMetrics")
	targets := map[string]string{}

	for _, inst := range b.instances.All() {
		if inst.State == StateRunning && inst.ProviderID != "" {
			targets[inst.ID] = inst.ProviderID
		}
	}
	b.mu.RUnlock()

	b.usageMu.Lock("EmitUsageMetrics")
	defer b.usageMu.Unlock()

	if b.lastUsage == nil {
		b.lastUsage = map[string]InstanceUsage{}
	}

	for id := range b.lastUsage {
		if _, running := targets[id]; !running {
			delete(b.lastUsage, id)
		}
	}

	for id, providerID := range targets {
		cur, err := reporter.Usage(ctx, providerID)
		if err != nil {
			continue
		}

		if prev, seen := b.lastUsage[id]; seen {
			b.putUsageDelta(id, prev, cur)
		}

		b.lastUsage[id] = cur
	}
}

func (b *InMemoryBackend) putUsageDelta(id string, prev, cur InstanceUsage) {
	dim := cwmetric.Dimension{Name: "InstanceId", Value: id}

	if sys := cur.SystemNanos - prev.SystemNanos; cur.SystemNanos > prev.SystemNanos && cur.CPUNanos >= prev.CPUNanos {
		cpu := float64(cur.CPUNanos-prev.CPUNanos) / float64(sys) * percentScale
		b.metrics.Put(b.Region, ec2MetricNamespace, "CPUUtilization", ec2UnitPercent, cpu, dim)
	}

	for _, m := range []struct {
		name, unit string
		prev, cur  uint64
	}{
		{"NetworkIn", ec2UnitBytes, prev.NetRxBytes, cur.NetRxBytes},
		{"NetworkOut", ec2UnitBytes, prev.NetTxBytes, cur.NetTxBytes},
		{"NetworkPacketsIn", ec2UnitCount, prev.NetRxPackets, cur.NetRxPackets},
		{"NetworkPacketsOut", ec2UnitCount, prev.NetTxPackets, cur.NetTxPackets},
		{"DiskReadBytes", ec2UnitBytes, prev.DiskReadBytes, cur.DiskReadBytes},
		{"DiskWriteBytes", ec2UnitBytes, prev.DiskWriteBytes, cur.DiskWriteBytes},
		{"DiskReadOps", ec2UnitCount, prev.DiskReadOps, cur.DiskReadOps},
		{"DiskWriteOps", ec2UnitCount, prev.DiskWriteOps, cur.DiskWriteOps},
	} {
		if m.cur >= m.prev {
			b.metrics.Put(b.Region, ec2MetricNamespace, m.name, m.unit, float64(m.cur-m.prev), dim)
		}
	}
}

// Usage reads the container's cumulative CPU, network and block-IO counters from the Docker daemon.
func (d *DockerCompute) Usage(ctx context.Context, providerID string) (InstanceUsage, error) {
	res, err := d.api.ContainerStats(ctx, providerID, mobyclient.ContainerStatsOptions{})
	if err != nil {
		return InstanceUsage{}, fmt.Errorf("container stats: %w", err)
	}

	defer func() { _ = res.Body.Close() }()

	var stats mobycontainer.StatsResponse
	if decodeErr := json.NewDecoder(res.Body).Decode(&stats); decodeErr != nil {
		return InstanceUsage{}, fmt.Errorf("decode container stats: %w", decodeErr)
	}

	return usageFromStats(&stats), nil
}

func usageFromStats(s *mobycontainer.StatsResponse) InstanceUsage {
	u := InstanceUsage{CPUNanos: s.CPUStats.CPUUsage.TotalUsage, SystemNanos: s.CPUStats.SystemUsage}

	for _, n := range s.Networks {
		u.NetRxBytes += n.RxBytes
		u.NetTxBytes += n.TxBytes
		u.NetRxPackets += n.RxPackets
		u.NetTxPackets += n.TxPackets
	}

	for _, e := range s.BlkioStats.IoServiceBytesRecursive {
		addBlkio(&u.DiskReadBytes, &u.DiskWriteBytes, e)
	}

	for _, e := range s.BlkioStats.IoServicedRecursive {
		addBlkio(&u.DiskReadOps, &u.DiskWriteOps, e)
	}

	return u
}

func addBlkio(read, write *uint64, e mobycontainer.BlkioStatEntry) {
	switch strings.ToLower(e.Op) {
	case "read":
		*read += e.Value
	case "write":
		*write += e.Value
	}
}
