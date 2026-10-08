package ecs

import (
	"context"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const (
	ecsMetricNamespace = "AWS/ECS"
	bytesPerMiB        = 1024.0 * 1024.0
	percent            = 100.0
)

// TaskUsageReader is implemented by runners that can sample a task's live resource use.
type TaskUsageReader interface {
	// TaskUsage returns the CPU (1024 units per vCPU) and memory bytes in use across the task's containers.
	TaskUsage(ctx context.Context, taskArn string) (cpuUnits, memoryBytes float64, ok bool)
}

// SetMetricEmitter sets the emitter that publishes AWS/ECS service utilization metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

type serviceUsage struct {
	cluster, service           string
	cpuUsed, cpuReserved       float64
	memUsedMiB, memReservedMiB float64
}

type usageTarget struct {
	taskArn, cluster, service   string
	cpuReserved, memReservedMiB float64
}

// emitUtilizationMetrics publishes CPUUtilization and MemoryUtilization per service from the runner's live
// container stats: used divided by the units reserved by the task definition.
func (b *InMemoryBackend) emitUtilizationMetrics(ctx context.Context) {
	reader, ok := b.runner.(TaskUsageReader)
	if !ok || !b.metrics.Enabled() {
		return
	}

	perService := map[[2]string]*serviceUsage{}

	for _, t := range b.usageTargets() {
		cpu, mem, sampled := reader.TaskUsage(ctx, t.taskArn)
		if !sampled {
			continue
		}

		key := [2]string{t.cluster, t.service}

		su := perService[key]
		if su == nil {
			su = &serviceUsage{cluster: t.cluster, service: t.service}
			perService[key] = su
		}

		su.cpuUsed += cpu
		su.cpuReserved += t.cpuReserved
		su.memUsedMiB += mem / bytesPerMiB
		su.memReservedMiB += t.memReservedMiB
	}

	for _, su := range perService {
		dims := []cwmetric.Dimension{
			{Name: "ClusterName", Value: su.cluster}, {Name: "ServiceName", Value: su.service},
		}

		if su.cpuReserved > 0 {
			b.metrics.Put(
				b.region,
				ecsMetricNamespace,
				"CPUUtilization",
				"Percent",
				su.cpuUsed/su.cpuReserved*percent,
				dims...)
		}

		if su.memReservedMiB > 0 {
			b.metrics.Put(
				b.region,
				ecsMetricNamespace,
				"MemoryUtilization",
				"Percent",
				su.memUsedMiB/su.memReservedMiB*percent,
				dims...,
			)
		}
	}
}

// usageTargets lists RUNNING service tasks with their reserved CPU and memory.
func (b *InMemoryBackend) usageTargets() []usageTarget {
	b.mu.RLock("UsageTargets")
	defer b.mu.RUnlock()

	var out []usageTarget

	for _, t := range b.tasks.All() {
		svcName, isService := strings.CutPrefix(t.Group, "service:")
		if !isService || t.LastStatus != statusRunning {
			continue
		}

		td, err := b.findTaskDefinitionLocked(t.TaskDefinitionArn)
		if err != nil {
			continue
		}

		cpu, mem := taskReservation(t, td)
		out = append(out, usageTarget{
			taskArn: t.TaskArn, cluster: clusterKey(t.ClusterArn), service: svcName,
			cpuReserved: cpu, memReservedMiB: mem,
		})
	}

	return out
}

// taskReservation returns the task's reserved CPU units and memory MiB: task-level values (overrides first),
// else the sum over its containers.
func taskReservation(t *Task, td *TaskDefinition) (float64, float64) {
	cpuStr, memStr := td.CPU, td.Memory
	if t.Overrides != nil {
		cpuStr = firstNonEmpty(t.Overrides.CPU, cpuStr)
		memStr = firstNonEmpty(t.Overrides.Memory, memStr)
	}

	cpuUnits, _ := strconv.ParseFloat(cpuStr, 64)
	memMiB, _ := strconv.ParseFloat(memStr, 64)

	if cpuUnits == 0 || memMiB == 0 {
		var cpuSum, memSum float64

		for _, cd := range td.ContainerDefinitions {
			cpuSum += float64(cd.CPU)
			memSum += float64(max(cd.Memory, cd.MemoryReservation))
		}

		if cpuUnits == 0 {
			cpuUnits = cpuSum
		}

		if memMiB == 0 {
			memMiB = memSum
		}
	}

	return cpuUnits, memMiB
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

// sampleUtilization runs emitUtilizationMetrics in the background, skipping a pass while one is in flight.
func (r *Reconciler) sampleUtilization(ctx context.Context) {
	if !r.backend.metrics.Enabled() || !r.sampling.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.sampling.Store(false)

		r.backend.emitUtilizationMetrics(ctx)
	}()
}

// SetMetricEmitter installs e on the home backend and every region sibling built so far.
func (h *Handler) SetMetricEmitter(e cwmetric.Emitter) {
	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			mem.SetMetricEmitter(e)
		}
	}
}
