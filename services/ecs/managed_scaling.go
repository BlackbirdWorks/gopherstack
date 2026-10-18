package ecs

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	managedScalingNamespace = "AWS/ECS/ManagedScaling"
	reservationMetric       = "CapacityProviderReservation"
	reservationIdle         = 100.0
	reservationNoCapacity   = 200.0
	percentOf               = 100
)

// ASGInfo is the slice of an Auto Scaling group managed scaling needs.
type ASGInfo struct {
	Name                 string
	InstanceType         string
	InServiceInstanceIDs []string
	Min, Max, Desired    int
}

// AutoScalingGroupController reads and drives the Auto Scaling group behind a capacity provider.
type AutoScalingGroupController interface {
	AutoScalingGroupResolver
	DescribeGroup(arnOrName string) (ASGInfo, bool)
	SetDesiredCapacity(arnOrName string, desired int) error
	SetInstanceProtection(arnOrName string, instanceIDs []string, protected bool) error
}

// SetAutoScalingGroupController wires the Auto Scaling backend: it validates provider ASGs and lets managed
// scaling observe and resize them.
func (b *InMemoryBackend) SetAutoScalingGroupController(c AutoScalingGroupController) {
	b.mu.Lock("SetAutoScalingGroupController")
	defer b.mu.Unlock()

	b.asgResolver = c
	b.asgController = c
}

// defaultLaunchType is Fargate unless the strategy names a non-Fargate (EC2 Auto Scaling) provider.
func defaultLaunchType(strategy []CapacityProviderStrategyItem) string {
	if len(strategy) > 0 {
		if p := strategy[0].CapacityProvider; p != launchTypeFargate && p != "FARGATE_SPOT" {
			return launchTypeEC2
		}
	}

	return launchTypeFargate
}

func managedScalingOn(p *AutoScalingGroupProvider) bool {
	return p != nil && p.ManagedScaling != nil && p.ManagedScaling.Status == statusEnabled
}

// managedScalingEnabledLocked reports whether the named provider scales its group (tasks then wait for capacity).
func (b *InMemoryBackend) managedScalingEnabledLocked(name string) bool {
	if name == "" || b.asgController == nil {
		return false
	}

	_, cp := b.findCapacityProviderLocked(name)

	return cp != nil && managedScalingOn(cp.AutoScalingGroupProvider)
}

// providerSkipLocked pre-skips instances outside the provider's Auto Scaling group.
func (b *InMemoryBackend) providerSkipLocked(clusterName, providerName string) map[string]bool {
	skip := map[string]bool{}

	if providerName == "" || b.asgController == nil {
		return skip
	}

	_, cp := b.findCapacityProviderLocked(providerName)
	if cp == nil || cp.AutoScalingGroupProvider == nil || cp.AutoScalingGroupProvider.AutoScalingGroupArn == "" {
		return skip
	}

	info, ok := b.asgController.DescribeGroup(cp.AutoScalingGroupProvider.AutoScalingGroupArn)
	if !ok {
		return skip
	}

	for _, ci := range b.containerInstancesByCluster.Get(clusterName) {
		if !slices.Contains(info.InServiceInstanceIDs, ci.EC2InstanceID) {
			skip[ci.ContainerInstanceArn] = true
		}
	}

	return skip
}

type scalingTarget struct {
	cluster  string
	provider CapacityProvider
}

// scalingTargets lists (cluster, provider) pairs with managed scaling ENABLED.
func (b *InMemoryBackend) scalingTargets() []scalingTarget {
	b.mu.RLock("ManagedScalingTargets")
	defer b.mu.RUnlock()

	var out []scalingTarget

	for _, c := range b.clusters.All() {
		for _, name := range c.CapacityProviders {
			_, cp := b.findCapacityProviderLocked(name)
			if cp != nil && managedScalingOn(cp.AutoScalingGroupProvider) {
				out = append(out, scalingTarget{cluster: c.ClusterName, provider: *cp})
			}
		}
	}

	return out
}

// ReconcileCapacity places tasks waiting for capacity and runs one managed-scaling pass per provider.
func (b *InMemoryBackend) ReconcileCapacity(ctx context.Context) {
	b.placeAwaitingTasks()

	b.mu.RLock("ReconcileCapacity")
	ctl := b.asgController
	b.mu.RUnlock()

	if ctl == nil {
		return
	}

	for _, t := range b.scalingTargets() {
		b.scaleProvider(ctx, ctl, t)
	}
}

func (b *InMemoryBackend) scaleProvider(ctx context.Context, ctl AutoScalingGroupController, t scalingTarget) {
	asg := t.provider.AutoScalingGroupProvider
	ms := asg.ManagedScaling

	info, ok := ctl.DescribeGroup(asg.AutoScalingGroupArn)
	if !ok {
		return
	}

	b.syncManagedInstances(t, info)

	demand := b.providerDemand(t, info)
	n := len(info.InServiceInstanceIDs)

	b.metrics.Put(b.region, managedScalingNamespace, reservationMetric, "Percent",
		reservation(demand.instances, n), cwmetric.Dimension{Name: "ClusterName", Value: t.cluster},
		cwmetric.Dimension{Name: "CapacityProviderName", Value: t.provider.Name})

	if asg.ManagedTerminationProtection == statusEnabled {
		b.protectBusyInstances(ctx, ctl, asg.AutoScalingGroupArn, info, demand.busyEC2)
	}

	want := targetInstances(demand.instances, ms.TargetCapacityPercent)
	if asg.ManagedTerminationProtection == statusEnabled {
		want = max(want, len(demand.busyEC2))
	}

	want = stepToward(info.Desired, want, ms.MinimumScalingStepSize, ms.MaximumScalingStepSize)
	want = min(max(want, info.Min), info.Max)

	if want == info.Desired {
		return
	}

	if err := ctl.SetDesiredCapacity(asg.AutoScalingGroupArn, want); err != nil {
		logger.Load(ctx).WarnContext(ctx, "ecs managed scaling: set desired capacity failed",
			"provider", t.provider.Name, "error", err)

		return
	}

	if next, found := ctl.DescribeGroup(asg.AutoScalingGroupArn); found {
		b.syncManagedInstances(t, next)
	}
}

// reservation is CapacityProviderReservation = M/N*100; 100 when idle and 200 with tasks but no instances.
func reservation(m, n int) float64 {
	switch {
	case n == 0 && m > 0:
		return reservationNoCapacity
	case n == 0:
		return reservationIdle
	default:
		return float64(m) / float64(n) * percentOf
	}
}

// targetInstances is the group size that puts the reservation at target percent: ceil(M*100/target).
func targetInstances(m, target int) int {
	if target <= 0 {
		target = percentOf
	}

	return (m*percentOf + target - 1) / target
}

// stepToward moves current toward want by at least minStep (scale-out may overshoot) and at most maxStep.
func stepToward(current, want, minStep, maxStep int) int {
	delta := want - current
	if delta == 0 {
		return current
	}

	size := min(max(abs(delta), max(minStep, 1)), max(maxStep, 1))
	if delta < 0 {
		return max(current-size, want)
	}

	return current + size
}

func abs(v int) int {
	if v < 0 {
		return -v
	}

	return v
}

type providerDemandResult struct {
	busyEC2   []string
	instances int
}

// providerDemand computes M: instances needed to hold the provider's live and waiting tasks (first-fit
// decreasing on CPU then memory), and the EC2 ids of managed instances currently running tasks.
func (b *InMemoryBackend) providerDemand(t scalingTarget, info ASGInfo) providerDemandResult {
	b.mu.RLock("ManagedScalingDemand")
	defer b.mu.RUnlock()

	binCPU, binMem := b.binCapacityLocked(t, info)

	var needs []taskNeed

	for _, task := range b.tasksByCluster.Get(t.cluster) {
		if task.CapacityProviderName != t.provider.Name || task.LastStatus == statusStopped {
			continue
		}

		td, err := b.findTaskDefinitionLocked(task.TaskDefinitionArn)
		if err != nil {
			continue
		}

		c, m := taskReservation(task, td)
		if int64(c) > binCPU || int64(m) > binMem {
			continue
		}

		needs = append(needs, taskNeed{int64(c), int64(m)})
	}

	res := providerDemandResult{instances: packBins(needs, binCPU, binMem)}

	for _, ci := range b.containerInstancesByCluster.Get(t.cluster) {
		if ci.ManagedBy == t.provider.Name && ci.Status != statusInactive && ci.EC2InstanceID != "" &&
			len(b.instanceTasksLocked(t.cluster, ci.ContainerInstanceArn)) > 0 {
			res.busyEC2 = append(res.busyEC2, ci.EC2InstanceID)
		}
	}

	return res
}

// binCapacityLocked is the size of one instance of this provider: the largest registered, else the group's type.
func (b *InMemoryBackend) binCapacityLocked(t scalingTarget, info ASGInfo) (int64, int64) {
	var cpu, mem int64

	for _, ci := range b.containerInstancesByCluster.Get(t.cluster) {
		if ci.ManagedBy != t.provider.Name {
			continue
		}

		c, _ := registeredInt(ci, resourceCPU)
		m, _ := registeredInt(ci, resourceMemory)
		cpu, mem = max(cpu, c), max(mem, m)
	}

	if cpu > 0 && mem > 0 {
		return cpu, mem
	}

	c, m := b.sizeForTypeLocked(info)

	return int64(c), int64(m)
}

func (b *InMemoryBackend) sizeForTypeLocked(info ASGInfo) (int, int) {
	if b.instanceResolver != nil {
		if c, m, ok := b.instanceResolver.InstanceTypeResources(info.InstanceType); ok {
			return c, m
		}

		if len(info.InServiceInstanceIDs) > 0 {
			if c, m, ok := b.instanceResolver.ContainerInstanceResources(info.InServiceInstanceIDs[0]); ok {
				return c, m
			}
		}
	}

	return defaultInstanceCPU, defaultInstanceMemMB
}

// syncManagedInstances registers in-service group instances as container instances (the emulated ECS agent)
// and retires managed ones that left the group, then places any tasks that were waiting.
func (b *InMemoryBackend) syncManagedInstances(t scalingTarget, info ASGInfo) {
	b.mu.Lock("ManagedScalingSync")

	registered := map[string]bool{}

	for _, ci := range b.containerInstancesByCluster.Get(t.cluster) {
		if ci.Status != statusInactive {
			registered[ci.EC2InstanceID] = true
		}
	}

	for _, id := range info.InServiceInstanceIDs {
		if registered[id] {
			continue
		}

		_ = b.registerManagedInstanceLocked(t, id)
	}

	for _, ci := range slices.Clone(b.containerInstancesByCluster.Get(t.cluster)) {
		if ci.ManagedBy == t.provider.Name && ci.Status != statusInactive &&
			!slices.Contains(info.InServiceInstanceIDs, ci.EC2InstanceID) &&
			len(b.instanceTasksLocked(t.cluster, ci.ContainerInstanceArn)) == 0 {
			ci.Status = statusInactive
			ci.InactiveAt = time.Now()
			ci.Version++
			b.emitContainerInstanceStateLocked(ci)
		}
	}

	b.mu.Unlock()

	b.placeAwaitingTasks()
}

func (b *InMemoryBackend) protectBusyInstances(
	ctx context.Context, ctl AutoScalingGroupController, group string, info ASGInfo, busy []string,
) {
	var idle []string

	for _, id := range info.InServiceInstanceIDs {
		if !slices.Contains(busy, id) {
			idle = append(idle, id)
		}
	}

	for _, set := range []struct {
		ids       []string
		protected bool
	}{{busy, true}, {idle, false}} {
		if len(set.ids) == 0 {
			continue
		}

		if err := ctl.SetInstanceProtection(group, set.ids, set.protected); err != nil {
			logger.Load(ctx).WarnContext(ctx, "ecs managed scaling: set instance protection failed", "error", err)
		}
	}
}

// placeAwaitingTasks retries placement for tasks waiting on capacity and starts those that now fit.
func (b *InMemoryBackend) placeAwaitingTasks() {
	var work []taskWork

	func() {
		b.mu.Lock("PlaceAwaitingTasks")
		defer b.mu.Unlock()

		for _, task := range b.tasks.All() {
			if !task.AwaitingCapacity || task.LastStatus != statusProvisioning || task.DesiredStatus == statusStopped {
				continue
			}

			td, err := b.findTaskDefinitionLocked(task.TaskDefinitionArn)
			if err != nil {
				continue
			}

			clusterName := clusterKey(task.ClusterArn)
			svc, _ := strings.CutPrefix(task.Group, "service:")

			bindings, failure := b.tryPlaceTaskLocked(clusterName, task, td, td.PlacementConstraints, nil, svc)
			if failure != nil {
				continue
			}

			task.AwaitingCapacity = false
			applyHostPortBindings(task, bindings)
			work = append(work, taskWork{task: task, td: td})
		}
	}()

	b.startTasksOutsideLock(work)
}

type taskNeed struct{ cpu, mem int64 }

// packBins counts the bins needed to hold needs (first-fit decreasing).
func packBins(needs []taskNeed, binCPU, binMem int64) int {
	slices.SortFunc(needs, func(x, y taskNeed) int { return int(y.cpu+y.mem) - int(x.cpu+x.mem) })

	var bins []taskNeed

	for _, nd := range needs {
		placed := false

		for i := range bins {
			if bins[i].cpu+nd.cpu <= binCPU && bins[i].mem+nd.mem <= binMem {
				bins[i].cpu += nd.cpu
				bins[i].mem += nd.mem
				placed = true

				break
			}
		}

		if !placed {
			bins = append(bins, nd)
		}
	}

	return len(bins)
}
