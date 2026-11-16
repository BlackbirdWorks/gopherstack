package gcpspanner

import (
	"slices"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

const (
	maxScaleFactor   = 10
	minCPUTarget     = 10
	maxCPUTarget     = 90
	maxStorageTarget = 99
)

type capacity struct {
	nodes, units int32
}

// limitCapacity reads min or max of a limits message; ok is false unless exactly one unit is set.
func limitCapacity(l *instancepb.AutoscalingConfig_AutoscalingLimits, upper bool) (capacity, bool) {
	var nodes, units int32

	if upper {
		nodes, units = l.GetMaxNodes(), l.GetMaxProcessingUnits()
	} else {
		nodes, units = l.GetMinNodes(), l.GetMinProcessingUnits()
	}

	switch {
	case nodes != 0 && units == 0:
		return capacity{nodes: nodes}, true
	case units != 0 && nodes == 0:
		return capacity{units: units}, true
	default:
		return capacity{}, false
	}
}

func (c capacity) pu() int32 {
	if c.nodes != 0 {
		return c.nodes * puPerNode
	}

	return c.units
}

func validateLimits(l *instancepb.AutoscalingConfig_AutoscalingLimits) error {
	lo, loOK := limitCapacity(l, false)
	hi, hiOK := limitCapacity(l, true)

	if !loOK || !hiOK {
		return errInvalid("autoscaling_limits must set exactly one minimum and one maximum.")
	}

	if (lo.nodes != 0) != (hi.nodes != 0) {
		return errInvalid("autoscaling_limits minimum and maximum must use the same unit.")
	}

	if lo.nodes < 0 || lo.units < 0 || hi.nodes < 0 || hi.units < 0 {
		return errInvalid("autoscaling_limits must not be negative.")
	}

	if lo.units != 0 && (lo.units%puPerNode != 0 || hi.units%puPerNode != 0) {
		return errInvalid("autoscaling_limits in processing units must be multiples of 1000.")
	}

	if hi.pu() < lo.pu() {
		return errInvalid("autoscaling_limits maximum must be greater than or equal to the minimum.")
	}

	if hi.pu() > lo.pu()*maxScaleFactor {
		return errInvalid("autoscaling_limits maximum must be at most %dx the minimum.", maxScaleFactor)
	}

	if hi.pu() > maxProcessingUnit {
		return errInvalid("autoscaling_limits maximum is out of range.")
	}

	return nil
}

func validateCPUTarget(name string, v int32) error {
	if v != 0 && (v < minCPUTarget || v > maxCPUTarget) {
		return errInvalid("%s must be 0 or within [%d, %d].", name, minCPUTarget, maxCPUTarget)
	}

	return nil
}

func validateTargets(t *instancepb.AutoscalingConfig_AutoscalingTargets) error {
	if t == nil {
		return errInvalid("autoscaling_targets is required.")
	}

	if s := t.GetStorageUtilizationPercent(); s < minCPUTarget || s > maxStorageTarget {
		return errInvalid("storage_utilization_percent must be within [%d, %d].", minCPUTarget, maxStorageTarget)
	}

	if err := validateCPUTarget(
		"high_priority_cpu_utilization_percent",
		t.GetHighPriorityCpuUtilizationPercent(),
	); err != nil {
		return err
	}

	return validateCPUTarget("total_cpu_utilization_percent", t.GetTotalCpuUtilizationPercent())
}

func readOnlyLocations(cfg *instancepb.InstanceConfig) []string {
	var out []string

	for _, r := range cfg.GetReplicas() {
		if r.GetType() == instancepb.ReplicaInfo_READ_ONLY && !slices.Contains(out, r.GetLocation()) {
			out = append(out, r.GetLocation())
		}
	}

	return out
}

func validateAsymmetric(
	opts []*instancepb.AutoscalingConfig_AsymmetricAutoscalingOption, cfg *instancepb.InstanceConfig,
) error {
	seen := map[string]bool{}
	readOnly := readOnlyLocations(cfg)

	for _, o := range opts {
		loc := o.GetReplicaSelection().GetLocation()
		if loc == "" {
			return errInvalid("asymmetric_autoscaling_options.replica_selection.location is required.")
		}

		if !slices.Contains(readOnly, loc) {
			return errInvalid("Replica selection %q is not a read-only replica of %s.", loc, cfg.GetName())
		}

		if seen[loc] {
			return errInvalid("Replica selections must not overlap: %q is selected twice.", loc)
		}

		seen[loc] = true

		if err := validateOverrides(o.GetOverrides()); err != nil {
			return err
		}
	}

	return nil
}

func validateOverrides(o *instancepb.AutoscalingConfig_AsymmetricAutoscalingOption_AutoscalingConfigOverrides) error {
	if o == nil {
		return nil
	}

	if o.GetAutoscalingLimits() != nil {
		if err := validateLimits(o.GetAutoscalingLimits()); err != nil {
			return err
		}
	}

	if err := validateCPUTarget("autoscaling_target_high_priority_cpu_utilization_percent",
		o.GetAutoscalingTargetHighPriorityCpuUtilizationPercent()); err != nil {
		return err
	}

	if err := validateCPUTarget("autoscaling_target_total_cpu_utilization_percent",
		o.GetAutoscalingTargetTotalCpuUtilizationPercent()); err != nil {
		return err
	}

	if o.GetDisableHighPriorityCpuAutoscaling() && o.GetDisableTotalCpuAutoscaling() {
		return errInvalid("Disabling both high priority and total CPU autoscaling for a replica is not supported.")
	}

	if o.GetDisableHighPriorityCpuAutoscaling() && o.GetAutoscalingTargetHighPriorityCpuUtilizationPercent() != 0 {
		return errInvalid("A high priority CPU target cannot be set while high priority CPU autoscaling is disabled.")
	}

	return nil
}

// validateAutoscaling checks an autoscaling_config against Instance.autoscaling_config and its message docs.
func validateAutoscaling(a *instancepb.AutoscalingConfig, cfg *instancepb.InstanceConfig) error {
	if a == nil {
		return nil
	}

	if a.GetAutoscalingLimits() == nil {
		return errInvalid("autoscaling_limits is required.")
	}

	if err := validateLimits(a.GetAutoscalingLimits()); err != nil {
		return err
	}

	if err := validateTargets(a.GetAutoscalingTargets()); err != nil {
		return err
	}

	return validateAsymmetric(a.GetAsymmetricAutoscalingOptions(), cfg)
}

// autoscaleMin is the capacity an autoscaled resource starts at: its minimum limit.
func autoscaleMin(a *instancepb.AutoscalingConfig) capacity {
	c, _ := limitCapacity(a.GetAutoscalingLimits(), false)

	return c
}

func (c capacity) clamp(a *instancepb.AutoscalingConfig) capacity {
	lo, _ := limitCapacity(a.GetAutoscalingLimits(), false)
	hi, _ := limitCapacity(a.GetAutoscalingLimits(), true)
	pu := min(max(c.pu(), lo.pu()), hi.pu())

	if lo.nodes != 0 {
		return capacity{nodes: pu / puPerNode}
	}

	return capacity{units: pu}
}

// setInstanceCapacity writes c to node_count and processing_units.
func setInstanceCapacity(pb *instancepb.Instance, c capacity) {
	pu := c.pu()
	pb.ProcessingUnits = pu
	pb.NodeCount = pu / puPerNode
}

// replicaCapacity fills the output-only replica_compute_capacity: one entry per read-write and read-only replica
// location, in the unit the instance was sized with.
func replicaCapacity(pb *instancepb.Instance, cfg *instancepb.InstanceConfig) {
	pb.ReplicaComputeCapacity = nil

	var locs []string

	for _, r := range cfg.GetReplicas() {
		if r.GetType() != instancepb.ReplicaInfo_WITNESS && !slices.Contains(locs, r.GetLocation()) {
			locs = append(locs, r.GetLocation())
		}
	}

	byLoc := map[string]capacity{}

	for _, o := range pb.GetAutoscalingConfig().GetAsymmetricAutoscalingOptions() {
		if l := o.GetOverrides().GetAutoscalingLimits(); l != nil {
			byLoc[o.GetReplicaSelection().GetLocation()] = autoscaleMin(
				&instancepb.AutoscalingConfig{AutoscalingLimits: l},
			)
		}
	}

	base := capacity{units: pb.GetProcessingUnits()}
	if pb.GetProcessingUnits()%puPerNode == 0 {
		base = capacity{nodes: pb.GetNodeCount()}
	}

	for _, loc := range locs {
		c, ok := byLoc[loc]
		if !ok {
			c = base
		}

		rc := &instancepb.ReplicaComputeCapacity{ReplicaSelection: &instancepb.ReplicaSelection{Location: loc}}

		if c.nodes != 0 {
			rc.ComputeCapacity = &instancepb.ReplicaComputeCapacity_NodeCount{NodeCount: c.nodes}
		} else {
			rc.ComputeCapacity = &instancepb.ReplicaComputeCapacity_ProcessingUnits{ProcessingUnits: c.units}
		}

		pb.ReplicaComputeCapacity = append(pb.ReplicaComputeCapacity, rc)
	}
}
