package autoscaling

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ELBTarget identifies a single ELBv2 target to register or deregister. ID is
// the EC2 instance ID. Port is left zero so the target group's own
// configured port applies, matching how ELBv2 RegisterTargets/DeregisterTargets
// default an omitted target port from the target group itself.
type ELBTarget struct {
	ID   string
	Port int
}

// ELBv2TargetRegistrar lets the Auto Scaling backend register/deregister real
// ELBv2 targets as group membership and TargetGroupARNs change, so ELBv2
// DescribeTargetHealth reflects ASG-managed instances instead of
// TargetGroupARNs/LoadBalancerNames being stored and echoed with no effect.
// When no registrar is configured (the default), behavior is unchanged from
// before this feature existed.
type ELBv2TargetRegistrar interface {
	// RegisterTargets registers targets with the given target group.
	RegisterTargets(ctx context.Context, targetGroupARN string, targets []ELBTarget) error
	// DeregisterTargets deregisters targets from the given target group.
	DeregisterTargets(ctx context.Context, targetGroupARN string, targets []ELBTarget) error
}

// SetELBv2Registrar wires an ELBv2TargetRegistrar so subsequent instance
// launches/terminations and TargetGroupARNs attach/detach calls register or
// deregister real ELBv2 targets. Passing nil restores the historical,
// registration-less behavior. Intended to be called once during service
// wiring, before the backend serves traffic.
func (b *InMemoryBackend) SetELBv2Registrar(r ELBv2TargetRegistrar) {
	b.mu.Lock("SetELBv2Registrar")
	defer b.mu.Unlock()
	b.elbv2Registrar = r
}

// registerELBTargets registers each of instanceIDs as a target of every
// target group ARN in tgARNs. No-op when no registrar is wired or either
// slice is empty. Best-effort: errors are logged, not propagated, mirroring
// terminateInEC2's failure handling — by the time this runs the ASG-side
// membership change has already been applied, so there is nothing to roll
// back. Must be called with b.mu held (write lock).
func (b *InMemoryBackend) registerELBTargets(instanceIDs, tgARNs []string) {
	if b.elbv2Registrar == nil || len(instanceIDs) == 0 || len(tgARNs) == 0 {
		return
	}

	targets := elbTargetsFromInstanceIDs(instanceIDs)

	for _, tgArn := range tgARNs {
		if err := b.elbv2Registrar.RegisterTargets(context.Background(), tgArn, targets); err != nil {
			logger.Load(context.Background()).Error(
				"autoscaling: ELBv2 RegisterTargets failed",
				"error", err, "targetGroupArn", tgArn, "instanceIDs", instanceIDs)
		}
	}
}

// deregisterELBTargets deregisters each of instanceIDs from every target
// group ARN in tgARNs. No-op when no registrar is wired or either slice is
// empty. Best-effort: errors are logged, not propagated. Must be called with
// b.mu held (write lock).
func (b *InMemoryBackend) deregisterELBTargets(instanceIDs, tgARNs []string) {
	if b.elbv2Registrar == nil || len(instanceIDs) == 0 || len(tgARNs) == 0 {
		return
	}

	targets := elbTargetsFromInstanceIDs(instanceIDs)

	for _, tgArn := range tgARNs {
		if err := b.elbv2Registrar.DeregisterTargets(context.Background(), tgArn, targets); err != nil {
			logger.Load(context.Background()).Error(
				"autoscaling: ELBv2 DeregisterTargets failed",
				"error", err, "targetGroupArn", tgArn, "instanceIDs", instanceIDs)
		}
	}
}

// elbTargetsFromInstanceIDs converts a slice of EC2 instance IDs into the
// ELBTarget shape RegisterTargets/DeregisterTargets expect.
func elbTargetsFromInstanceIDs(instanceIDs []string) []ELBTarget {
	targets := make([]ELBTarget, len(instanceIDs))
	for i, id := range instanceIDs {
		targets[i] = ELBTarget{ID: id}
	}

	return targets
}

// instanceIDsOf extracts the InstanceID of each Instance in instances.
func instanceIDsOf(instances []Instance) []string {
	ids := make([]string, len(instances))
	for i, inst := range instances {
		ids[i] = inst.InstanceID
	}

	return ids
}
