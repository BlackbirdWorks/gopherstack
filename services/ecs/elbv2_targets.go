package ecs

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ELBTarget identifies a single ELBv2 target to register or deregister. ID is
// the task's awsvpc ENI private IP address (ECS registers "ip" target-type
// targets for awsvpc-mode tasks); Port is the container port the target
// group forwards to, from the owning service's LoadBalancer entry.
type ELBTarget struct {
	ID   string
	Port int
}

// ELBv2TargetRegistrar lets the ECS backend register/deregister real ELBv2
// targets as tasks belonging to a service with LoadBalancers reach or leave
// RUNNING, so ELBv2 DescribeTargetHealth reflects ECS-managed tasks instead
// of Service.LoadBalancers being stored and echoed with no effect. When no
// registrar is configured (the default), behavior is unchanged from before
// this feature existed.
type ELBv2TargetRegistrar interface {
	// RegisterTargets registers targets with the given target group.
	RegisterTargets(ctx context.Context, targetGroupARN string, targets []ELBTarget) error
	// DeregisterTargets deregisters targets from the given target group.
	DeregisterTargets(ctx context.Context, targetGroupARN string, targets []ELBTarget) error
}

// SetELBv2Registrar wires an ELBv2TargetRegistrar so subsequent task
// start/stop transitions register or deregister real ELBv2 targets for
// services with LoadBalancers configured. Passing nil restores the
// historical, registration-less behavior. Intended to be called once during
// service wiring, before the backend serves traffic.
func (b *InMemoryBackend) SetELBv2Registrar(r ELBv2TargetRegistrar) {
	b.mu.Lock("SetELBv2Registrar")
	defer b.mu.Unlock()
	b.elbv2Registrar = r
}

// registerTaskWithELBv2Locked registers task as an ELBv2 target of each
// LoadBalancer entry on its owning service, if any. No-op when no registrar
// is wired, the task has no resolvable awsvpc private IP (see
// taskPrivateIPLocked), or the task's owning service has no LoadBalancers
// configured. Best-effort: errors are logged, not propagated, mirroring
// terminateInEC2's failure handling in the autoscaling package. Must be
// called with b.mu held (write lock).
func (b *InMemoryBackend) registerTaskWithELBv2Locked(task *Task, clusterName string) {
	if b.elbv2Registrar == nil {
		return
	}

	ip, lbs, ok := b.taskELBTargetsLocked(task, clusterName)
	if !ok {
		return
	}

	for _, lb := range lbs {
		if lb.TargetGroupArn == "" {
			continue
		}

		target := []ELBTarget{{ID: ip, Port: lb.ContainerPort}}
		if err := b.elbv2Registrar.RegisterTargets(context.Background(), lb.TargetGroupArn, target); err != nil {
			logger.Load(context.Background()).ErrorContext(
				context.Background(),
				"ecs: ELBv2 RegisterTargets failed",
				"error", err, "targetGroupArn", lb.TargetGroupArn, "taskArn", task.TaskArn)
		}
	}
}

// deregisterTaskFromELBv2Locked deregisters task from each LoadBalancer entry
// on its owning service, if any. No-op when no registrar is wired, the task
// has no resolvable awsvpc private IP, or the task's owning service has no
// LoadBalancers configured. Best-effort: errors are logged, not propagated.
// Safe to call even if the task was never actually registered (deregistering
// an unregistered target is a no-op on the ELBv2 side). Must be called with
// b.mu held (write lock).
func (b *InMemoryBackend) deregisterTaskFromELBv2Locked(task *Task, clusterName string) {
	if b.elbv2Registrar == nil {
		return
	}

	ip, lbs, ok := b.taskELBTargetsLocked(task, clusterName)
	if !ok {
		return
	}

	for _, lb := range lbs {
		if lb.TargetGroupArn == "" {
			continue
		}

		target := []ELBTarget{{ID: ip, Port: lb.ContainerPort}}
		if err := b.elbv2Registrar.DeregisterTargets(context.Background(), lb.TargetGroupArn, target); err != nil {
			logger.Load(context.Background()).ErrorContext(
				context.Background(),
				"ecs: ELBv2 DeregisterTargets failed",
				"error", err, "targetGroupArn", lb.TargetGroupArn, "taskArn", task.TaskArn)
		}
	}
}

// taskELBTargetsLocked resolves task's awsvpc private IP and its owning
// service's LoadBalancers. It reports ok=false when task has no resolvable
// private IP (non-Fargate tasks in this backend have no ENI attachment — see
// newFargateTaskAttachment) or its owning service has no LoadBalancers
// configured, in which case the caller has nothing to register/deregister.
// Must be called with at least the read lock held.
func (b *InMemoryBackend) taskELBTargetsLocked(task *Task, clusterName string) (string, []LoadBalancer, bool) {
	ip, ok := privateIPFromAttachments(task.Attachments)
	if !ok {
		return "", nil, false
	}

	svc := b.serviceForTaskLocked(task, clusterName)
	if svc == nil || len(svc.LoadBalancers) == 0 {
		return "", nil, false
	}

	return ip, svc.LoadBalancers, true
}

// serviceForTaskLocked resolves the Service owning task, based on task.Group
// (set to "service:<serviceName>" for service-managed tasks — see
// createTaskEntriesLocked/StartTask's callers). Returns nil for standalone
// tasks (task.Group has no "service:" prefix) or if the service no longer
// exists. Must be called with at least the read lock held.
func (b *InMemoryBackend) serviceForTaskLocked(task *Task, clusterName string) *Service {
	const servicePrefix = "service:"

	if !strings.HasPrefix(task.Group, servicePrefix) {
		return nil
	}

	name := strings.TrimPrefix(task.Group, servicePrefix)
	svc, _ := b.services.Get(scopedKey(clusterName, serviceKey(name)))

	return svc
}

// privateIPFromAttachments returns the privateIPv4Address detail of task's
// ElasticNetworkInterface attachment, if any (Fargate/awsvpc tasks only —
// see newFargateTaskAttachment). EC2-launch-type tasks in this backend have
// no ENI modeling, so ok is false for them: there is no usable ELBv2 target
// identity to register (their LoadBalancers wiring would need bridge-mode
// dynamic host-port tracking on the container instance, which this backend
// does not model).
func privateIPFromAttachments(attachments []TaskAttachment) (string, bool) {
	for _, a := range attachments {
		if a.Type != "ElasticNetworkInterface" {
			continue
		}

		for _, d := range a.Details {
			if d.Name == "privateIPv4Address" && d.Value != "" {
				return d.Value, true
			}
		}
	}

	return "", false
}
