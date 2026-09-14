package ecs

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ELBTarget identifies a single ELBv2 target to register or deregister. For
// awsvpc-mode tasks, ID is the task's ENI private IP address and Port is the
// container port (ECS registers "ip" target-type targets for these,
// forwarding straight to the ENI -- the container and host port are the
// same address space). For bridge/host-mode EC2-launch-type tasks, ID is
// instead the container instance's EC2 instance ID and Port is the
// allocated host port (ECS registers "instance" target-type targets for
// these -- see host_ports.go and
// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/target-group-register-targets.html
// on target-group targetType "instance" taking an instance ID vs "ip" taking
// an IP address; services/elbv2 already models both).
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
// LoadBalancer entry on its owning service, if any, for which a target
// identity can be resolved (see resolveELBTargetLocked). No-op when no
// registrar is wired, or the task's owning service has no LoadBalancers
// configured. Best-effort: errors are logged, not propagated, mirroring
// terminateInEC2's failure handling in the autoscaling package. Must be
// called with b.mu held (write lock).
func (b *InMemoryBackend) registerTaskWithELBv2Locked(task *Task, clusterName string) {
	if b.elbv2Registrar == nil {
		return
	}

	targets, ok := b.taskELBTargetsLocked(task, clusterName)
	if !ok {
		return
	}

	for _, rt := range targets {
		if err := b.elbv2Registrar.RegisterTargets(
			context.Background(), rt.lb.TargetGroupArn, []ELBTarget{rt.target},
		); err != nil {
			logger.Load(context.Background()).ErrorContext(
				context.Background(),
				"ecs: ELBv2 RegisterTargets failed",
				"error", err, "targetGroupArn", rt.lb.TargetGroupArn, "taskArn", task.TaskArn)
		}
	}
}

// deregisterTaskFromELBv2Locked deregisters task from each LoadBalancer entry
// on its owning service for which a target identity can be resolved. No-op
// when no registrar is wired, or the task's owning service has no
// LoadBalancers configured. Best-effort: errors are logged, not propagated.
// Safe to call even if the task was never actually registered (deregistering
// an unregistered target is a no-op on the ELBv2 side). Must be called with
// b.mu held (write lock).
func (b *InMemoryBackend) deregisterTaskFromELBv2Locked(task *Task, clusterName string) {
	if b.elbv2Registrar == nil {
		return
	}

	targets, ok := b.taskELBTargetsLocked(task, clusterName)
	if !ok {
		return
	}

	for _, rt := range targets {
		if err := b.elbv2Registrar.DeregisterTargets(
			context.Background(), rt.lb.TargetGroupArn, []ELBTarget{rt.target},
		); err != nil {
			logger.Load(context.Background()).ErrorContext(
				context.Background(),
				"ecs: ELBv2 DeregisterTargets failed",
				"error", err, "targetGroupArn", rt.lb.TargetGroupArn, "taskArn", task.TaskArn)
		}
	}
}

// resolvedELBTarget pairs a service's LoadBalancer entry with the concrete
// ELBv2 target identity resolved for a specific task.
type resolvedELBTarget struct {
	lb     LoadBalancer
	target ELBTarget
}

// taskELBTargetsLocked resolves task's owning service's LoadBalancers into
// concrete ELBv2 targets, skipping any LoadBalancer entry with no
// TargetGroupArn or no resolvable target identity for this task (see
// resolveELBTargetLocked). Reports ok=false when the owning service has no
// LoadBalancers configured or none resolved, in which case the caller has
// nothing to register/deregister. Must be called with at least the read
// lock held.
func (b *InMemoryBackend) taskELBTargetsLocked(task *Task, clusterName string) ([]resolvedELBTarget, bool) {
	svc := b.serviceForTaskLocked(task, clusterName)
	if svc == nil || len(svc.LoadBalancers) == 0 {
		return nil, false
	}

	out := make([]resolvedELBTarget, 0, len(svc.LoadBalancers))

	for _, lb := range svc.LoadBalancers {
		if lb.TargetGroupArn == "" {
			continue
		}

		target, ok := b.resolveELBTargetLocked(task, clusterName, lb)
		if !ok {
			continue
		}

		out = append(out, resolvedELBTarget{lb: lb, target: target})
	}

	if len(out) == 0 {
		return nil, false
	}

	return out, true
}

// resolveELBTargetLocked resolves the concrete ELBv2 target identity for
// task against a single LoadBalancer entry. awsvpc-mode tasks (Fargate, or
// EC2 with awsvpc) register their ENI private IP as an "ip" target-type
// target, at the LoadBalancer's ContainerPort -- unchanged from before this
// backend modeled bridge/host host ports. bridge/host-mode EC2-launch-type
// tasks instead register their container instance's EC2 instance ID as an
// "instance" target-type target, at the host port this backend allocated
// for lb.ContainerName's mapping of lb.ContainerPort (see host_ports.go);
// ok is false when neither identity resolves (no ENI, no container
// instance, or no matching NetworkBinding -- e.g. the LoadBalancer's
// ContainerName/ContainerPort does not match any port mapping actually
// exposed by the task). Must be called with at least the read lock held.
func (b *InMemoryBackend) resolveELBTargetLocked(
	task *Task, clusterName string, lb LoadBalancer,
) (ELBTarget, bool) {
	if ip, ok := privateIPFromAttachments(task.Attachments); ok {
		return ELBTarget{ID: ip, Port: lb.ContainerPort}, true
	}

	if task.ContainerInstanceArn == "" {
		return ELBTarget{}, false
	}

	ci, found := b.containerInstances.Get(scopedKey(clusterName, task.ContainerInstanceArn))
	if !found || ci.EC2InstanceID == "" {
		return ELBTarget{}, false
	}

	hostPort, ok := hostPortForContainer(task.Containers, lb.ContainerName, lb.ContainerPort)
	if !ok {
		return ELBTarget{}, false
	}

	return ELBTarget{ID: ci.EC2InstanceID, Port: hostPort}, true
}

// hostPortForContainer returns the allocated host port bound to
// containerPort on the named container's NetworkBindings, if any.
func hostPortForContainer(containers []Container, containerName string, containerPort int) (int, bool) {
	for _, c := range containers {
		if c.Name != containerName {
			continue
		}

		for _, nb := range c.NetworkBindings {
			if nb.ContainerPort == containerPort {
				return nb.HostPort, true
			}
		}
	}

	return 0, false
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
