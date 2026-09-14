package ecs

import (
	"crypto/rand"
	"math/big"
	"strconv"
)

const (
	// ephemeralPortRangeMin/Max is the dynamic host-port range ECS assigns
	// bridge-mode containers when hostPort is 0 or omitted. Matches the AWS
	// ECS developer guide's Port mappings section verbatim ("The default
	// ephemeral port range Docker version 1.6.0 and later is listed on the
	// instance under /proc/sys/net/ipv4/ip_local_port_range. If this kernel
	// parameter is unavailable, the default ephemeral port range from
	// 49153-65535 is used."), confirmed identical on both the Fargate
	// (task_definition_parameters.html) and EC2-launch-type
	// (task_definition_parameters_ec2.html) parameter pages under
	// docs.aws.amazon.com/AmazonECS/latest/developerguide/. gopherstack has
	// no real container-instance kernel to inspect, so this documented
	// fallback range is used unconditionally.
	ephemeralPortRangeMin = 49153
	ephemeralPortRangeMax = 65535

	// dynamicPortRandomAttempts bounds random sampling before falling back to
	// a linear scan, so exhaustion is only reported once the range is
	// genuinely full rather than merely unlucky.
	dynamicPortRandomAttempts = 64

	// failureReasonResource and failureReasonResourcePorts are RunTask/
	// StartTask Failure.Reason values, verified against the real reason
	// family documented at
	// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/api_failures_messages.html
	// ("RunTask or StartTask: RESOURCE:* -- The resource or resources that
	// are requested by the task are unavailable on the container instances
	// in the cluster. If the resource is CPU, memory, ports, or elastic
	// network interfaces..."). RESOURCE:* itself is the literal reason AWS
	// returns when no container instance can satisfy placement at all;
	// RESOURCE:PORTS is this backend's specific reason once a container
	// instance for a host/bridge-mode task is chosen but its port mappings
	// cannot be satisfied.
	failureReasonResource      = "RESOURCE:*"
	failureReasonResourcePorts = "RESOURCE:PORTS"
)

// hostPortKey builds the ContainerInstance.AllocatedPorts key for a
// (protocol, port) pair, e.g. "tcp/51000".
func hostPortKey(protocol string, port int) string {
	return protocol + "/" + strconv.Itoa(port)
}

// effectiveEC2NetworkMode returns the network mode used for EC2-launch-type
// host-port allocation: an empty NetworkMode defaults to bridge on EC2 (see
// validateNetworkMode's doc comment in taskdef_validation.go).
func effectiveEC2NetworkMode(mode string) string {
	if mode == "" {
		return networkModeBridge
	}

	return mode
}

// reserveTaskHostPortsLocked reserves host ports on instanceArn for every
// port mapping in td's container definitions, returning the resulting
// NetworkBinding list keyed by container name. Only bridge and host network
// modes consume container-instance host ports (awsvpc/none return ok=true
// with no bindings: this backend's EC2+awsvpc tasks get no host-port or ENI
// modeling here, unchanged from before -- see privateIPFromAttachments).
//
// All-or-nothing: if any port mapping cannot be satisfied, every port
// reserved earlier in this call is released before returning ok=false, so a
// partially-placed task never leaks a reservation. Must be called with the
// write lock held.
func (b *InMemoryBackend) reserveTaskHostPortsLocked(
	clusterName, instanceArn string, td *TaskDefinition,
) (map[string][]NetworkBinding, bool) {
	mode := effectiveEC2NetworkMode(td.NetworkMode)
	if mode != networkModeBridge && mode != networkModeHost {
		return nil, true
	}

	ci, found := b.containerInstances.Get(scopedKey(clusterName, instanceArn))
	if !found {
		return nil, false
	}

	if ci.AllocatedPorts == nil {
		ci.AllocatedPorts = make(map[string]bool)
	}

	bindings := make(map[string][]NetworkBinding, len(td.ContainerDefinitions))

	var reservedKeys []string

	for _, cd := range td.ContainerDefinitions {
		for _, pm := range cd.PortMappings {
			if pm.ContainerPort == 0 {
				continue
			}

			proto := pm.Protocol
			if proto == "" {
				proto = transportTCP
			}

			hostPort, ok := assignHostPortLocked(ci, mode, proto, pm.HostPort, pm.ContainerPort)
			if !ok {
				releaseReservedPortsLocked(ci, reservedKeys)

				return nil, false
			}

			reservedKeys = append(reservedKeys, hostPortKey(proto, hostPort))

			bindings[cd.Name] = append(bindings[cd.Name], NetworkBinding{
				BindIP:        "0.0.0.0",
				Protocol:      proto,
				ContainerPort: pm.ContainerPort,
				HostPort:      hostPort,
			})
		}
	}

	return bindings, true
}

// assignHostPortLocked reserves one host port on ci for a single port
// mapping. host mode always binds hostPort == containerPort (per the ECS
// developer guide: "If the network mode of a task definition is set to
// host, host ports must either be undefined or match the container port in
// the port mapping."); bridge mode honors a nonzero requested hostPort as a
// static mapping or, when zero, assigns a dynamic port from the documented
// ephemeral range. Must be called with the write lock held.
func assignHostPortLocked(
	ci *ContainerInstance, mode, protocol string, requestedHostPort, containerPort int,
) (int, bool) {
	if mode == networkModeHost {
		return reserveExactPortLocked(ci, protocol, containerPort)
	}

	if requestedHostPort != 0 {
		return reserveExactPortLocked(ci, protocol, requestedHostPort)
	}

	return assignDynamicHostPortLocked(ci, protocol)
}

// reserveExactPortLocked reserves a specific host port, reporting ok=false
// if it is already taken on ci. Must be called with the write lock held.
func reserveExactPortLocked(ci *ContainerInstance, protocol string, port int) (int, bool) {
	key := hostPortKey(protocol, port)
	if ci.AllocatedPorts[key] {
		return 0, false
	}

	ci.AllocatedPorts[key] = true

	return port, true
}

// assignDynamicHostPortLocked picks a free port from the ephemeral range via
// random sampling, falling back to a linear scan once sampling is unlucky
// enough times running to plausibly indicate the range is exhausted. Must be
// called with the write lock held.
func assignDynamicHostPortLocked(ci *ContainerInstance, protocol string) (int, bool) {
	span := big.NewInt(int64(ephemeralPortRangeMax - ephemeralPortRangeMin + 1))

	for range dynamicPortRandomAttempts {
		n, err := rand.Int(rand.Reader, span)
		if err != nil {
			break
		}

		candidate := ephemeralPortRangeMin + int(n.Int64())
		if reserved, ok := reserveExactPortLocked(ci, protocol, candidate); ok {
			return reserved, true
		}
	}

	for candidate := ephemeralPortRangeMin; candidate <= ephemeralPortRangeMax; candidate++ {
		if reserved, ok := reserveExactPortLocked(ci, protocol, candidate); ok {
			return reserved, true
		}
	}

	return 0, false
}

// releaseReservedPortsLocked frees the given AllocatedPorts keys on ci. Must
// be called with the write lock held.
func releaseReservedPortsLocked(ci *ContainerInstance, keys []string) {
	for _, k := range keys {
		delete(ci.AllocatedPorts, k)
	}
}

// applyHostPortBindings sets NetworkBindings on task's containers from
// bindings (keyed by container name), matching what reserveTaskHostPortsLocked
// reserved for this task. A no-op when bindings is empty (Fargate/awsvpc
// tasks, or an EC2-launch-type task whose network mode consumes no host
// ports).
func applyHostPortBindings(task *Task, bindings map[string][]NetworkBinding) {
	if len(bindings) == 0 {
		return
	}

	for i := range task.Containers {
		if nb, ok := bindings[task.Containers[i].Name]; ok {
			task.Containers[i].NetworkBindings = nb
		}
	}
}

// releaseTaskHostPortsLocked frees every host port task reserved on its
// container instance (bridge/host EC2-launch-type tasks only; awsvpc/Fargate
// tasks hold no such reservation and task.ContainerInstanceArn is empty for
// them). Safe to call for a task that reserved nothing. Must be called with
// the write lock held, alongside the other end-of-task-life cleanup (see
// deregisterTaskFromELBv2Locked's call sites in tasks.go/lifecycle.go).
func (b *InMemoryBackend) releaseTaskHostPortsLocked(clusterName string, task *Task) {
	if task.ContainerInstanceArn == "" {
		return
	}

	ci, found := b.containerInstances.Get(scopedKey(clusterName, task.ContainerInstanceArn))
	if !found || ci.AllocatedPorts == nil {
		return
	}

	for _, c := range task.Containers {
		for _, nb := range c.NetworkBindings {
			delete(ci.AllocatedPorts, hostPortKey(nb.Protocol, nb.HostPort))
		}
	}
}
