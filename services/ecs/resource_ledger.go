package ecs

import (
	"slices"
	"strconv"
	"strings"
)

const (
	resourceCPU      = "CPU"
	resourceMemory   = "MEMORY"
	resourcePorts    = "PORTS"
	resourcePortsUDP = "PORTS_UDP"

	resourceTypeInteger  = "INTEGER"
	resourceTypeStrSet   = "STRINGSET"
	failureReasonCPU     = "RESOURCE:CPU"
	failureReasonMemory  = "RESOURCE:MEMORY"
	defaultInstanceCPU   = 4096
	defaultInstanceMemMB = 15776
)

// reservedAgentPorts are the host ports the ECS agent reserves on every container instance.
var reservedAgentPorts = []string{"22", "2375", "2376", "51678", "51679"} //nolint:gochecknoglobals // constant list

// Resource mirrors types.Resource: a named, typed container-instance resource value.
type Resource struct {
	Name           string   `json:"name"`
	Type           string   `json:"type"`
	StringSetValue []string `json:"stringSetValue,omitempty"`
	DoubleValue    float64  `json:"doubleValue"`
	LongValue      int64    `json:"longValue"`
	IntegerValue   int32    `json:"integerValue"`
}

// InstanceResourceResolver reports the CPU units and memory MiB of an EC2 instance's type.
type InstanceResourceResolver interface {
	ContainerInstanceResources(ec2InstanceID string) (cpuUnits, memoryMiB int, ok bool)
	InstanceTypeResources(instanceType string) (cpuUnits, memoryMiB int, ok bool)
}

// SetInstanceResourceResolver wires the EC2 lookup that sizes container instances registered without totalResources.
func (b *InMemoryBackend) SetInstanceResourceResolver(r InstanceResourceResolver) {
	b.mu.Lock("SetInstanceResourceResolver")
	defer b.mu.Unlock()

	b.instanceResolver = r
}

func intResource(name string, v int) Resource {
	return Resource{
		Name: name, Type: resourceTypeInteger, LongValue: int64(v), DoubleValue: float64(v),
		IntegerValue: int32(v), //nolint:gosec // bounded instance sizes
	}
}

func stringSetResource(name string, vals []string) Resource {
	return Resource{Name: name, Type: resourceTypeStrSet, StringSetValue: slices.Clone(vals)}
}

// registeredResourcesLocked returns the resources to record for a new instance: the request's totalResources
// (missing members defaulted), else defaults sized from the EC2 instance type.
func (b *InMemoryBackend) registeredResourcesLocked(ec2InstanceID string, given []Resource) []Resource {
	cpu, mem := defaultInstanceCPU, defaultInstanceMemMB

	if b.instanceResolver != nil && ec2InstanceID != "" {
		if c, m, ok := b.instanceResolver.ContainerInstanceResources(ec2InstanceID); ok {
			cpu, mem = c, m
		}
	}

	out := make([]Resource, 0, len(given)+4) //nolint:mnd // CPU, MEMORY, PORTS, PORTS_UDP

	for _, r := range given {
		out = append(out, normalizeResource(r))
	}

	if findResource(out, resourceCPU) == nil {
		out = append(out, intResource(resourceCPU, cpu))
	}

	if findResource(out, resourceMemory) == nil {
		out = append(out, intResource(resourceMemory, mem))
	}

	if findResource(out, resourcePorts) == nil {
		out = append(out, stringSetResource(resourcePorts, reservedAgentPorts))
	}

	if findResource(out, resourcePortsUDP) == nil {
		out = append(out, stringSetResource(resourcePortsUDP, nil))
	}

	return out
}

// normalizeResource fills the type and sibling numeric members the way the agent reports them.
func normalizeResource(r Resource) Resource {
	if r.Type == "" {
		r.Type = resourceTypeInteger
		if len(r.StringSetValue) > 0 {
			r.Type = resourceTypeStrSet
		}
	}

	if r.Type == resourceTypeInteger {
		v := int64(r.IntegerValue)
		if v == 0 {
			v = r.LongValue
		}

		if v == 0 {
			v = int64(r.DoubleValue)
		}

		r.IntegerValue, r.LongValue, r.DoubleValue = int32(v), v, float64(v) //nolint:gosec // bounded
	}

	return r
}

func findResource(rs []Resource, name string) *Resource {
	for i := range rs {
		if rs[i].Name == name {
			return &rs[i]
		}
	}

	return nil
}

// instanceTasksLocked returns the live (non-STOPPED) tasks placed on an instance.
func (b *InMemoryBackend) instanceTasksLocked(clusterName, instanceArn string) []*Task {
	var out []*Task

	for taskArn := range b.tasksByInstance[clusterName][instanceArn] {
		if t, ok := b.tasks.Get(taskArn); ok && t.LastStatus != statusStopped {
			out = append(out, t)
		}
	}

	return out
}

// instanceUsedLocked sums the CPU units and memory MiB reserved by an instance's live tasks.
func (b *InMemoryBackend) instanceUsedLocked(clusterName, instanceArn string) (int64, int64) {
	var cpu, mem float64

	for _, t := range b.instanceTasksLocked(clusterName, instanceArn) {
		td, err := b.findTaskDefinitionLocked(t.TaskDefinitionArn)
		if err != nil {
			continue
		}

		c, m := taskReservation(t, td)
		cpu += c
		mem += m
	}

	return int64(cpu), int64(mem)
}

func registeredInt(ci *ContainerInstance, name string) (int64, bool) {
	r := findResource(ci.RegisteredResources, name)
	if r == nil {
		return 0, false
	}

	return int64(r.IntegerValue), true
}

// instanceFreeLocked returns the unreserved CPU and memory of an instance; ok is false for unmetered instances.
func (b *InMemoryBackend) instanceFreeLocked(clusterName string, ci *ContainerInstance) (int64, int64, bool) {
	cpu, okCPU := registeredInt(ci, resourceCPU)
	mem, okMem := registeredInt(ci, resourceMemory)

	if !okCPU || !okMem {
		return 0, 0, false
	}

	usedCPU, usedMem := b.instanceUsedLocked(clusterName, ci.ContainerInstanceArn)

	return cpu - usedCPU, mem - usedMem, true
}

// resourceShortfallLocked returns the AWS failure reason when the instance lacks the requested CPU or memory.
func (b *InMemoryBackend) resourceShortfallLocked(clusterName string, ci *ContainerInstance, cpu, mem int64) string {
	freeCPU, freeMem, metered := b.instanceFreeLocked(clusterName, ci)

	switch {
	case !metered:
		return ""
	case freeCPU < cpu:
		return failureReasonCPU
	case freeMem < mem:
		return failureReasonMemory
	default:
		return ""
	}
}

// remainingResourcesLocked derives remainingResources: registered minus live tasks' reservations and host ports.
func (b *InMemoryBackend) remainingResourcesLocked(clusterName string, ci *ContainerInstance) []Resource {
	if len(ci.RegisteredResources) == 0 {
		return nil
	}

	freeCPU, freeMem, _ := b.instanceFreeLocked(clusterName, ci)
	out := make([]Resource, 0, len(ci.RegisteredResources))

	for _, r := range ci.RegisteredResources {
		switch r.Name {
		case resourceCPU:
			out = append(out, intResource(resourceCPU, int(freeCPU)))
		case resourceMemory:
			out = append(out, intResource(resourceMemory, int(freeMem)))
		case resourcePorts:
			out = append(out, stringSetResource(resourcePorts, withAllocatedPorts(r.StringSetValue, ci, transportTCP)))
		case resourcePortsUDP:
			out = append(out, stringSetResource(resourcePortsUDP, withAllocatedPorts(r.StringSetValue, ci, "udp")))
		default:
			out = append(out, r)
		}
	}

	return out
}

// withAllocatedPorts appends the instance's host ports for proto to the reserved list, numerically sorted.
func withAllocatedPorts(reserved []string, ci *ContainerInstance, proto string) []string {
	out := slices.Clone(reserved)
	prefix := proto + "/"

	for key := range ci.AllocatedPorts {
		if p, ok := strings.CutPrefix(key, prefix); ok && !slices.Contains(out, p) {
			out = append(out, p)
		}
	}

	slices.SortFunc(out, func(a, b string) int {
		ai, _ := strconv.Atoi(a)
		bi, _ := strconv.Atoi(b)

		return ai - bi
	})

	return out
}
