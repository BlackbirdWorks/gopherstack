package ec2

import (
	"maps"
	"time"
)

// ExecTarget is a Compute-backed instance that can run commands in its container.
type ExecTarget struct {
	LaunchTime         time.Time
	Tags               map[string]string
	InstanceID         string
	ContainerID        string
	HasInstanceProfile bool
}

// ExecTargets returns running Compute-backed instances, restricted to ids when non-empty.
func (b *InMemoryBackend) ExecTargets(ids []string) []ExecTarget {
	b.mu.RLock("ExecTargets")
	defer b.mu.RUnlock()

	profiled := make(map[string]struct{})

	for _, a := range b.iamAssociations.All() {
		if a.State == stateAssociated {
			profiled[a.InstanceID] = struct{}{}
		}
	}

	var out []ExecTarget

	visit := func(inst *Instance) {
		if inst.ProviderID == "" || inst.State.Name != StateRunning.Name {
			return
		}

		_, has := profiled[inst.ID]
		out = append(out, ExecTarget{
			InstanceID:         inst.ID,
			ContainerID:        inst.ProviderID,
			LaunchTime:         inst.LaunchTime,
			Tags:               maps.Clone(b.tags[inst.ID]),
			HasInstanceProfile: has,
		})
	}

	if len(ids) > 0 {
		for _, id := range ids {
			if inst, ok := b.instances.Get(id); ok {
				visit(inst)
			}
		}

		return out
	}

	for _, inst := range b.instances.All() {
		visit(inst)
	}

	return out
}
