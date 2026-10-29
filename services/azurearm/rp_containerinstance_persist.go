package azurearm

import "maps"

type containerInstanceSnapshot struct {
	Groups map[string]*storedACIGroup `json:"groups"`
}

func (p *ContainerInstanceProvider) snapshot() *containerInstanceSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &containerInstanceSnapshot{Groups: make(map[string]*storedACIGroup, len(p.groups))}
	maps.Copy(snap.Groups, p.groups)

	return snap
}

// restore replaces the stored groups; restored groups have no running containers and report Stopped until started.
func (p *ContainerInstanceProvider) restore(snap *containerInstanceSnapshot) {
	p.engine.RemoveUnprefixed(acaKeyPrefix)

	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.groups = map[string]*storedACIGroup{}

	if snap == nil {
		return
	}

	for k, g := range snap.Groups {
		if g != nil {
			p.groups[k] = g
		}
	}
}
