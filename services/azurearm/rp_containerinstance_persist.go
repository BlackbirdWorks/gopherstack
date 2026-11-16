package azurearm

type containerInstanceSnapshot struct {
	Groups map[string]*storedACIGroup `json:"groups"`
}

func (p *ContainerInstanceProvider) snapshot() *containerInstanceSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &containerInstanceSnapshot{Groups: make(map[string]*storedACIGroup, len(p.groups))}

	for k, g := range p.groups {
		cp := *g
		cp.Resume = p.engine.Resumable(aciKey(g.ResourceGroup, g.Name))
		snap.Groups[k] = &cp
	}

	return snap
}

// restore replaces the stored groups and redeploys the ones that were running or starting when the snapshot was
// taken; the others report Stopped until started.
func (p *ContainerInstanceProvider) restore(snap *containerInstanceSnapshot) {
	p.engine.RemoveUnprefixed(acaKeyPrefix)

	p.mu.Lock("restore")

	p.groups = map[string]*storedACIGroup{}

	var resume []aciConfig

	if snap != nil {
		for k, g := range snap.Groups {
			if g == nil {
				continue
			}

			p.groups[k] = g

			if g.Resume {
				resume = append(resume, p.config(g))
			}
		}
	}

	p.mu.Unlock()

	for _, cfg := range resume {
		p.engine.Deploy(cfg)
	}
}
