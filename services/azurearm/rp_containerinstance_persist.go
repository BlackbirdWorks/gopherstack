package azurearm

type containerInstanceSnapshot struct {
	Groups   map[string]*storedACIGroup   `json:"groups"`
	Profiles map[string]*storedACIProfile `json:"profiles,omitempty"`
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

	if len(p.profiles) > 0 {
		snap.Profiles = make(map[string]*storedACIProfile, len(p.profiles))

		for k, g := range p.profiles {
			cp := *g
			cp.Revisions = append([]aciProfileRevision(nil), g.Revisions...)
			snap.Profiles[k] = &cp
		}
	}

	return snap
}

// restore replaces the stored groups and redeploys the ones that were running or starting when the snapshot was
// taken; the others report Stopped until started.
func (p *ContainerInstanceProvider) restore(snap *containerInstanceSnapshot) {
	p.engine.RemoveUnprefixed(acaKeyPrefix)

	p.mu.Lock("restore")

	p.groups = map[string]*storedACIGroup{}
	p.profiles = map[string]*storedACIProfile{}

	var resume []aciConfig

	if snap != nil {
		for k, g := range snap.Profiles {
			if g != nil {
				p.profiles[k] = g
			}
		}

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
