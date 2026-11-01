package azurearm

import "encoding/json"

type containerAppsSnapshot struct {
	Envs map[string]*storedACAEnv `json:"envs"`
	Apps map[string]*storedACAApp `json:"apps"`
	Jobs map[string]*storedACAJob `json:"jobs"`
}

// snapshot deep-copies the stored state; execution results are settled first so they survive without their replicas.
func (p *ContainerAppsProvider) snapshot() *containerAppsSnapshot {
	p.mu.Lock("snapshot")
	defer p.mu.Unlock()

	for _, j := range p.jobs {
		for _, e := range j.Executions {
			if e.Status, e.End = p.execStatus(j, e); e.Status == acaExecUnknown {
				e.Status = ""
			}
		}
	}

	snap := &containerAppsSnapshot{Envs: p.envs, Apps: p.apps, Jobs: p.jobs}

	buf, err := json.Marshal(snap)
	if err != nil {
		return &containerAppsSnapshot{}
	}

	var out containerAppsSnapshot
	if err = json.Unmarshal(buf, &out); err != nil {
		return &containerAppsSnapshot{}
	}

	return &out
}

// restore replaces the stored state. Restored revisions have no replicas; the first request starts them.
func (p *ContainerAppsProvider) restore(snap *containerAppsSnapshot) {
	p.opMu.Lock("restore")
	defer p.opMu.Unlock()

	p.rt.closeAll()

	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.envs, p.apps, p.jobs = map[string]*storedACAEnv{}, map[string]*storedACAApp{}, map[string]*storedACAJob{}

	if snap != nil {
		for k, e := range snap.Envs {
			if e != nil {
				p.envs[k] = e
			}
		}

		for k, a := range snap.Apps {
			if a != nil {
				p.apps[k] = a
			}
		}

		for k, j := range snap.Jobs {
			if j != nil {
				p.jobs[k] = j
			}
		}
	}

	p.rebuildHostsLocked()
}
