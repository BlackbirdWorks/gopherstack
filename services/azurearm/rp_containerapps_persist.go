package azurearm

import (
	"context"
	"encoding/json"
)

type containerAppsSnapshot struct {
	Envs map[string]*storedACAEnv `json:"envs"`
	Apps map[string]*storedACAApp `json:"apps"`
	Jobs map[string]*storedACAJob `json:"jobs"`
}

// snapshot deep-copies the stored state; execution results are settled first so they survive without their replicas.
func (p *ContainerAppsProvider) snapshot() *containerAppsSnapshot {
	p.mu.Lock("snapshot")
	defer p.mu.Unlock()

	now := p.now().UTC()

	for _, j := range p.jobs {
		for _, e := range j.Executions {
			if v := p.evaluate(j, e, now); v.final {
				e.Status, e.End, e.Done = v.status, v.end, true
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

// restore replaces the stored state. Revisions with minReplicas above zero start them again; the rest start on the
// first request.
func (p *ContainerAppsProvider) restore(snap *containerAppsSnapshot) {
	p.opMu.Lock("restore")
	defer p.opMu.Unlock()
	defer p.armScheduler()

	p.rt.closeAll()
	p.replaceState(snap)
	p.startMinReplicas()
}

func (p *ContainerAppsProvider) startMinReplicas() {
	type job struct {
		spec *acaRevSpec
		key  string
	}

	var jobs []job

	p.mu.RLock("startMinReplicas")

	for _, a := range p.apps {
		if a.Stopped {
			continue
		}

		for _, r := range a.Revisions {
			if !r.Active {
				continue
			}

			rk := acaRevKey(a, r.Name)

			spec, err := p.buildSpec(a, r)
			if err != nil {
				p.rt.setFailure(rk, err.Error())

				continue
			}

			if spec.Min > 0 {
				jobs = append(jobs, job{spec: spec, key: rk})
			}
		}
	}
	p.mu.RUnlock()

	for _, j := range jobs {
		if err := p.rt.ensure(context.Background(), j.spec, j.spec.Min); err != nil {
			p.rt.setFailure(j.key, err.Error())
		}
	}
}

func (p *ContainerAppsProvider) replaceState(snap *containerAppsSnapshot) {
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
