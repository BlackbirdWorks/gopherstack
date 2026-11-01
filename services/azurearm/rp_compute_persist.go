package azurearm

import "maps"

type computeSnapshot struct {
	Items map[string]*cmpRes `json:"items"`
}

func (p *ComputeProvider) snapshot() *computeSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &computeSnapshot{Items: make(map[string]*cmpRes, len(p.items))}
	maps.Copy(snap.Items, p.items)

	return snap
}

func (p *ComputeProvider) restore(snap *computeSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.items = map[string]*cmpRes{}

	if snap == nil {
		return
	}

	for k, r := range snap.Items {
		if r == nil {
			continue
		}

		if r.Aux == nil {
			r.Aux = map[string]string{}
		}

		p.items[k] = r
	}
}
