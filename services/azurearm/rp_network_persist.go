package azurearm

import "maps"

type networkSnapshot struct {
	Items map[string]*netParent `json:"items"`
}

func (p *NetworkProvider) snapshot() *networkSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &networkSnapshot{Items: make(map[string]*netParent, len(p.items))}
	maps.Copy(snap.Items, p.items)

	return snap
}

func (p *NetworkProvider) restore(snap *networkSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.items = map[string]*netParent{}

	if snap == nil {
		return
	}

	for k, it := range snap.Items {
		if it == nil {
			continue
		}

		if it.Kids == nil {
			it.Kids = map[string][]*netObj{}
		}

		if it.Obj.Aux == nil {
			it.Obj.Aux = map[string]string{}
		}

		p.items[k] = it
	}
}
