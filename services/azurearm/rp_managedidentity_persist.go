package azurearm

import "maps"

type managedIdentitySnapshot struct {
	Identities map[string]*storedIdentity `json:"identities"`
}

func (p *ManagedIdentityProvider) snapshot() *managedIdentitySnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &managedIdentitySnapshot{Identities: make(map[string]*storedIdentity, len(p.identities))}
	maps.Copy(snap.Identities, p.identities)

	return snap
}

func (p *ManagedIdentityProvider) restore(snap *managedIdentitySnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.identities = map[string]*storedIdentity{}

	if snap == nil {
		return
	}

	for k, i := range snap.Identities {
		if i != nil {
			p.identities[k] = i
		}
	}
}
