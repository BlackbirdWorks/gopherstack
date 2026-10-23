package azurearm

// sqlSnapshot is the Microsoft.Sql provider's persisted ARM-side state; servers and databases are persisted by
// the SQL data plane.
type sqlSnapshot struct {
	Servers map[string]*sqlServer `json:"servers,omitempty"`
}

func (p *SQLProvider) snapshot() *sqlSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &sqlSnapshot{Servers: make(map[string]*sqlServer, len(p.res))}
	for k, r := range p.res {
		snap.Servers[k] = cloneServer(r)
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *SQLProvider) restore(snap *sqlSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.res = map[string]*sqlServer{}

	if snap == nil {
		return
	}

	for k, r := range snap.Servers {
		if r != nil {
			p.res[k] = r
		}
	}
}

func (h *Handler) sqlProvider() (*SQLProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftSQL)
	if !ok {
		return nil, false
	}

	sp, ok := p.(*SQLProvider)

	return sp, ok
}
