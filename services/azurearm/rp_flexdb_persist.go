package azurearm

// flexSnapshot is a flexible server provider's persisted ARM-side state; databases hosted by an engine are not
// persisted.
type flexSnapshot struct {
	Servers  map[string]*flexServer `json:"servers,omitempty"`
	Instance string                 `json:"instance,omitempty"`
}

func (p *FlexDBProvider) snapshot() *flexSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &flexSnapshot{Servers: make(map[string]*flexServer, len(p.res)), Instance: p.instance}
	for k, r := range p.res {
		snap.Servers[k] = cloneFlexServer(r)
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The engine is not touched.
func (p *FlexDBProvider) restore(snap *flexSnapshot) {
	p.mu.Lock("restore")

	p.res = map[string]*flexServer{}

	if snap != nil {
		if flexInstanceRE.MatchString(snap.Instance) {
			p.instance = snap.Instance
		}

		for k, r := range snap.Servers {
			if r != nil {
				p.res[k] = r
			}
		}
	}

	keep := map[string]bool{}
	for _, f := range p.allBackupFiles() {
		keep[f] = true
	}

	eng := p.engine
	p.mu.Unlock()

	if eng != nil {
		p.dumpStore().sweep(p.dumpPrefix(), keep)
	}
}

func (h *Handler) postgresFlexProvider() (*FlexDBProvider, bool) {
	return h.flexProviderFor(namespaceDBforPostgreSQL)
}

func (h *Handler) mysqlFlexProvider() (*FlexDBProvider, bool) {
	return h.flexProviderFor(namespaceDBforMySQL)
}
