package azuremonitor

// ListWorkspaces returns the ARM body of every workspace across subscriptions.
func (b *Backend) ListWorkspaces() []map[string]any {
	b.mu.RLock("ListWorkspaces")
	defer b.mu.RUnlock()

	return listScoped(b.workspaces, func(*Workspace) bool { return true }, (*Workspace).body)
}

// ListWorkspaceTables returns the ARM body of every table of the workspace named by id, customer id or name.
func (b *Backend) ListWorkspaceTables(idOrName string) ([]map[string]any, error) {
	b.mu.RLock("ListWorkspaceTables")
	defer b.mu.RUnlock()

	w, ok := b.findWorkspaceLocked(idOrName)
	if !ok {
		return nil, notFound("workspace %q", idOrName)
	}

	return listScoped(w.Tables, func(*Table) bool { return true },
		func(t *Table) map[string]any { return t.body(w) }), nil
}

// ListDataCollectionEndpoints returns every data collection endpoint, advertising endpoints for host.
func (b *Backend) ListDataCollectionEndpoints(host string) []map[string]any {
	b.mu.RLock("ListDataCollectionEndpoints")
	defer b.mu.RUnlock()

	ep := b.endpoint(host)

	return listScoped(b.dces, func(*DCE) bool { return true }, func(d *DCE) map[string]any { return d.body(ep) })
}

// ListDataCollectionRules returns every data collection rule, advertising endpoints for host.
func (b *Backend) ListDataCollectionRules(host string) []map[string]any {
	b.mu.RLock("ListDataCollectionRules")
	defer b.mu.RUnlock()

	ep := b.endpoint(host)

	return listScoped(b.dcrs, func(*DCR) bool { return true }, func(d *DCR) map[string]any { return b.dcrBody(d, ep) })
}

// QueryJSON runs a query against one workspace and returns the HTTP status and JSON-ready body the
// data-plane API would answer with (the error envelope on failure).
func (b *Backend) QueryJSON(idOrName string, req QueryRequest) (int, any) {
	resp, aerr := b.QueryWorkspace(idOrName, req, false)
	if aerr != nil {
		return aerr.status, aerr.envelope("")
	}

	return statusOK, resp
}
