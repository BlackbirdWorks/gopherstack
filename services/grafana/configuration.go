package grafana

import "time"

// DescribeWorkspaceConfiguration returns the opaque configuration JSON blob
// and current Grafana version for a workspace.
func (b *InMemoryBackend) DescribeWorkspaceConfiguration(id string) (string, string, error) {
	b.mu.RLock("DescribeWorkspaceConfiguration")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return "", "", notFoundError(resourceTypeWorkspace, id)
	}

	return w.Configuration, w.GrafanaVersion, nil
}

// UpdateWorkspaceConfiguration replaces a workspace's configuration blob and,
// when GrafanaVersion is supplied, upgrades it (upgrade-only -- see
// isUpgradeVersion). The workspace must be ACTIVE or DEGRADED.
func (b *InMemoryBackend) UpdateWorkspaceConfiguration(id string, req *updateWorkspaceConfigurationRequest) error {
	b.mu.Lock("UpdateWorkspaceConfiguration")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return notFoundError(resourceTypeWorkspace, id)
	}

	if w.Status != StatusActive && w.Status != StatusDegraded {
		return notActiveError(id, w.Status)
	}

	w.Configuration = req.Configuration
	w.Modified = time.Now().UTC()

	if req.GrafanaVersion == "" || req.GrafanaVersion == w.GrafanaVersion {
		return nil
	}

	if !isUpgradeVersion(w.GrafanaVersion, req.GrafanaVersion) {
		return validationError("grafanaVersion " + req.GrafanaVersion +
			" is not a valid upgrade from " + w.GrafanaVersion)
	}

	w.GrafanaVersion = req.GrafanaVersion
	w.Status = StatusVersionUpdating
	b.scheduleWorkspaceTransition(id, StatusVersionUpdating)

	return nil
}
