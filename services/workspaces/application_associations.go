package workspaces

// AssociateWorkspaceApplication associates an application with a workspace.
func (b *InMemoryBackend) AssociateWorkspaceApplication(workspaceID, applicationID string) error {
	b.mu.Lock("AssociateWorkspaceApplication")
	defer b.mu.Unlock()

	if b.appAssociations[workspaceID] == nil {
		b.appAssociations[workspaceID] = make(map[string]struct{})
	}

	b.appAssociations[workspaceID][applicationID] = struct{}{}

	return nil
}

// DisassociateWorkspaceApplication removes an application association from a workspace.
func (b *InMemoryBackend) DisassociateWorkspaceApplication(
	workspaceID, applicationID string,
) error {
	b.mu.Lock("DisassociateWorkspaceApplication")
	defer b.mu.Unlock()

	delete(b.appAssociations[workspaceID], applicationID)

	return nil
}

// DeployWorkspaceApplications deploys applications on a workspace (no-op in memory).
func (b *InMemoryBackend) DeployWorkspaceApplications(
	workspaceID string, //nolint:revive // existing issue.
	_ bool,
) ([]map[string]string, error) {
	b.mu.RLock("DeployWorkspaceApplications")
	defer b.mu.RUnlock()

	return []map[string]string{}, nil
}

// DescribeWorkspaceAssociations returns application associations for a workspace.
func (b *InMemoryBackend) DescribeWorkspaceAssociations(
	workspaceID string,
	_ []string,
) ([]map[string]string, error) {
	b.mu.RLock("DescribeWorkspaceAssociations")
	defer b.mu.RUnlock()

	assoc := b.appAssociations[workspaceID]
	result := make([]map[string]string, 0, len(assoc))

	for appID := range assoc {
		result = append(result, map[string]string{
			wireKeyWorkspaceID:     workspaceID,
			"AssociatedResourceId": appID,
			"AssociationStatus":    "INSTALLED", //nolint:goconst // existing issue.
		})
	}

	return result, nil
}

// DescribeApplicationAssociations returns workspace associations for an application.
func (b *InMemoryBackend) DescribeApplicationAssociations(
	applicationID string, _ []string, _ int32, _ string,
) ([]map[string]string, string, error) {
	b.mu.RLock("DescribeApplicationAssociations")
	defer b.mu.RUnlock()

	var result []map[string]string

	for wsID, apps := range b.appAssociations {
		if _, ok := apps[applicationID]; ok {
			result = append(result, map[string]string{
				wireKeyWorkspaceID:     wsID,
				"AssociatedResourceId": applicationID,
				"AssociationStatus":    "INSTALLED",
			})
		}
	}

	if result == nil {
		result = []map[string]string{}
	}

	return result, "", nil
}

// DescribeApplications returns stored applications, filtered by IDs.
func (b *InMemoryBackend) DescribeApplications(
	appIDs []string,
	_ int32,
	_ string,
) ([]*storedApplication, string, error) {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	filter := buildFilter(appIDs)
	var result []*storedApplication

	for _, app := range b.applications.All() {
		if !matchesFilter(filter, app.AppID) {
			continue
		}

		cp := *app
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedApplication{}
	}

	return result, "", nil
}
