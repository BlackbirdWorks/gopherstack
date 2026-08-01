package grafana

//nolint:gochecknoglobals // static enum table, never mutated after init
var validServiceAccountRoles = map[string]bool{RoleAdmin: true, RoleEditor: true, RoleViewer: true}

// CreateWorkspaceServiceAccount creates a service account within a
// workspace.
func (b *InMemoryBackend) CreateWorkspaceServiceAccount(
	workspaceID, name, grafanaRole string,
) (*ServiceAccount, error) {
	if !validServiceAccountRoles[grafanaRole] {
		return nil, validationError("invalid grafanaRole: " + grafanaRole)
	}

	b.mu.Lock("CreateWorkspaceServiceAccount")
	defer b.mu.Unlock()

	if _, ok := b.workspaces.Get(workspaceID); !ok {
		return nil, notFoundError(resourceTypeWorkspace, workspaceID)
	}

	for _, sa := range b.serviceAccountsByWorkspace.Get(workspaceID) {
		if sa.Name == name {
			return nil, conflictError("serviceAccount", name,
				"a service account named "+name+" already exists in this workspace")
		}
	}

	sa := &ServiceAccount{
		WorkspaceID: workspaceID,
		ID:          b.nextServiceAccountIDLocked(),
		Name:        name,
		GrafanaRole: grafanaRole,
	}

	b.serviceAccounts.Put(sa)

	cp := *sa

	return &cp, nil
}

// DeleteWorkspaceServiceAccount deletes a service account, cascading to its
// tokens (real behavior: "This will delete any tokens created for the
// service account, as well").
func (b *InMemoryBackend) DeleteWorkspaceServiceAccount(workspaceID, serviceAccountID string) error {
	b.mu.Lock("DeleteWorkspaceServiceAccount")
	defer b.mu.Unlock()

	key := serviceAccountKeyFn(&ServiceAccount{WorkspaceID: workspaceID, ID: serviceAccountID})
	if !b.serviceAccounts.Has(key) {
		return notFoundError("serviceAccount", serviceAccountID)
	}

	for _, tok := range b.tokensByServiceAccount.Get(workspaceID + "::" + serviceAccountID) {
		b.tokens.Delete(tokenKeyFn(tok))
	}

	b.serviceAccounts.Delete(key)

	return nil
}

// ListWorkspaceServiceAccounts returns every service account for a
// workspace.
func (b *InMemoryBackend) ListWorkspaceServiceAccounts(workspaceID string) ([]*ServiceAccount, error) {
	b.mu.RLock("ListWorkspaceServiceAccounts")
	defer b.mu.RUnlock()

	if _, ok := b.workspaces.Get(workspaceID); !ok {
		return nil, notFoundError(resourceTypeWorkspace, workspaceID)
	}

	items := b.serviceAccountsByWorkspace.Get(workspaceID)
	out := make([]*ServiceAccount, len(items))

	for i, sa := range items {
		cp := *sa
		out[i] = &cp
	}

	return out, nil
}
