package grafana

import "time"

// validateUpdateWorkspaceRequest validates enum fields (when provided) and
// the mutual exclusivity of the remove-vs-set network/VPC configuration
// pairs, matching UpdateWorkspaceInput's doc comments on
// RemoveNetworkAccessConfiguration/RemoveVpcConfiguration: setting either of
// those true while also providing a new config of that type returns an error.
func validateUpdateWorkspaceRequest(req *updateWorkspaceRequest) error {
	if req.AccountAccessType != "" && !validAccountAccessTypes[req.AccountAccessType] {
		return validationError("invalid accountAccessType: " + req.AccountAccessType)
	}

	if req.PermissionType != "" && !validPermissionTypes[req.PermissionType] {
		return validationError("invalid permissionType: " + req.PermissionType)
	}

	if req.RemoveNetworkAccessConfiguration && req.NetworkAccessControl != nil {
		return validationError("cannot set removeNetworkAccessConfiguration and networkAccessControl together")
	}

	if req.RemoveVpcConfiguration && req.VpcConfiguration != nil {
		return validationError("cannot set removeVpcConfiguration and vpcConfiguration together")
	}

	return nil
}

// applyUpdateWorkspaceRequest merges req's provided (non-zero) fields onto
// w, leaving every omitted field unchanged -- matching UpdateWorkspace's doc
// comment: "If you ... omit any optional parameters, the existing values of
// those parameters are not changed." Callers must hold b.mu.
func applyUpdateWorkspaceRequest(w *Workspace, req *updateWorkspaceRequest) {
	applyUpdateWorkspaceScalars(w, req)
	applyUpdateWorkspaceLists(w, req)
	applyUpdateWorkspaceNetworking(w, req)
}

func applyUpdateWorkspaceScalars(w *Workspace, req *updateWorkspaceRequest) {
	if req.AccountAccessType != "" {
		w.AccountAccessType = req.AccountAccessType
	}

	if req.IPAddressType != "" {
		w.IPAddressType = req.IPAddressType
	}

	if req.OrganizationRoleName != "" {
		w.OrganizationRoleName = req.OrganizationRoleName
	}

	if req.PermissionType != "" {
		w.PermissionType = req.PermissionType
	}

	if req.StackSetName != "" {
		w.StackSetName = req.StackSetName
	}

	if req.WorkspaceDescription != "" {
		w.Description = req.WorkspaceDescription
	}

	if req.WorkspaceName != "" {
		w.Name = req.WorkspaceName
	}

	if req.WorkspaceRoleArn != "" {
		w.WorkspaceRoleARN = req.WorkspaceRoleArn
	}
}

func applyUpdateWorkspaceLists(w *Workspace, req *updateWorkspaceRequest) {
	if req.WorkspaceDataSources != nil {
		w.DataSources = cloneStrs(req.WorkspaceDataSources)
	}

	if req.WorkspaceNotificationDestinations != nil {
		w.NotificationDestinations = cloneStrs(req.WorkspaceNotificationDestinations)
	}

	if req.WorkspaceOrganizationalUnits != nil {
		w.OrganizationalUnits = cloneStrs(req.WorkspaceOrganizationalUnits)
	}
}

func applyUpdateWorkspaceNetworking(w *Workspace, req *updateWorkspaceRequest) {
	if req.RemoveNetworkAccessConfiguration {
		w.NetworkAccessControl = nil
	} else if req.NetworkAccessControl != nil {
		w.NetworkAccessControl = fromNetworkAccessWire(req.NetworkAccessControl)
	}

	if req.RemoveVpcConfiguration {
		w.VpcConfiguration = nil
	} else if req.VpcConfiguration != nil {
		w.VpcConfiguration = fromVpcConfigWire(req.VpcConfiguration)
	}
}

// UpdateWorkspace merges req onto the named workspace. The workspace must be
// ACTIVE or DEGRADED (not mid-transition) or this returns a ConflictException-
// shaped error.
func (b *InMemoryBackend) UpdateWorkspace(id string, req *updateWorkspaceRequest) (*Workspace, error) {
	if err := validateUpdateWorkspaceRequest(req); err != nil {
		return nil, err
	}

	if err := b.validateWorkspaceRoleArn(req.WorkspaceRoleArn); err != nil {
		return nil, err
	}

	if err := b.validateVpcConfiguration(req.VpcConfiguration); err != nil {
		return nil, err
	}

	if err := b.validateOrganizationalUnits(req.WorkspaceOrganizationalUnits); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateWorkspace")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	if w.Status != StatusActive && w.Status != StatusDegraded {
		return nil, notActiveError(id, w.Status)
	}

	applyUpdateWorkspaceRequest(w, req)
	w.Status = StatusUpdating
	w.Modified = time.Now().UTC()
	b.scheduleWorkspaceTransition(id, StatusUpdating)

	cp := *w

	return &cp, nil
}

// DeleteWorkspace deletes a workspace, cascading to its API keys, service
// accounts, service account tokens, and permissions (real AWS would reject
// deletion of a workspace with certain dependent resources still attached;
// this emulator instead cascades synchronously, matching services/eks's
// DeleteCluster "for test convenience this fake cascades" precedent). The
// returned copy reflects the workspace's state immediately before removal,
// with Status forced to DELETING.
func (b *InMemoryBackend) DeleteWorkspace(id string) (*Workspace, error) {
	b.mu.Lock("DeleteWorkspace")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	if reason, degraded := b.injectedTransitionOutcome(); reason != "" || degraded {
		if degraded {
			reason = "chaos-injected deletion failure"
		}

		w.Status = StatusDeletionFailed
		w.DegradedWorkspaceReason = reason
		w.Modified = time.Now().UTC()

		cp := *w

		return &cp, nil
	}

	cp := *w
	cp.Status = StatusDeleting

	b.cascadeDeleteWorkspaceLocked(id)

	if w.Tags != nil {
		w.Tags.Close()
	}

	b.workspaces.Delete(id)

	return &cp, nil
}

// cascadeDeleteWorkspaceLocked removes every child resource (API keys,
// service accounts, tokens, permissions) belonging to id. Callers must hold
// b.mu.
func (b *InMemoryBackend) cascadeDeleteWorkspaceLocked(id string) {
	for _, k := range b.apiKeysByWorkspace.Get(id) {
		b.apiKeys.Delete(apiKeyKeyFn(k))
	}

	for _, sa := range b.serviceAccountsByWorkspace.Get(id) {
		for _, tok := range b.tokensByServiceAccount.Get(sa.WorkspaceID + "::" + sa.ID) {
			b.tokens.Delete(tokenKeyFn(tok))
		}

		b.serviceAccounts.Delete(serviceAccountKeyFn(sa))
	}

	for _, p := range b.permissionsByWorkspace.Get(id) {
		b.permissions.Delete(permissionKeyFn(p))
	}
}
