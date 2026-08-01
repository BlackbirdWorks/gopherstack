package grafana

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const resourceTypeWorkspace = "workspace"

// validAccountAccessTypes / validPermissionTypes / validAuthProviders back
// the enum validation CreateWorkspace/UpdateWorkspace perform, mirroring
// types.AccountAccessType / types.PermissionType / types.AuthenticationProviderTypes.
//
//nolint:gochecknoglobals // static enum tables, never mutated after init
var (
	validAccountAccessTypes = map[string]bool{
		AccountAccessTypeCurrentAccount: true,
		AccountAccessTypeOrganization:   true,
	}
	validPermissionTypes = map[string]bool{
		PermissionTypeCustomerManaged: true,
		PermissionTypeServiceManaged:  true,
	}
	validAuthProviders = map[string]bool{
		AuthProviderAWSSSO: true,
		AuthProviderSAML:   true,
	}
)

func validateAuthenticationProviders(providers []string) error {
	if len(providers) == 0 {
		return validationError("authenticationProviders must not be empty")
	}

	for _, p := range providers {
		if !validAuthProviders[p] {
			return validationError("invalid authentication provider: " + p)
		}
	}

	return nil
}

// validateCreateWorkspaceRequest validates the required enum fields and
// their cross-field dependencies for CreateWorkspace.
func validateCreateWorkspaceRequest(req *createWorkspaceRequest) error {
	if !validAccountAccessTypes[req.AccountAccessType] {
		return validationError("invalid accountAccessType: " + req.AccountAccessType)
	}

	if !validPermissionTypes[req.PermissionType] {
		return validationError("invalid permissionType: " + req.PermissionType)
	}

	if err := validateAuthenticationProviders(req.AuthenticationProviders); err != nil {
		return err
	}

	if req.AccountAccessType == AccountAccessTypeOrganization && len(req.WorkspaceOrganizationalUnits) == 0 {
		return validationError("workspaceOrganizationalUnits is required when accountAccessType is ORGANIZATION")
	}

	if req.GrafanaVersion != "" && !isValidGrafanaVersion(req.GrafanaVersion) {
		return validationError("invalid grafanaVersion: " + req.GrafanaVersion)
	}

	return nil
}

// newWorkspaceAuthState resolves the SsoClientID/SamlConfigurationStatus
// sub-state implied by a set of AuthenticationProviders, used by both
// CreateWorkspace and UpdateWorkspaceAuthentication.
func newWorkspaceAuthState(providers []string) (string, string) {
	var ssoClientID, samlStatus string

	for _, p := range providers {
		switch p {
		case AuthProviderAWSSSO:
			ssoClientID = "sso-" + randomHexID()
		case AuthProviderSAML:
			samlStatus = SamlStatusNotConfigured
		}
	}

	return ssoClientID, samlStatus
}

// CreateWorkspace creates a new workspace in the CREATING state, scheduling
// its async transition to ACTIVE (see scheduleWorkspaceTransition).
func (b *InMemoryBackend) CreateWorkspace(req *createWorkspaceRequest) (*Workspace, error) {
	if err := validateCreateWorkspaceRequest(req); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateWorkspace")
	defer b.mu.Unlock()

	id := newWorkspaceID()
	now := time.Now().UTC()

	grafanaVersion := req.GrafanaVersion
	if grafanaVersion == "" {
		grafanaVersion = defaultGrafanaVersion()
	}

	configuration := req.Configuration
	if configuration == "" {
		configuration = "{}"
	}

	ssoClientID, samlStatus := newWorkspaceAuthState(req.AuthenticationProviders)

	t := tags.New("grafana.workspace." + id + ".tags")
	t.Merge(req.Tags)

	w := &Workspace{
		ID:                       id,
		ARN:                      b.WorkspaceARN(id),
		Name:                     req.WorkspaceName,
		Description:              req.WorkspaceDescription,
		AccountAccessType:        req.AccountAccessType,
		AuthenticationProviders:  cloneStrs(req.AuthenticationProviders),
		PermissionType:           req.PermissionType,
		Status:                   StatusCreating,
		Created:                  now,
		Modified:                 now,
		Endpoint:                 fmt.Sprintf("https://%s.grafana-workspace.%s.amazonaws.com", id, b.region),
		GrafanaVersion:           grafanaVersion,
		Configuration:            configuration,
		DataSources:              cloneStrs(req.WorkspaceDataSources),
		NotificationDestinations: cloneStrs(req.WorkspaceNotificationDestinations),
		OrganizationRoleName:     req.OrganizationRoleName,
		OrganizationalUnits:      cloneStrs(req.WorkspaceOrganizationalUnits),
		StackSetName:             req.StackSetName,
		KmsKeyID:                 req.KmsKeyID,
		IPAddressType:            req.IPAddressType,
		NetworkAccessControl:     fromNetworkAccessWire(req.NetworkAccessControl),
		VpcConfiguration:         fromVpcConfigWire(req.VpcConfiguration),
		WorkspaceRoleARN:         req.WorkspaceRoleArn,
		Tags:                     t,
		SsoClientID:              ssoClientID,
		SamlConfigurationStatus:  samlStatus,
	}

	b.workspaces.Put(w)
	b.scheduleWorkspaceTransition(id, StatusCreating)

	cp := *w

	return &cp, nil
}

// scheduleWorkspaceTransition schedules an async transition of the named
// workspace from fromStatus to ACTIVE, matching services/eks's
// scheduleClusterActivation pattern. Every transitional status in this
// service (CREATING/UPDATING/UPGRADING/VERSION_UPDATING) resolves to ACTIVE,
// so the target is not a parameter. The transition only applies if the
// workspace still has fromStatus when the timer fires (a subsequent
// operation may have already moved it on).
func (b *InMemoryBackend) scheduleWorkspaceTransition(id, fromStatus string) {
	b.work.After("WorkspaceTransition", workspaceTransitionDelay, func() {
		b.mu.Lock("WorkspaceTransition-async")
		defer b.mu.Unlock()

		if w, ok := b.workspaces.Get(id); ok && w.Status == fromStatus {
			w.Status = StatusActive
			w.Modified = time.Now().UTC()
		}
	})
}

// DescribeWorkspace returns a copy of the workspace with the given ID.
func (b *InMemoryBackend) DescribeWorkspace(id string) (*Workspace, error) {
	b.mu.RLock("DescribeWorkspace")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	cp := *w

	return &cp, nil
}

// ListWorkspaces returns every workspace, ordered by ID (matching
// store.Table.Snapshot's deterministic ordering).
func (b *InMemoryBackend) ListWorkspaces() []*Workspace {
	b.mu.RLock("ListWorkspaces")
	defer b.mu.RUnlock()

	return b.workspaces.Snapshot()
}

// cloneStrs returns a deep copy of a string slice (nil-safe).
func cloneStrs(ss []string) []string {
	if ss == nil {
		return nil
	}

	cp := make([]string, len(ss))
	copy(cp, ss)

	return cp
}
