package workspaces

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// applicationAssociationsPageSize is this backend's default page size for
// DescribeApplicationAssociations; real AWS doesn't document an exact
// default, so this is chosen generously (larger than any realistic
// per-application association list) so pagination only activates when a
// caller explicitly requests a smaller MaxResults.
const applicationAssociationsPageSize = 100

// associationStateCompleted/associationStateRemoved are the two terminal
// values of the real AssociationState enum this backend can reach: it applies
// Associate/Disassociate/Deploy synchronously, with no pending/installing
// window, so an association goes straight from nonexistent to COMPLETED (or
// gone straight to REMOVED on disassociate) -- see WorkspaceResourceAssociation's
// doc comment in interfaces.go. Neither "INSTALLED" nor "UNINSTALLED" (the
// previous hardcoded values here) are members of the real enum.
const (
	associationStateCompleted = "COMPLETED"
	associationStateRemoved   = "REMOVED"
)

// AssociateWorkspaceApplication associates an application with a workspace.
//
// ApplicationId is intentionally not validated against a real catalog: real
// AWS's WorkSpaces Application Manager applications come from a read-only
// AWS/partner catalog with no public API to create one, and this backend
// never seeds b.applications, so requiring a match would make every call to
// this operation permanently fail -- see DescribeApplications, which has the
// same "always empty" catalog for the same reason.
func (b *InMemoryBackend) AssociateWorkspaceApplication(
	workspaceID, applicationID string,
) (WorkspaceResourceAssociation, error) {
	b.mu.Lock("AssociateWorkspaceApplication")
	defer b.mu.Unlock()

	if !b.workspaces.Has(workspaceID) {
		return WorkspaceResourceAssociation{}, ErrWorkspaceNotFound
	}

	if b.appAssociations[workspaceID] == nil {
		b.appAssociations[workspaceID] = make(map[string]*storedAppAssociation)
	}

	now := time.Now().UTC()
	b.appAssociations[workspaceID][applicationID] = &storedAppAssociation{
		State:           associationStateCompleted,
		CreatedAt:       now,
		LastUpdatedTime: now,
	}

	return b.workspaceAssociationLocked(workspaceID, applicationID), nil
}

// DisassociateWorkspaceApplication removes an application association from a
// workspace, returning the resulting REMOVED association. Created/
// LastUpdatedTime are left zero-valued on the returned value: once removed,
// this backend no longer tracks when the association was originally made, and
// fabricating a timestamp would be dishonest.
func (b *InMemoryBackend) DisassociateWorkspaceApplication(
	workspaceID, applicationID string,
) (WorkspaceResourceAssociation, error) {
	b.mu.Lock("DisassociateWorkspaceApplication")
	defer b.mu.Unlock()

	if !b.workspaces.Has(workspaceID) {
		return WorkspaceResourceAssociation{}, ErrWorkspaceNotFound
	}

	delete(b.appAssociations[workspaceID], applicationID)

	return WorkspaceResourceAssociation{
		WorkspaceID:            workspaceID,
		AssociatedResourceID:   applicationID,
		AssociatedResourceType: associatedResourceTypeApplication,
		State:                  associationStateRemoved,
	}, nil
}

// DeployWorkspaceApplications deploys the applications currently associated
// with a WorkSpace. This backend has no pending-install queue -- Associate
// already leaves every association COMPLETED -- so Deploy's real work here is
// validating the WorkSpace exists and reporting its current associations.
func (b *InMemoryBackend) DeployWorkspaceApplications(
	workspaceID string, _ bool,
) ([]WorkspaceResourceAssociation, error) {
	b.mu.RLock("DeployWorkspaceApplications")
	defer b.mu.RUnlock()

	if !b.workspaces.Has(workspaceID) {
		return nil, ErrWorkspaceNotFound
	}

	return b.workspaceAssociationsLocked(workspaceID), nil
}

// DescribeWorkspaceAssociations returns application associations for a workspace.
func (b *InMemoryBackend) DescribeWorkspaceAssociations(
	workspaceID string,
	associatedResourceTypes []string,
) ([]WorkspaceResourceAssociation, error) {
	b.mu.RLock("DescribeWorkspaceAssociations")
	defer b.mu.RUnlock()

	if !b.workspaces.Has(workspaceID) {
		return nil, ErrWorkspaceNotFound
	}

	if err := validateAssociatedResourceTypes(associatedResourceTypes); err != nil {
		return nil, err
	}

	return b.workspaceAssociationsLocked(workspaceID), nil
}

// workspaceAssociationLocked returns one association, or the zero value if
// absent. Callers must hold b.mu.
func (b *InMemoryBackend) workspaceAssociationLocked(
	workspaceID, applicationID string,
) WorkspaceResourceAssociation {
	a, ok := b.appAssociations[workspaceID][applicationID]
	if !ok {
		return WorkspaceResourceAssociation{}
	}

	return WorkspaceResourceAssociation{
		WorkspaceID:            workspaceID,
		AssociatedResourceID:   applicationID,
		AssociatedResourceType: associatedResourceTypeApplication,
		State:                  a.State,
		Created:                a.CreatedAt,
		LastUpdatedTime:        a.LastUpdatedTime,
	}
}

// workspaceAssociationsLocked returns every association for one workspace.
// Callers must hold b.mu.
func (b *InMemoryBackend) workspaceAssociationsLocked(workspaceID string) []WorkspaceResourceAssociation {
	assoc := b.appAssociations[workspaceID]
	result := make([]WorkspaceResourceAssociation, 0, len(assoc))

	for appID := range assoc {
		result = append(result, b.workspaceAssociationLocked(workspaceID, appID))
	}

	return result
}

// DescribeApplicationAssociations returns workspace associations for an
// application.
//
// ApplicationId is not validated for existence, matching
// AssociateWorkspaceApplication: the applications catalog is never populated
// in this backend, so requiring a match would make this operation
// permanently return zero results even for associations it created itself.
func (b *InMemoryBackend) DescribeApplicationAssociations(
	applicationID string, associatedResourceTypes []string, maxResults int32, nextToken string,
) ([]ApplicationResourceAssociation, string, error) {
	b.mu.RLock("DescribeApplicationAssociations")
	defer b.mu.RUnlock()

	if err := validateApplicationAssociatedResourceTypes(associatedResourceTypes); err != nil {
		return nil, "", err
	}

	workspaceIDs := make([]string, 0, len(b.appAssociations))
	for wsID, apps := range b.appAssociations {
		if _, ok := apps[applicationID]; ok {
			workspaceIDs = append(workspaceIDs, wsID)
		}
	}

	sort.Strings(workspaceIDs)

	result := make([]ApplicationResourceAssociation, 0, len(workspaceIDs))
	for _, wsID := range workspaceIDs {
		a := b.appAssociations[wsID][applicationID]
		result = append(result, ApplicationResourceAssociation{
			ApplicationID:          applicationID,
			AssociatedResourceID:   wsID,
			AssociatedResourceType: "WORKSPACE",
			State:                  a.State,
			Created:                a.CreatedAt,
			LastUpdatedTime:        a.LastUpdatedTime,
		})
	}

	pg := page.New(result, nextToken, int(maxResults), applicationAssociationsPageSize)

	return pg.Data, pg.Next, nil
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
