package workspaces

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// associatedResourceTypeApplication is the only real enum value for both
// ImageAssociatedResourceType and BundleAssociatedResourceType (verified
// against the SDK's types.ImageAssociatedResourceType /
// types.BundleAssociatedResourceType, which each carry a single "APPLICATION"
// value) -- the WorkSpaces Application Manager feature only associates
// applications with images/bundles.
const associatedResourceTypeApplication = "APPLICATION"

// validateAssociatedResourceTypes validates the required AssociatedResourceTypes
// field shared by DescribeImageAssociations/DescribeBundleAssociations.
func validateAssociatedResourceTypes(types []string) error {
	if len(types) == 0 {
		return awserr.New("AssociatedResourceTypes is required", awserr.ErrInvalidParameter)
	}

	for _, t := range types {
		if t != associatedResourceTypeApplication {
			return awserr.Newf(
				"invalid AssociatedResourceType %q", awserr.ErrInvalidParameter, t)
		}
	}

	return nil
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                lockmetrics.New("workspaces"),
		registry:          store.NewRegistry(),
		tags:              make(map[string]map[string]string),
		directoryIpGroups: make(map[string]map[string]struct{}),
		imagePermissions:  make(map[string]map[string]bool),
		clientProperties:  make(map[string]storedClientProps),
		appAssociations:   make(map[string]map[string]struct{}),
		accountID:         accountID,
		region:            region,
	}

	registerAllTables(b)

	return b
}

// regionFor returns the region from ctx when present, falling back to b.region.
func (b *InMemoryBackend) regionFor(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.directoryIpGroups = make(map[string]map[string]struct{})
	b.imagePermissions = make(map[string]map[string]bool)
	b.clientProperties = make(map[string]storedClientProps)
	b.appAssociations = make(map[string]map[string]struct{})
	b.accountConfig = storedAccountConfig{}
	b.accountModifications = nil
	b.counter = 0
}

// wireKeyWorkspaceID is the shared AWS wire-format key name for a WorkspaceId,
// reused across the workspace-lifecycle and application-association response
// builders (split across multiple files by family).
const wireKeyWorkspaceID = "WorkspaceId"

// nextID generates a sequential resource ID with the given prefix.
func (b *InMemoryBackend) nextID(prefix string) string {
	b.counter++

	return fmt.Sprintf("%s%08x", prefix, b.counter)
}

// cloneTags returns a copy of the given tag map.
func cloneTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

// buildFilter converts a string slice to a set for O(1) membership tests.
// An empty result means "no filter" (accept all).
func buildFilter(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}

	f := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		f[id] = struct{}{}
	}

	return f
}

// matchesFilter returns true when filter is empty (no filter) or value is in filter.
func matchesFilter(filter map[string]struct{}, value string) bool {
	if len(filter) == 0 {
		return true
	}

	_, ok := filter[value]

	return ok
}
