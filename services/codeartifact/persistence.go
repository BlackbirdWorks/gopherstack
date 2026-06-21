package codeartifact

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backendSnapshot mirrors the region-nested backend maps (outer key = region).
type backendSnapshot struct {
	Domains             map[string]map[string]*Domain                      `json:"domains"`
	Repositories        map[string]map[string]*Repository                  `json:"repositories"`
	PackageGroups       map[string]map[string]*PackageGroup                `json:"packageGroups"`
	Packages            map[string]map[string]*Package                     `json:"packages"`
	PackageVersions     map[string]map[string]*PackageVersion              `json:"packageVersions"`
	ExternalConnections map[string]map[string][]ExternalConnection         `json:"externalConnections"`
	RepositoryPolicies  map[string]map[string]*RepositoryPermissionsPolicy `json:"repositoryPolicies"`
	DomainPolicies      map[string]map[string]*DomainPermissionsPolicy     `json:"domainPolicies"`
	AccountID           string                                             `json:"accountID"`
	Region              string                                             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:             b.domains,
		Repositories:        b.repositories,
		PackageGroups:       b.packageGroups,
		Packages:            b.packages,
		PackageVersions:     b.packageVersions,
		ExternalConnections: b.externalConnections,
		RepositoryPolicies:  b.repositoryPolicies,
		DomainPolicies:      b.domainPolicies,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	return persistence.MarshalSnapshot(ctx, "codeartifact", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codeartifact", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Domains == nil {
		snap.Domains = make(map[string]map[string]*Domain)
	}
	if snap.Repositories == nil {
		snap.Repositories = make(map[string]map[string]*Repository)
	}
	if snap.PackageGroups == nil {
		snap.PackageGroups = make(map[string]map[string]*PackageGroup)
	}
	if snap.Packages == nil {
		snap.Packages = make(map[string]map[string]*Package)
	}
	if snap.PackageVersions == nil {
		snap.PackageVersions = make(map[string]map[string]*PackageVersion)
	}
	if snap.ExternalConnections == nil {
		snap.ExternalConnections = make(map[string]map[string][]ExternalConnection)
	}
	if snap.RepositoryPolicies == nil {
		snap.RepositoryPolicies = make(map[string]map[string]*RepositoryPermissionsPolicy)
	}
	if snap.DomainPolicies == nil {
		snap.DomainPolicies = make(map[string]map[string]*DomainPermissionsPolicy)
	}

	b.domains = snap.Domains
	b.repositories = snap.Repositories
	b.packageGroups = snap.PackageGroups
	b.packages = snap.Packages
	b.packageVersions = snap.PackageVersions
	b.externalConnections = snap.ExternalConnections
	b.repositoryPolicies = snap.RepositoryPolicies
	b.domainPolicies = snap.DomainPolicies
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
