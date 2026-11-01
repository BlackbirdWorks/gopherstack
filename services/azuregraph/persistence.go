package azuregraph

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Owners       map[string][]string     `json:"owners"`
	Members      map[string][]string     `json:"members"`
	Secrets      map[string]string       `json:"secrets"`
	Applications []Application           `json:"applications"`
	Principals   []ServicePrincipal      `json:"servicePrincipals"`
	Users        []User                  `json:"users"`
	Groups       []Group                 `json:"groups"`
	Assignments  []AppRoleAssignment     `json:"appRoleAssignments"`
	Grants       []OAuth2PermissionGrant `json:"oauth2PermissionGrants"`
	Version      int                     `json:"version"`
}

// Snapshot implements persistence.Persistable. Secrets are salted PBKDF2 hashes; plaintext is never stored.
func (b *Backend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "azuregraph", backendSnapshot{
		Version: snapshotVersion, Owners: b.owners, Members: b.members, Secrets: b.secrets,
		Applications: b.apps.slice(), Principals: b.sps.slice(), Users: b.users.slice(), Groups: b.groups.slice(),
		Assignments: b.assigns.slice(), Grants: b.grants.slice(),
	})
}

func fill[T any](s *store[T], items []T, id func(*T) string) {
	s.reset()

	for i := range items {
		s.put(lc(id(&items[i])), &items[i])
	}
}

// Restore implements persistence.Persistable.
func (b *Backend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azuregraph", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azuregraph: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.reset()

		return nil
	}

	fill(&b.apps, snap.Applications, func(a *Application) string { return a.ID })
	fill(&b.sps, snap.Principals, func(a *ServicePrincipal) string { return a.ID })
	fill(&b.users, snap.Users, func(a *User) string { return a.ID })
	fill(&b.groups, snap.Groups, func(a *Group) string { return a.ID })
	fill(&b.assigns, snap.Assignments, func(a *AppRoleAssignment) string { return a.ID })
	fill(&b.grants, snap.Grants, func(a *OAuth2PermissionGrant) string { return a.ID })

	b.owners, b.members, b.secrets = orEmpty(snap.Owners), orEmpty(snap.Members), orEmpty(snap.Secrets)

	return nil
}

func orEmpty[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}

	return m
}
