package ociidentity

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to a stored type would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Created     time.Time                      `json:"created"`
	Policies    map[string]*Policy             `json:"policies"`
	TagDefs     map[string]*TagDef             `json:"tagDefinitions"`
	Groups      map[string]*Group              `json:"groups"`
	Members     map[string]*Membership         `json:"memberships"`
	DynGroups   map[string]*DynamicGroup       `json:"dynamicGroups"`
	Compart     map[string]*Compartment        `json:"compartments"`
	TagNS       map[string]*TagNamespace       `json:"tagNamespaces"`
	Users       map[string]*User               `json:"users"`
	TagDefaults map[string]*TagDefault         `json:"tagDefaults"`
	WorkReqs    map[string]*WorkRequest        `json:"workRequests"`
	NetSources  map[string]*NetworkSource      `json:"networkSources"`
	RegionSubs  map[string]*RegionSubscription `json:"regionSubscriptions"`
	IDPs        map[string]*IdentityProvider   `json:"identityProviders"`
	IdpMappings map[string]*IdpGroupMapping    `json:"idpGroupMappings"`
	AuthPolicy  *AuthPolicy                    `json:"authenticationPolicy,omitempty"`
	Retry       map[string]ociretry.Entry      `json:"retryTokens"`
	Auth        []ociauth.KeyRecord            `json:"authKeys"`
	Version     int                            `json:"version"`
	Seq         int64                          `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ociidentity", backendSnapshot{
		Version: snapshotVersion, Created: b.created, Compart: b.compartment, Users: b.users, Groups: b.groups,
		Members: b.members, DynGroups: b.dynGroups, Policies: b.policies, TagNS: b.tagNS, TagDefs: b.tagDefs,
		TagDefaults: b.tagDefaults, WorkReqs: b.workReqs, NetSources: b.netSources, RegionSubs: b.regionSubs,
		AuthPolicy: b.authPolicy, IDPs: b.idps, IdpMappings: b.idpMappings, Auth: keys, Seq: b.seq,
		Retry: b.retry.Entries(),
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ociidentity", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ociidentity: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.compartment, b.users, b.groups = snap.Compart, snap.Users, snap.Groups
	b.members, b.dynGroups, b.policies = snap.Members, snap.DynGroups, snap.Policies
	b.tagNS, b.tagDefs, b.tagDefaults, b.workReqs = snap.TagNS, snap.TagDefs, snap.TagDefaults, snap.WorkReqs
	b.netSources, b.regionSubs, b.authPolicy = snap.NetSources, snap.RegionSubs, snap.AuthPolicy
	b.idps, b.idpMappings = snap.IDPs, snap.IdpMappings
	b.seq = snap.Seq

	if !snap.Created.IsZero() {
		b.created = snap.Created
	}

	b.retry.Load(snap.Retry)
	b.authReplay = map[string]replaySecret{}
	b.Auth.Load(snap.Auth)

	return nil
}

func nilCheck[T any](kind string, m *map[string]*T) error {
	if *m == nil {
		*m = map[string]*T{}
	}

	for id, v := range *m {
		if v == nil {
			return fmt.Errorf("%w: %s %q", ErrSnapshotNull, kind, id)
		}
	}

	return nil
}

func checkSnapshot(s *backendSnapshot) error {
	checks := []error{
		nilCheck("compartment", &s.Compart), nilCheck("user", &s.Users), nilCheck("group", &s.Groups),
		nilCheck("membership", &s.Members), nilCheck("dynamic group", &s.DynGroups), nilCheck("policy", &s.Policies),
		nilCheck("tag namespace", &s.TagNS), nilCheck("tag definition", &s.TagDefs),
		nilCheck("tag default", &s.TagDefaults), nilCheck("work request", &s.WorkReqs),
		nilCheck("network source", &s.NetSources), nilCheck("region subscription", &s.RegionSubs),
		nilCheck("identity provider", &s.IDPs), nilCheck("idp group mapping", &s.IdpMappings),
	}

	for _, err := range checks {
		if err != nil {
			return err
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ociidentity: restore snapshot: %w", err)
	}

	return nil
}
