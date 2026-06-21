package cognitoidentity

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Pools         map[string]map[string]*IdentityPool        `json:"pools"`
	Identities    map[string]map[string]*Identity            `json:"identities"`
	Roles         map[string]map[string]*IdentityRoles       `json:"roles"`
	PrincipalTags map[string]map[string]*PrincipalTagMapping `json:"principalTags"`
	AccountID     string                                     `json:"accountID"`
	Region        string                                     `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Pools:         b.pools,
		Identities:    b.identities,
		Roles:         b.roles,
		PrincipalTags: b.principalTags,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cognitoidentity: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds indexes.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cognitoidentity", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Pools == nil {
		snap.Pools = make(map[string]map[string]*IdentityPool)
	}

	if snap.Identities == nil {
		snap.Identities = make(map[string]map[string]*Identity)
	}

	if snap.Roles == nil {
		snap.Roles = make(map[string]map[string]*IdentityRoles)
	}

	if snap.PrincipalTags == nil {
		snap.PrincipalTags = make(map[string]map[string]*PrincipalTagMapping)
	}

	b.pools = snap.Pools
	b.identities = snap.Identities
	b.roles = snap.Roles
	b.principalTags = snap.PrincipalTags
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild poolsByName, poolsByARN, and identitiesByPool indexes (all region-nested).
	b.poolsByName = make(map[string]map[string]*IdentityPool)
	b.poolsByARN = make(map[string]map[string]*IdentityPool)

	for region, regionPools := range snap.Pools {
		if b.poolsByName[region] == nil {
			b.poolsByName[region] = make(map[string]*IdentityPool)
		}

		if b.poolsByARN[region] == nil {
			b.poolsByARN[region] = make(map[string]*IdentityPool)
		}

		for _, p := range regionPools {
			b.poolsByName[region][p.IdentityPoolName] = p
			b.poolsByARN[region][p.ARN] = p
		}
	}

	b.identitiesByPool = make(map[string]map[string][]*Identity)

	for region, regionIdentities := range snap.Identities {
		if b.identitiesByPool[region] == nil {
			b.identitiesByPool[region] = make(map[string][]*Identity)
		}

		for _, identity := range regionIdentities {
			b.identitiesByPool[region][identity.IdentityPoolID] = append(
				b.identitiesByPool[region][identity.IdentityPoolID], identity,
			)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
