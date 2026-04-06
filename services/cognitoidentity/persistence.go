package cognitoidentity

import "encoding/json"

type backendSnapshot struct {
	Pools         map[string]*IdentityPool        `json:"pools"`
	Identities    map[string]*Identity            `json:"identities"`
	Roles         map[string]*IdentityRoles       `json:"roles"`
	PrincipalTags map[string]*PrincipalTagMapping `json:"principalTags"`
	AccountID     string                          `json:"accountID"`
	Region        string                          `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
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

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds indexes.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Pools == nil {
		snap.Pools = make(map[string]*IdentityPool)
	}

	if snap.Identities == nil {
		snap.Identities = make(map[string]*Identity)
	}

	if snap.Roles == nil {
		snap.Roles = make(map[string]*IdentityRoles)
	}

	if snap.PrincipalTags == nil {
		snap.PrincipalTags = make(map[string]*PrincipalTagMapping)
	}

	b.pools = snap.Pools
	b.identities = snap.Identities
	b.roles = snap.Roles
	b.principalTags = snap.PrincipalTags
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild poolsByName, poolsByARN, and identitiesByPool indexes.
	b.poolsByName = make(map[string]*IdentityPool, len(snap.Pools))
	b.poolsByARN = make(map[string]*IdentityPool, len(snap.Pools))

	for _, p := range snap.Pools {
		b.poolsByName[p.IdentityPoolName] = p
		b.poolsByARN[p.ARN] = p
	}

	b.identitiesByPool = make(map[string][]*Identity)

	for _, identity := range snap.Identities {
		b.identitiesByPool[identity.IdentityPoolID] = append(b.identitiesByPool[identity.IdentityPoolID], identity)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
