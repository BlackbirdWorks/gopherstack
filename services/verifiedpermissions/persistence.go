package verifiedpermissions

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	PolicyStores    map[string]*PolicyStore               `json:"policyStores"`
	Policies        map[string]map[string]*Policy         `json:"policies"`
	PolicyTemplates map[string]map[string]*PolicyTemplate `json:"policyTemplates"`
	IdentitySources map[string]map[string]*IdentitySource `json:"identitySources"`
	Schemas         map[string]*PolicyStoreSchema         `json:"schemas"`
	AccountID       string                                `json:"accountID"`
	Region          string                                `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		PolicyStores:    b.policyStores,
		Policies:        b.policies,
		PolicyTemplates: b.policyTemplates,
		IdentitySources: b.identitySources,
		Schemas:         b.schemas,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("verifiedpermissions: snapshot marshal failed", "err", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.policyStores = snap.PolicyStores
	b.policies = snap.Policies
	b.policyTemplates = snap.PolicyTemplates
	b.identitySources = snap.IdentitySources
	b.schemas = snap.Schemas
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the ARN index from the restored policy stores.
	b.arnIndex = make(map[string]string, len(snap.PolicyStores))

	for id, ps := range snap.PolicyStores {
		b.arnIndex[ps.Arn] = id
	}

	return nil
}

// ensureNonNilMaps initialises any nil maps in a snapshot so Restore never
// assigns a nil map to the backend.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.PolicyStores == nil {
		snap.PolicyStores = make(map[string]*PolicyStore)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]*Policy)
	}

	if snap.PolicyTemplates == nil {
		snap.PolicyTemplates = make(map[string]map[string]*PolicyTemplate)
	}

	if snap.IdentitySources == nil {
		snap.IdentitySources = make(map[string]map[string]*IdentitySource)
	}

	if snap.Schemas == nil {
		snap.Schemas = make(map[string]*PolicyStoreSchema)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
