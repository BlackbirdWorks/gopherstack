package verifiedpermissions

import (
	"context"
	"encoding/json"

	cedar "github.com/cedar-policy/cedar-go"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	PolicyStores    map[string]*PolicyStore               `json:"policyStores"`
	Policies        map[string]map[string]*Policy         `json:"policies"`
	PolicyTemplates map[string]map[string]*PolicyTemplate `json:"policyTemplates"`
	IdentitySources map[string]map[string]*IdentitySource `json:"identitySources"`
	Schemas         map[string]*PolicyStoreSchema         `json:"schemas"`
	ResourceTags    map[string]map[string]string          `json:"resourceTags,omitempty"`
	AccountID       string                                `json:"accountID"`
	Region          string                                `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		PolicyStores:    b.policyStores,
		Policies:        b.policies,
		PolicyTemplates: b.policyTemplates,
		IdentitySources: b.identitySources,
		Schemas:         b.schemas,
		ResourceTags:    b.resourceTags,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "verifiedpermissions: snapshot marshal failed", "err", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "verifiedpermissions", data, &snap); err != nil {
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
	b.resourceTags = snap.ResourceTags
	b.policySetCache = make(map[string]*cedar.PolicySet)
	b.policySetDirty = make(map[string]bool)
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the ARN index from all restored resources.
	count := len(snap.PolicyStores)
	for _, policies := range snap.Policies {
		count += len(policies)
	}

	for _, templates := range snap.PolicyTemplates {
		count += len(templates)
	}

	for _, sources := range snap.IdentitySources {
		count += len(sources)
	}

	b.arnIndex = make(map[string]string, count)

	for id, ps := range snap.PolicyStores {
		b.arnIndex[ps.Arn] = "policyStore:" + id
	}

	for storeID, policies := range snap.Policies {
		for policyID := range policies {
			b.arnIndex[policyARN(snap.AccountID, storeID, policyID)] = "policy:" + storeID + ":" + policyID
		}
	}

	for storeID, templates := range snap.PolicyTemplates {
		for templateID := range templates {
			b.arnIndex[policyTemplateARN(snap.AccountID, storeID, templateID)] = "policyTemplate:" + storeID + ":" + templateID
		}
	}

	for storeID, sources := range snap.IdentitySources {
		for sourceID := range sources {
			b.arnIndex[identitySourceARN(snap.AccountID, storeID, sourceID)] = "identitySource:" + storeID + ":" + sourceID
		}
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

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
