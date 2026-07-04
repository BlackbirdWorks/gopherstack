package shield

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Protections               map[string]*Protection      `json:"protections"`
	ProtectionGroups          map[string]*ProtectionGroup `json:"protectionGroups"`
	Attacks                   map[string]*Attack          `json:"attacks"`
	ALARConfigs               map[string]*ALARConfig      `json:"alarConfigs,omitempty"`
	Subscription              *Subscription               `json:"subscription,omitempty"`
	DRTAccess                 *DRTAccess                  `json:"drtAccess,omitempty"`
	AccountID                 string                      `json:"accountID"`
	Region                    string                      `json:"region"`
	ProactiveEngagementStatus string                      `json:"proactiveEngagementStatus,omitempty"`
	EmergencyContacts         []EmergencyContact          `json:"emergencyContacts,omitempty"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Protections == nil {
		s.Protections = make(map[string]*Protection)
	}

	if s.ProtectionGroups == nil {
		s.ProtectionGroups = make(map[string]*ProtectionGroup)
	}

	if s.Attacks == nil {
		s.Attacks = make(map[string]*Attack)
	}
}

// Snapshot serialises the backend state to JSON.
// All mutable maps are deep-copied so the snapshot is isolated from subsequent mutations.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy protections so mutations after snapshot cannot corrupt it.
	protectionsCopy := make(map[string]*Protection, len(b.protections))
	for k, v := range b.protections {
		protectionsCopy[k] = cloneProtection(v)
	}

	groupsCopy := make(map[string]*ProtectionGroup, len(b.protectionGroups))
	for k, v := range b.protectionGroups {
		groupsCopy[k] = cloneProtectionGroup(v)
	}

	attacksCopy := make(map[string]*Attack, len(b.attacks))

	for k, a := range b.attacks {
		cp := *a
		attacksCopy[k] = &cp
	}

	var drtCopy *DRTAccess
	if b.drtAccess != nil {
		cp := *b.drtAccess
		cp.LogBucketList = append([]string(nil), b.drtAccess.LogBucketList...)
		drtCopy = &cp
	}

	alarCopy := make(map[string]*ALARConfig, len(b.alarConfigs))
	for k, v := range b.alarConfigs {
		cp := *v
		alarCopy[k] = &cp
	}

	snap := backendSnapshot{
		Protections:               protectionsCopy,
		ProtectionGroups:          groupsCopy,
		Attacks:                   attacksCopy,
		ALARConfigs:               alarCopy,
		Subscription:              b.subscription,
		DRTAccess:                 drtCopy,
		EmergencyContacts:         append([]EmergencyContact(nil), b.emergencyContacts...),
		ProactiveEngagementStatus: b.proactiveEngagementStatus,
		AccountID:                 b.accountID,
		Region:                    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "shield: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds indexes.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "shield", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.protections = snap.Protections
	b.protectionGroups = snap.ProtectionGroups
	b.attacks = snap.Attacks
	b.subscription = snap.Subscription
	b.drtAccess = snap.DRTAccess
	b.emergencyContacts = snap.EmergencyContacts
	b.proactiveEngagementStatus = snap.ProactiveEngagementStatus
	b.accountID = snap.AccountID
	b.region = snap.Region

	if snap.ALARConfigs != nil {
		b.alarConfigs = snap.ALARConfigs
	} else {
		b.alarConfigs = make(map[string]*ALARConfig)
	}

	// Rebuild O(1) indexes.
	b.resourceARNIndex = make(map[string]string, len(snap.Protections))
	b.nameIndex = make(map[string]string, len(snap.Protections))

	for id, p := range snap.Protections {
		b.resourceARNIndex[p.ResourceARN] = id
		b.nameIndex[p.Name] = id
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
