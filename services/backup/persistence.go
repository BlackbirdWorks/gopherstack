package backup

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Vaults                   map[string]*Vault                              `json:"vaults"`
	Plans                    map[string]*Plan                               `json:"plans"`
	Jobs                     map[string]*Job                                `json:"jobs"`
	Selections               map[string]map[string]*Selection               `json:"selections,omitempty"`
	Frameworks               map[string]*Framework                          `json:"frameworks,omitempty"`
	LegalHolds               map[string]*LegalHold                          `json:"legalHolds,omitempty"`
	ReportPlans              map[string]*ReportPlan                         `json:"reportPlans,omitempty"`
	RestoreAccessVaults      map[string]*RestoreAccessVault                 `json:"restoreAccessVaults,omitempty"`
	RestoreTestingPlans      map[string]*RestoreTestingPlan                 `json:"restoreTestingPlans,omitempty"`
	RestoreTestingSelections map[string]map[string]*RestoreTestingSelection `json:"restoreTestingSelections,omitempty"`
	MpaApprovals             map[string]string                              `json:"mpaApprovals,omitempty"`
	AccountID                string                                         `json:"accountID"`
	Region                   string                                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Vaults:                   b.vaults,
		Plans:                    b.plans,
		Jobs:                     b.jobs,
		Selections:               b.selections,
		Frameworks:               b.frameworks,
		LegalHolds:               b.legalHolds,
		ReportPlans:              b.reportPlans,
		RestoreAccessVaults:      b.restoreAccessVaults,
		RestoreTestingPlans:      b.restoreTestingPlans,
		RestoreTestingSelections: b.restoreTestingSelections,
		MpaApprovals:             b.mpaApprovals,
		AccountID:                b.accountID,
		Region:                   b.region,
	}

	return persistence.MarshalSnapshot(ctx, "backup", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "backup", data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.applySnapshot(snap)
	b.rebuildARNIndexes()

	return nil
}

// ensureNonNil initialises any nil map fields to empty maps so callers never
// need to handle nil maps after a partial or legacy snapshot.
func (s *backendSnapshot) ensureNonNil() {
	if s.Vaults == nil {
		s.Vaults = make(map[string]*Vault)
	}
	if s.Plans == nil {
		s.Plans = make(map[string]*Plan)
	}
	if s.Jobs == nil {
		s.Jobs = make(map[string]*Job)
	}
	if s.Selections == nil {
		s.Selections = make(map[string]map[string]*Selection)
	}
	if s.Frameworks == nil {
		s.Frameworks = make(map[string]*Framework)
	}
	if s.LegalHolds == nil {
		s.LegalHolds = make(map[string]*LegalHold)
	}
	if s.ReportPlans == nil {
		s.ReportPlans = make(map[string]*ReportPlan)
	}
	if s.RestoreAccessVaults == nil {
		s.RestoreAccessVaults = make(map[string]*RestoreAccessVault)
	}
	if s.RestoreTestingPlans == nil {
		s.RestoreTestingPlans = make(map[string]*RestoreTestingPlan)
	}
	if s.RestoreTestingSelections == nil {
		s.RestoreTestingSelections = make(map[string]map[string]*RestoreTestingSelection)
	}
	if s.MpaApprovals == nil {
		s.MpaApprovals = make(map[string]string)
	}
}

// applySnapshot writes snapshot data into the backend. Must be called with mu held.
func (b *InMemoryBackend) applySnapshot(snap backendSnapshot) {
	b.vaults = snap.Vaults
	b.plans = snap.Plans
	b.jobs = snap.Jobs
	b.selections = snap.Selections
	b.frameworks = snap.Frameworks
	b.legalHolds = snap.LegalHolds
	b.reportPlans = snap.ReportPlans
	b.restoreAccessVaults = snap.RestoreAccessVaults
	b.restoreTestingPlans = snap.RestoreTestingPlans
	b.restoreTestingSelections = snap.RestoreTestingSelections
	b.mpaApprovals = snap.MpaApprovals
	b.accountID = snap.AccountID
	b.region = snap.Region
}

// rebuildARNIndexes reconstructs all O(1) lookup indexes from the current maps.
// Must be called with mu held.
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.vaultARNIndex = make(map[string]string, len(b.vaults))
	for name, v := range b.vaults {
		b.vaultARNIndex[v.BackupVaultArn] = name
	}

	b.planARNIndex = make(map[string]string, len(b.plans))
	b.planIDIndex = make(map[string]string, len(b.plans))
	for name, p := range b.plans {
		b.planARNIndex[p.BackupPlanArn] = name
		b.planIDIndex[p.BackupPlanID] = name
	}

	b.frameworkARNIndex = make(map[string]string, len(b.frameworks))
	for name, f := range b.frameworks {
		b.frameworkARNIndex[f.FrameworkArn] = name
	}

	b.reportPlanARNIndex = make(map[string]string, len(b.reportPlans))
	for name, rp := range b.reportPlans {
		b.reportPlanARNIndex[rp.ReportPlanArn] = name
	}

	// Ensure each RestoreTestingPlan has a corresponding selection map.
	for name := range b.restoreTestingPlans {
		if b.restoreTestingSelections[name] == nil {
			b.restoreTestingSelections[name] = make(map[string]*RestoreTestingSelection)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
