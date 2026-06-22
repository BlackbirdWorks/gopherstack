package ce

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	CostCategories       map[string]*CostCategory        `json:"costCategories"`
	AnomalyMonitors      map[string]*AnomalyMonitor      `json:"anomalyMonitors"`
	AnomalySubscriptions map[string]*AnomalySubscription `json:"anomalySubscriptions"`
	Anomalies            map[string]*Anomaly             `json:"anomalies"`
	CostAllocationTags   map[string]*CostAllocationTag   `json:"costAllocationTags"`
	CommitmentAnalyses   map[string]*CommitmentAnalysis  `json:"commitmentAnalyses"`
	AccountID            string                          `json:"accountID"`
	Region               string                          `json:"region"`
	BackfillJobs         []*BackfillJob                  `json:"backfillJobs"`
	AnomalyTTL           time.Duration                   `json:"anomalyTTL"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		CostCategories:       b.costCategories,
		AnomalyMonitors:      b.anomalyMonitors,
		AnomalySubscriptions: b.anomalySubscriptions,
		Anomalies:            b.anomalies,
		CostAllocationTags:   b.costAllocationTags,
		CommitmentAnalyses:   b.commitmentAnalyses,
		BackfillJobs:         b.backfillJobs,
		AccountID:            b.accountID,
		Region:               b.region,
		AnomalyTTL:           b.anomalyTTL,
	}

	return persistence.MarshalSnapshot(ctx, "ce", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ce", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.CostCategories == nil {
		snap.CostCategories = make(map[string]*CostCategory)
	}

	if snap.AnomalyMonitors == nil {
		snap.AnomalyMonitors = make(map[string]*AnomalyMonitor)
	}

	if snap.AnomalySubscriptions == nil {
		snap.AnomalySubscriptions = make(map[string]*AnomalySubscription)
	}

	if snap.Anomalies == nil {
		snap.Anomalies = make(map[string]*Anomaly)
	}

	if snap.CostAllocationTags == nil {
		snap.CostAllocationTags = make(map[string]*CostAllocationTag)
	}

	if snap.CommitmentAnalyses == nil {
		snap.CommitmentAnalyses = make(map[string]*CommitmentAnalysis)
	}

	b.costCategories = snap.CostCategories
	b.anomalyMonitors = snap.AnomalyMonitors
	b.anomalySubscriptions = snap.AnomalySubscriptions
	b.anomalies = snap.Anomalies
	b.costAllocationTags = snap.CostAllocationTags
	b.commitmentAnalyses = snap.CommitmentAnalyses
	b.backfillJobs = snap.BackfillJobs
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Default to DefaultAnomalyTTL when restoring from a pre-AnomalyTTL snapshot
	// to avoid evicting every anomaly on Restore (zero TTL is "evict everything").
	if snap.AnomalyTTL > 0 {
		b.anomalyTTL = snap.AnomalyTTL
	} else {
		b.anomalyTTL = DefaultAnomalyTTL
	}

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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
