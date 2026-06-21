package fis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type backendSnapshot struct {
	Templates            map[string]*ExperimentTemplate                    `json:"templates"`
	Experiments          map[string]*Experiment                            `json:"experiments"`
	TargetAccountConfigs map[string]map[string]*TargetAccountConfiguration `json:"targetAccountConfigs"`
	SafetyLever          *SafetyLever                                      `json:"safetyLever"`
	AccountID            string                                            `json:"accountID"`
	Region               string                                            `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Templates:            b.templates,
		Experiments:          b.experiments,
		TargetAccountConfigs: b.targetAccountConfigs,
		SafetyLever:          b.safetyLever,
		AccountID:            b.accountID,
		Region:               b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "fis: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It cancels any in-flight experiment goroutines before replacing state
// to prevent goroutine leaks when restored experiments have no cancel func.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	// Cancel any running goroutines before replacing state.
	b.mu.Lock("Restore-cancel")

	for _, exp := range b.experiments {
		if exp.cancel != nil {
			exp.cancel()
		}
	}

	b.mu.Unlock()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Templates == nil {
		snap.Templates = make(map[string]*ExperimentTemplate)
	}

	if snap.Experiments == nil {
		snap.Experiments = make(map[string]*Experiment)
	}

	if snap.TargetAccountConfigs == nil {
		snap.TargetAccountConfigs = make(map[string]map[string]*TargetAccountConfiguration)
	}

	b.templates = snap.Templates
	b.experiments = snap.Experiments
	b.targetAccountConfigs = snap.TargetAccountConfigs
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild or restore safety lever.
	if snap.SafetyLever != nil {
		b.safetyLever = snap.SafetyLever
	} else {
		safetyLeverARN := arn.Build("fis", b.region, b.accountID, "safety-lever/"+b.accountID)
		b.safetyLever = &SafetyLever{
			ID:    b.accountID,
			Arn:   safetyLeverARN,
			Tags:  make(map[string]string),
			State: SafetyLeverState{Status: statusDisengaged},
		}
	}

	// Rebuild ARN indexes from restored state.
	b.templateARNIndex = make(map[string]string, len(b.templates))
	for id, tpl := range b.templates {
		b.templateARNIndex[tpl.Arn] = id
	}

	b.experimentARNIndex = make(map[string]string, len(b.experiments))
	for id, exp := range b.experiments {
		b.experimentARNIndex[exp.Arn] = id
	}

	markRestoredExperimentsTerminal(b.experiments)

	return nil
}

// markRestoredExperimentsTerminal marks any non-terminal experiment as failed.
// Restored experiments have no cancel func, so they cannot be resumed.
func markRestoredExperimentsTerminal(experiments map[string]*Experiment) {
	now := time.Now()

	for _, exp := range experiments {
		if isActiveStatus(exp.Status.Status) {
			exp.Status = ExperimentStatus{Status: statusFailed, Reason: "restored from snapshot"}
			exp.EndTime = &now
		}
	}
}

// isActiveStatus returns true for non-terminal experiment statuses.
func isActiveStatus(s string) bool {
	switch s {
	case statusPending, statusInitiating, statusRunning, statusCompleting, statusStopping:
		return true
	}

	return false
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Restore(ctx, data)
	}

	return nil
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		mem.Reset()
	}
}
