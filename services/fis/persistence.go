package fis

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
func (b *InMemoryBackend) Snapshot() []byte {
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
		slog.Default().Warn("fis: Snapshot marshal failure", "error", err)

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
			State: SafetyLeverState{Status: "disengaged"},
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

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Restore(data)
	}

	return nil
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		mem.Reset()
	}
}
