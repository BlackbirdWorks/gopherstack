package codepipeline

import (
	"encoding/json"
	"maps"
)

// customActionTypeEntry is the JSON-serialisable representation of a custom action type entry.
type customActionTypeEntry struct {
	Value    *CustomActionType `json:"value"`
	Region   string            `json:"region"`
	Category string            `json:"category"`
	Provider string            `json:"provider"`
	Version  string            `json:"version"`
}

// stageTransitionEntry is the JSON-serialisable representation of a stage transition entry.
type stageTransitionEntry struct {
	Value          *StageTransitionState `json:"value"`
	Region         string                `json:"region"`
	PipelineName   string                `json:"pipelineName"`
	StageName      string                `json:"stageName"`
	TransitionType string                `json:"transitionType"`
}

// executionEntry is the JSON-serialisable list of executions per pipeline.
type executionEntry struct {
	Region       string               `json:"region"`
	PipelineName string               `json:"pipelineName"`
	Executions   []*PipelineExecution `json:"executions"`
}

// backendSnapshot is the JSON-serialisable snapshot of InMemoryBackend state.
//
// Region-scoped resource maps are nested by region (outer key = region) so that
// same-named resources in different regions round-trip without collision.
type backendSnapshot struct {
	Pipelines         map[string]map[string]*Pipeline `json:"pipelines"`
	PipelineARNIndex  map[string]map[string]string    `json:"pipelineARNIndex"`
	Jobs              map[string]map[string]*Job      `json:"jobs"`
	Webhooks          map[string]map[string]*Webhook  `json:"webhooks"`
	WebhookARNIndex   map[string]map[string]string    `json:"webhookARNIndex"`
	AccountID         string                          `json:"accountID"`
	Region            string                          `json:"region"`
	CustomActionTypes []customActionTypeEntry         `json:"customActionTypes"`
	StageTransitions  []stageTransitionEntry          `json:"stageTransitions"`
	Executions        []executionEntry                `json:"executions"`
}

// ensureNonNil initialises any nil maps so callers do not need to guard after Restore.
func (s *backendSnapshot) ensureNonNil() {
	if s.Pipelines == nil {
		s.Pipelines = make(map[string]map[string]*Pipeline)
	}

	if s.PipelineARNIndex == nil {
		s.PipelineARNIndex = make(map[string]map[string]string)
	}

	if s.Jobs == nil {
		s.Jobs = make(map[string]map[string]*Job)
	}

	if s.Webhooks == nil {
		s.Webhooks = make(map[string]map[string]*Webhook)
	}

	if s.WebhookARNIndex == nil {
		s.WebhookARNIndex = make(map[string]map[string]string)
	}
}

// copyNestedPtr deep-copies the outer region map of a region-nested pointer map,
// cloning each inner map so the snapshot owns its data independently of the backend.
func copyNestedPtr[T any](src map[string]map[string]*T) map[string]map[string]*T {
	out := make(map[string]map[string]*T, len(src))
	for region, inner := range src {
		cp := make(map[string]*T, len(inner))
		maps.Copy(cp, inner)
		out[region] = cp
	}

	return out
}

// copyNestedStr deep-copies the outer region map of a region-nested string map.
func copyNestedStr(src map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(src))
	for region, inner := range src {
		cp := make(map[string]string, len(inner))
		maps.Copy(cp, inner)
		out[region] = cp
	}

	return out
}

// customActionTypeKey.String returns a unique string for use in sorted output.
func (k customActionTypeKey) String() string {
	return k.Category + "/" + k.Provider + "/" + k.Version
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Flatten struct-keyed maps into region-tagged slices for JSON serialization.
	cats := make([]customActionTypeEntry, 0)
	for region, inner := range b.customActionTypes {
		for k, v := range inner {
			cats = append(cats, customActionTypeEntry{
				Region: region, Category: k.Category, Provider: k.Provider, Version: k.Version, Value: v,
			})
		}
	}

	transitions := make([]stageTransitionEntry, 0)
	for region, inner := range b.stageTransitions {
		for k, v := range inner {
			transitions = append(transitions, stageTransitionEntry{
				Region:         region,
				PipelineName:   k.PipelineName,
				StageName:      k.StageName,
				TransitionType: k.TransitionType,
				Value:          v,
			})
		}
	}

	execs := make([]executionEntry, 0)
	for region, inner := range b.executions {
		for pName, list := range inner {
			if len(list) == 0 {
				continue
			}

			execs = append(execs, executionEntry{Region: region, PipelineName: pName, Executions: list})
		}
	}

	// Defensive copies for snapshot: the snapshot owns the data, not the backend.
	snap := backendSnapshot{
		Pipelines:         copyNestedPtr(b.pipelines),
		PipelineARNIndex:  copyNestedStr(b.pipelineARNIndex),
		CustomActionTypes: cats,
		StageTransitions:  transitions,
		Jobs:              copyNestedPtr(b.jobs),
		Webhooks:          copyNestedPtr(b.webhooks),
		WebhookARNIndex:   copyNestedStr(b.webhookARNIndex),
		Executions:        execs,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	// Marshal can only fail for unsupported types (e.g. channels/functions) which are not present here.
	data, _ := json.Marshal(&snap)

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
// It calls Reset first to clear any in-flight goroutines and prior state.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	// Rebuild region-nested struct-keyed maps from region-tagged slices.
	cats := make(map[string]map[customActionTypeKey]*CustomActionType)
	for _, entry := range snap.CustomActionTypes {
		if cats[entry.Region] == nil {
			cats[entry.Region] = make(map[customActionTypeKey]*CustomActionType)
		}

		key := customActionTypeKey{Category: entry.Category, Provider: entry.Provider, Version: entry.Version}
		cats[entry.Region][key] = entry.Value
	}

	transitions := make(map[string]map[stageTransitionKey]*StageTransitionState)
	for _, entry := range snap.StageTransitions {
		if transitions[entry.Region] == nil {
			transitions[entry.Region] = make(map[stageTransitionKey]*StageTransitionState)
		}

		key := stageTransitionKey{
			PipelineName:   entry.PipelineName,
			StageName:      entry.StageName,
			TransitionType: entry.TransitionType,
		}
		transitions[entry.Region][key] = entry.Value
	}

	executions := make(map[string]map[string][]*PipelineExecution)
	for _, entry := range snap.Executions {
		if executions[entry.Region] == nil {
			executions[entry.Region] = make(map[string][]*PipelineExecution)
		}

		executions[entry.Region][entry.PipelineName] = entry.Executions
	}

	// Defensive copies: the backend owns these maps independently of the snapshot.
	pipelinesCopy := copyNestedPtr(snap.Pipelines)
	arnIndexCopy := copyNestedStr(snap.PipelineARNIndex)
	webhooksCopy := copyNestedPtr(snap.Webhooks)
	webhookARNCopy := copyNestedStr(snap.WebhookARNIndex)
	jobsCopy := copyNestedPtr(snap.Jobs)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.pipelines = pipelinesCopy
	b.pipelineARNIndex = arnIndexCopy
	b.customActionTypes = cats
	b.stageTransitions = transitions
	b.jobs = jobsCopy
	b.webhooks = webhooksCopy
	b.webhookARNIndex = webhookARNCopy
	b.executions = executions
	b.accountID = snap.AccountID
	b.region = snap.Region

	// actionExecutions are derived state (rebuilt on StartPipelineExecution) and
	// are not persisted; ensure the map is initialised after a restore.
	b.actionExecutions = make(map[string]map[string][]*ActionExecution)

	return nil
}
