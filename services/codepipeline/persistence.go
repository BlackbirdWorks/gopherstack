package codepipeline

import "encoding/json"

// customActionTypeEntry is the JSON-serialisable representation of a custom action type entry.
type customActionTypeEntry struct {
	Value    *CustomActionType `json:"value"`
	Category string            `json:"category"`
	Provider string            `json:"provider"`
	Version  string            `json:"version"`
}

// stageTransitionEntry is the JSON-serialisable representation of a stage transition entry.
type stageTransitionEntry struct {
	Value          *StageTransitionState `json:"value"`
	PipelineName   string                `json:"pipelineName"`
	StageName      string                `json:"stageName"`
	TransitionType string                `json:"transitionType"`
}

// backendSnapshot is the JSON-serialisable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Pipelines         map[string]*Pipeline    `json:"pipelines"`
	PipelineARNIndex  map[string]string       `json:"pipelineARNIndex"`
	Jobs              map[string]*Job         `json:"jobs"`
	Webhooks          map[string]*Webhook     `json:"webhooks"`
	AccountID         string                  `json:"accountID"`
	Region            string                  `json:"region"`
	CustomActionTypes []customActionTypeEntry `json:"customActionTypes"`
	StageTransitions  []stageTransitionEntry  `json:"stageTransitions"`
}

// ensureNonNil initialises any nil maps so callers do not need to guard after Restore.
func (s *backendSnapshot) ensureNonNil() {
	if s.Pipelines == nil {
		s.Pipelines = make(map[string]*Pipeline)
	}

	if s.PipelineARNIndex == nil {
		s.PipelineARNIndex = make(map[string]string)
	}

	if s.Jobs == nil {
		s.Jobs = make(map[string]*Job)
	}

	if s.Webhooks == nil {
		s.Webhooks = make(map[string]*Webhook)
	}
}

// customActionTypeKey.String returns a unique string for use in sorted output.
func (k customActionTypeKey) String() string {
	return k.Category + "/" + k.Provider + "/" + k.Version
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Flatten struct-keyed maps into slices for JSON serialization.
	cats := make([]customActionTypeEntry, 0, len(b.customActionTypes))
	for k, v := range b.customActionTypes {
		cats = append(cats, customActionTypeEntry{
			Category: k.Category, Provider: k.Provider, Version: k.Version, Value: v,
		})
	}

	transitions := make([]stageTransitionEntry, 0, len(b.stageTransitions))
	for k, v := range b.stageTransitions {
		transitions = append(transitions, stageTransitionEntry{
			PipelineName:   k.PipelineName,
			StageName:      k.StageName,
			TransitionType: k.TransitionType,
			Value:          v,
		})
	}

	snap := backendSnapshot{
		Pipelines:         b.pipelines,
		PipelineARNIndex:  b.pipelineARNIndex,
		CustomActionTypes: cats,
		StageTransitions:  transitions,
		Jobs:              b.jobs,
		Webhooks:          b.webhooks,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	// Marshal can only fail for unsupported types (e.g. channels/functions) which are not present here.
	data, _ := json.Marshal(&snap)

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	// Rebuild struct-keyed maps from slices.
	cats := make(map[customActionTypeKey]*CustomActionType, len(snap.CustomActionTypes))
	for _, entry := range snap.CustomActionTypes {
		key := customActionTypeKey{Category: entry.Category, Provider: entry.Provider, Version: entry.Version}
		cats[key] = entry.Value
	}

	transitions := make(map[stageTransitionKey]*StageTransitionState, len(snap.StageTransitions))
	for _, entry := range snap.StageTransitions {
		key := stageTransitionKey{
			PipelineName:   entry.PipelineName,
			StageName:      entry.StageName,
			TransitionType: entry.TransitionType,
		}
		transitions[key] = entry.Value
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.pipelines = snap.Pipelines
	b.pipelineARNIndex = snap.PipelineARNIndex
	b.customActionTypes = cats
	b.stageTransitions = transitions
	b.jobs = snap.Jobs
	b.webhooks = snap.Webhooks
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
