package codepipeline

import (
	"encoding/json"
	"maps"
)

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

// executionEntry is the JSON-serialisable list of executions per pipeline.
type executionEntry struct {
	PipelineName string               `json:"pipelineName"`
	Executions   []*PipelineExecution `json:"executions"`
}

// backendSnapshot is the JSON-serialisable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Pipelines        map[string]*Pipeline    `json:"pipelines"`
	PipelineARNIndex map[string]string       `json:"pipelineARNIndex"`
	Jobs             map[string]*Job         `json:"jobs"`
	Webhooks         map[string]*Webhook     `json:"webhooks"`
	WebhookARNIndex  map[string]string       `json:"webhookARNIndex"`
	AccountID        string                  `json:"accountID"`
	Region           string                  `json:"region"`
	CustomActionTypes []customActionTypeEntry `json:"customActionTypes"`
	StageTransitions  []stageTransitionEntry  `json:"stageTransitions"`
	Executions        []executionEntry        `json:"executions"`
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

	if s.WebhookARNIndex == nil {
		s.WebhookARNIndex = make(map[string]string)
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

	execs := make([]executionEntry, 0, len(b.executions))
	for pName, list := range b.executions {
		if len(list) == 0 {
			continue
		}

		execs = append(execs, executionEntry{PipelineName: pName, Executions: list})
	}

	// Defensive copies for snapshot: the snapshot owns the data, not the backend.
	pipelinesCopy := make(map[string]*Pipeline, len(b.pipelines))
	maps.Copy(pipelinesCopy, b.pipelines)

	arnIndexCopy := make(map[string]string, len(b.pipelineARNIndex))
	maps.Copy(arnIndexCopy, b.pipelineARNIndex)

	webhooksCopy := make(map[string]*Webhook, len(b.webhooks))
	maps.Copy(webhooksCopy, b.webhooks)

	webhookARNCopy := make(map[string]string, len(b.webhookARNIndex))
	maps.Copy(webhookARNCopy, b.webhookARNIndex)

	jobsCopy := make(map[string]*Job, len(b.jobs))
	maps.Copy(jobsCopy, b.jobs)

	snap := backendSnapshot{
		Pipelines:         pipelinesCopy,
		PipelineARNIndex:  arnIndexCopy,
		CustomActionTypes: cats,
		StageTransitions:  transitions,
		Jobs:              jobsCopy,
		Webhooks:          webhooksCopy,
		WebhookARNIndex:   webhookARNCopy,
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

	executions := make(map[string][]*PipelineExecution, len(snap.Executions))
	for _, entry := range snap.Executions {
		executions[entry.PipelineName] = entry.Executions
	}

	// Defensive copies: the backend owns these maps independently of the snapshot.
	pipelinesCopy := make(map[string]*Pipeline, len(snap.Pipelines))
	maps.Copy(pipelinesCopy, snap.Pipelines)

	arnIndexCopy := make(map[string]string, len(snap.PipelineARNIndex))
	maps.Copy(arnIndexCopy, snap.PipelineARNIndex)

	webhooksCopy := make(map[string]*Webhook, len(snap.Webhooks))
	maps.Copy(webhooksCopy, snap.Webhooks)

	webhookARNCopy := make(map[string]string, len(snap.WebhookARNIndex))
	maps.Copy(webhookARNCopy, snap.WebhookARNIndex)

	jobsCopy := make(map[string]*Job, len(snap.Jobs))
	maps.Copy(jobsCopy, snap.Jobs)

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

	return nil
}
