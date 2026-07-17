package codepipeline

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreatePipeline creates a new CodePipeline pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	decl PipelineDeclaration,
	tags map[string]string,
) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.pipelines.Has(regionKey(region, decl.Name)) {
		return nil, fmt.Errorf("%w: pipeline %q already exists", ErrPipelineNameInUse, decl.Name)
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	if decl.Version == 0 {
		decl.Version = 1
	}

	if decl.PipelineType == "" {
		decl.PipelineType = PipelineTypeV1
	}

	if decl.ExecutionMode == "" {
		decl.ExecutionMode = ExecutionModeSuperseded
	}

	p := &Pipeline{
		region:      region,
		Declaration: decl,
		Metadata: PipelineMetadata{
			PipelineArn: b.buildPipelineARN(region, decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	b.pipelines.Put(p)

	return copyPipeline(p), nil
}

// GetPipeline returns the pipeline with the given name.
func (b *InMemoryBackend) GetPipeline(ctx context.Context, name string) (*Pipeline, error) {
	b.mu.RLock("GetPipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines.Get(regionKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	return copyPipeline(p), nil
}

// UpdatePipeline replaces the pipeline declaration.
// If decl.Version is non-zero it must match the current version (optimistic concurrency).
func (b *InMemoryBackend) UpdatePipeline(ctx context.Context, decl PipelineDeclaration) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(regionKey(getRegion(ctx, b.region), decl.Name))
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, decl.Name)
	}

	if decl.Version != 0 && decl.Version != p.Declaration.Version {
		return nil, fmt.Errorf("%w: pipeline %q version mismatch: got %d, current %d",
			ErrConflict, decl.Name, decl.Version, p.Declaration.Version)
	}

	currentVersion := p.Declaration.Version
	p.Declaration = decl
	p.Declaration.Version = currentVersion + 1
	p.Metadata.Updated = float64(time.Now().Unix())

	return copyPipeline(p), nil
}

// DeletePipeline removes the pipeline with the given name and cleans up associated state.
func (b *InMemoryBackend) DeletePipeline(ctx context.Context, name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, name)

	if !b.pipelines.Has(key) {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	b.pipelines.Delete(key)
	delete(b.executionsStore(region), name)
	delete(b.actionExecutionsStore(region), name)

	// Cascade: remove disabled stage transitions for this pipeline.
	for _, st := range slices.Clone(b.stageTransitionsByPipeline.Get(regionKey(region, name))) {
		b.stageTransitions.Delete(stageTransitionKeyFn(st))
	}

	return nil
}

// ListPipelines returns a sorted summary of all pipelines in the request region.
func (b *InMemoryBackend) ListPipelines(ctx context.Context) []PipelineSummary {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	entries := b.pipelinesByRegion.Get(region)

	summaries := make([]PipelineSummary, 0, len(entries))
	for _, p := range entries {
		summaries = append(summaries, PipelineSummary{
			Name:          p.Declaration.Name,
			Version:       p.Declaration.Version,
			PipelineType:  p.Declaration.PipelineType,
			ExecutionMode: p.Declaration.ExecutionMode,
			Created:       p.Metadata.Created,
			Updated:       p.Metadata.Updated,
			PipelineArn:   p.Metadata.PipelineArn,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	return summaries
}

func copyPipeline(p *Pipeline) *Pipeline {
	tagsCopy := make(map[string]string, len(p.Tags))
	maps.Copy(tagsCopy, p.Tags)

	out := *p
	out.Tags = tagsCopy
	out.Declaration = copyDeclaration(p.Declaration)

	return &out
}

// AddPipelineInternal seeds a pipeline directly into the backend's default region (for testing).
func (b *InMemoryBackend) AddPipelineInternal(decl PipelineDeclaration, tags map[string]string) *Pipeline {
	b.mu.Lock("AddPipelineInternal")
	defer b.mu.Unlock()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	if decl.Version == 0 {
		decl.Version = 1
	}

	p := &Pipeline{
		region:      b.region,
		Declaration: decl,
		Metadata: PipelineMetadata{
			PipelineArn: b.buildPipelineARN(b.region, decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	b.pipelines.Put(p)

	return copyPipeline(p)
}

// copyDeclaration deep-copies a PipelineDeclaration so callers cannot mutate
// the backend's stored stages, actions, or configuration maps.
func copyDeclaration(d PipelineDeclaration) PipelineDeclaration {
	out := d
	out.Stages = copyStages(d.Stages)
	out.Variables = copyVariables(d.Variables)
	out.Triggers = copyTriggers(d.Triggers)

	if d.ArtifactStores != nil {
		out.ArtifactStores = make(map[string]ArtifactStore, len(d.ArtifactStores))
		maps.Copy(out.ArtifactStores, d.ArtifactStores)
	}

	return out
}

func copyVariables(vars []PipelineVariable) []PipelineVariable {
	if vars == nil {
		return nil
	}

	out := make([]PipelineVariable, len(vars))
	copy(out, vars)

	return out
}

func copyTriggers(triggers []Trigger) []Trigger {
	if triggers == nil {
		return nil
	}

	out := make([]Trigger, len(triggers))
	copy(out, triggers)

	return out
}

func copyStages(stages []Stage) []Stage {
	if stages == nil {
		return nil
	}

	out := make([]Stage, len(stages))
	for i, s := range stages {
		out[i] = Stage{
			Name:        s.Name,
			Type:        s.Type,
			Actions:     copyActions(s.Actions),
			BeforeEntry: copyCondition(s.BeforeEntry),
			OnFailure:   copyCondition(s.OnFailure),
			OnSuccess:   copyCondition(s.OnSuccess),
		}
	}

	return out
}

func copyCondition(c *Condition) *Condition {
	if c == nil {
		return nil
	}

	cp := *c
	if c.Rules != nil {
		cp.Rules = make([]Rule, len(c.Rules))
		copy(cp.Rules, c.Rules)
	}

	return &cp
}

func copyActions(actions []Action) []Action {
	if actions == nil {
		return nil
	}

	out := make([]Action, len(actions))
	for i, a := range actions {
		actionCopy := a
		actionCopy.Configuration = copyStringMap(a.Configuration)
		actionCopy.InputArtifacts = copyArtifactRefs(a.InputArtifacts)
		actionCopy.OutputArtifacts = copyArtifactRefs(a.OutputArtifacts)
		out[i] = actionCopy
	}

	return out
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func copyArtifactRefs(refs []ArtifactRef) []ArtifactRef {
	if refs == nil {
		return nil
	}

	out := make([]ArtifactRef, len(refs))
	copy(out, refs)

	return out
}

// StartPipelineExecution starts and stores a new execution of a pipeline.
func (b *InMemoryBackend) StartPipelineExecution(ctx context.Context, pipelineName string) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return nil, ErrNotFound
	}

	exec := &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: uuid.NewString(),
		Status:              statusInProgress,
		PipelineVersion:     p.Declaration.Version,
	}

	execs := b.executionsStore(region)
	execs[pipelineName] = append(execs[pipelineName], exec)

	// Record an action execution for every action in the pipeline so that
	// ListActionExecutions reflects the work performed by this execution.
	now := time.Now().UTC()

	actionExecs := b.actionExecutionsStore(region)

	for _, stage := range p.Declaration.Stages {
		for _, action := range stage.Actions {
			ae := &ActionExecution{
				PipelineExecutionID: exec.PipelineExecutionID,
				ActionExecutionID:   uuid.NewString(),
				StageName:           stage.Name,
				ActionName:          action.Name,
				Status:              statusSucceeded,
				StartTime:           now,
				LastUpdateTime:      now,
			}
			actionExecs[pipelineName] = append(actionExecs[pipelineName], ae)
		}
	}

	// gopherstack runs every action synchronously and instantaneously (the
	// loop above already marks each action execution Succeeded), so the
	// pipeline execution itself is done by the time this call returns.
	// Leaving Status at statusInProgress here left every execution stuck
	// InProgress forever: GetPipelineExecution/ListPipelineExecutions would
	// never report a terminal status, so any client polling for completion
	// (as the real, asynchronous AWS service expects callers to do) would
	// spin indefinitely.
	exec.Status = statusSucceeded

	cp := *exec

	return &cp, nil
}

// StopPipelineExecution stops a pipeline execution. Real AWS transitions
// through a transient "Stopping" state while in-progress actions finish (or
// are abandoned, if abandon is true) before reaching the terminal "Stopped"
// state. gopherstack runs every action synchronously and instantaneously (see
// StartPipelineExecution), so there is never an in-progress action left to
// wait for by the time a client can call this -- the execution goes straight
// to "Stopped" regardless of abandon. Leaving it at "Stopping" left every
// stopped execution stuck there forever, indistinguishable (to a polling
// client) from a stop request that never completed.
func (b *InMemoryBackend) StopPipelineExecution(
	ctx context.Context,
	pipelineName, executionID, reason string,
	abandon bool,
) (*PipelineExecution, error) {
	b.mu.Lock("StopPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if !b.pipelines.Has(regionKey(region, pipelineName)) {
		return nil, ErrNotFound
	}

	_, _ = reason, abandon

	for _, exec := range b.executionsStore(region)[pipelineName] {
		if exec.PipelineExecutionID == executionID {
			exec.Status = statusStopped
			cp := *exec

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: pipeline %q execution %q", ErrExecutionNotFound, pipelineName, executionID)
}
