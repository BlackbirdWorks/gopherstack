package glue

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// BatchGetBlueprints retrieves multiple blueprints by name.
func (b *InMemoryBackend) BatchGetBlueprints(names []string) ([]*Blueprint, []string) {
	b.mu.RLock("BatchGetBlueprints")
	defer b.mu.RUnlock()

	found := make([]*Blueprint, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		bp, ok := b.blueprints.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		cp := *bp
		found = append(found, &cp)
	}

	return found, missing
}

// AddBlueprintInternal adds a blueprint directly to the backend without validation.
func (b *InMemoryBackend) AddBlueprintInternal(bp *Blueprint) {
	b.mu.Lock("AddBlueprintInternal")
	defer b.mu.Unlock()

	cp := *bp
	b.blueprints.Put(&cp)
}

var ErrBlueprintRunNotFound = fmt.Errorf("blueprint run not found: %w", ErrNotFound)

// CreateBlueprint stores a new blueprint.
func (b *InMemoryBackend) CreateBlueprint(name string) error {
	if name == "" {
		return fmt.Errorf("%w: blueprint Name is required", ErrValidation)
	}

	b.mu.Lock("CreateBlueprint")
	defer b.mu.Unlock()

	if b.blueprints.Has(name) {
		return fmt.Errorf("blueprint %q already exists: %w", name, ErrAlreadyExists)
	}

	b.blueprints.Put(&Blueprint{Name: name, Status: "ACTIVE"})

	return nil
}

// DeleteBlueprint removes a blueprint.
func (b *InMemoryBackend) DeleteBlueprint(name string) error {
	b.mu.Lock("DeleteBlueprint")
	defer b.mu.Unlock()

	if !b.blueprints.Has(name) {
		return fmt.Errorf("blueprint %q not found: %w", name, ErrNotFound)
	}

	b.blueprints.Delete(name)

	return nil
}

// UpdateBlueprint updates an existing blueprint.
func (b *InMemoryBackend) UpdateBlueprint(name string) (*Blueprint, error) {
	b.mu.Lock("UpdateBlueprint")
	defer b.mu.Unlock()

	bp, ok := b.blueprints.Get(name)
	if !ok {
		return nil, fmt.Errorf("blueprint %q not found: %w", name, ErrNotFound)
	}

	cp := *bp

	return &cp, nil
}

// ListBlueprints returns all blueprint names.
func (b *InMemoryBackend) ListBlueprints() []string {
	b.mu.RLock("ListBlueprints")
	defer b.mu.RUnlock()

	src := b.blueprints.Snapshot()
	names := make([]string, len(src))
	for i, bp := range src {
		names[i] = bp.Name
	}

	return names
}

// StartBlueprintRun creates a new blueprint run record.
func (b *InMemoryBackend) StartBlueprintRun(blueprintName string) (*BlueprintRun, error) {
	b.mu.Lock("StartBlueprintRun")
	defer b.mu.Unlock()

	if !b.blueprints.Has(blueprintName) {
		return nil, fmt.Errorf("blueprint %q not found: %w", blueprintName, ErrNotFound)
	}

	runID := "bp-run-" + uuid.NewString()[:8]
	run := &BlueprintRun{
		BlueprintName: blueprintName,
		RunID:         runID,
		WorkflowName:  "workflow-" + runID,
		State:         stateRunning,
		StartedOn:     time.Now().UTC(),
	}
	b.blueprintRuns.Put(run)

	cp := *run

	return &cp, nil
}

// GetBlueprintRun returns a blueprint run by ID.
func (b *InMemoryBackend) GetBlueprintRun(blueprintName, runID string) (*BlueprintRun, error) {
	b.mu.RLock("GetBlueprintRun")
	defer b.mu.RUnlock()

	run, ok := b.blueprintRuns.Get(runID)
	if !ok || (blueprintName != "" && run.BlueprintName != blueprintName) {
		return nil, ErrBlueprintRunNotFound
	}

	cp := *run

	return &cp, nil
}

// GetBlueprintRuns returns all runs for a blueprint.
func (b *InMemoryBackend) GetBlueprintRuns(blueprintName string) []*BlueprintRun {
	b.mu.RLock("GetBlueprintRuns")
	defer b.mu.RUnlock()

	var runs []*BlueprintRun
	for _, r := range b.blueprintRuns.All() {
		if blueprintName == "" || r.BlueprintName == blueprintName {
			cp := *r
			runs = append(runs, &cp)
		}
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn.Before(runs[k].StartedOn)
	})

	return runs
}
