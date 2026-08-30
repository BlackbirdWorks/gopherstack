package glue

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// cloneBlueprint returns a deep copy of a Blueprint.
func cloneBlueprint(bp *Blueprint) *Blueprint {
	cp := *bp
	cp.Tags = maps.Clone(bp.Tags)

	return &cp
}

// blueprintARN returns the ARN for a Glue blueprint.
func (b *InMemoryBackend) blueprintARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "blueprint/"+name)
}

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

		found = append(found, cloneBlueprint(bp))
	}

	return found, missing
}

// AddBlueprintInternal adds a blueprint directly to the backend without validation.
func (b *InMemoryBackend) AddBlueprintInternal(bp *Blueprint) {
	b.mu.Lock("AddBlueprintInternal")
	defer b.mu.Unlock()

	b.blueprints.Put(cloneBlueprint(bp))
}

var ErrBlueprintRunNotFound = fmt.Errorf("blueprint run not found: %w", ErrNotFound)

// CreateBlueprint stores a new blueprint. blueprintLocation mirrors
// CreateBlueprintInput's required BlueprintLocation (the S3 path where the
// blueprint is published).
func (b *InMemoryBackend) CreateBlueprint(
	name, blueprintLocation, description string,
	tags map[string]string,
) (*Blueprint, error) {
	if name == "" || blueprintLocation == "" {
		return nil, fmt.Errorf("%w: Name and BlueprintLocation are required", ErrValidation)
	}

	b.mu.Lock("CreateBlueprint")
	defer b.mu.Unlock()

	if b.blueprints.Has(name) {
		return nil, fmt.Errorf("blueprint %q already exists: %w", name, ErrAlreadyExists)
	}

	now := float64(time.Now().Unix())
	bp := &Blueprint{
		Name:                     name,
		Status:                   "ACTIVE",
		BlueprintLocation:        blueprintLocation,
		BlueprintServiceLocation: "s3://glue-blueprints-" + b.accountID + "/" + name,
		Description:              description,
		Tags:                     maps.Clone(tags),
		CreatedOn:                now,
		LastModifiedOn:           now,
	}
	b.blueprints.Put(bp)

	return cloneBlueprint(bp), nil
}

// DeleteBlueprint removes a blueprint. Its error switch has no
// EntityNotFoundException case, unlike GetBlueprint's, so an unknown Name
// surfaces as InvalidInputException.
func (b *InMemoryBackend) DeleteBlueprint(name string) error {
	b.mu.Lock("DeleteBlueprint")
	defer b.mu.Unlock()

	if !b.blueprints.Has(name) {
		return fmt.Errorf("blueprint %q not found: %w", name, ErrValidation)
	}

	b.blueprints.Delete(name)

	return nil
}

// UpdateBlueprint updates an existing blueprint's BlueprintLocation and
// Description, mirroring UpdateBlueprintInput.
func (b *InMemoryBackend) UpdateBlueprint(name, blueprintLocation, description string) (*Blueprint, error) {
	if blueprintLocation == "" {
		return nil, fmt.Errorf("%w: BlueprintLocation is required", ErrValidation)
	}

	b.mu.Lock("UpdateBlueprint")
	defer b.mu.Unlock()

	bp, ok := b.blueprints.Get(name)
	if !ok {
		return nil, fmt.Errorf("blueprint %q not found: %w", name, ErrNotFound)
	}

	bp.BlueprintLocation = blueprintLocation
	bp.Description = description
	bp.LastModifiedOn = float64(time.Now().Unix())

	return cloneBlueprint(bp), nil
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
func (b *InMemoryBackend) StartBlueprintRun(blueprintName, roleARN, parameters string) (*BlueprintRun, error) {
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
		RoleARN:       roleARN,
		Parameters:    parameters,
		StartedOn:     float64(time.Now().Unix()),
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
		if runs[i].StartedOn != runs[k].StartedOn {
			return runs[i].StartedOn < runs[k].StartedOn
		}

		return runs[i].RunID < runs[k].RunID
	})

	return runs
}
