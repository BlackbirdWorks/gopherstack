package backup

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

// CreateBackupSelection creates a backup selection for a plan.
func (b *InMemoryBackend) CreateBackupSelection(
	planID, selectionName, iamRoleArn string,
	resources, notResources []string,
	listOfTags []TagCondition,
	conditions *SelectionConditions,
) (*Selection, error) {
	b.mu.Lock("CreateBackupSelection")
	defer b.mu.Unlock()

	if selectionName == "" {
		return nil, fmt.Errorf("%w: SelectionName is required", ErrValidation)
	}

	// Resolve planID: accept either a plan ID (from planIDIndex) or a plan name.
	if _, found := b.planIDIndex[planID]; !found {
		// planID is not a known ID — try it as a plan name.
		p, exists := b.plans.Get(planID)
		if !exists {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
		// Switch planID to the canonical UUID stored on the plan.
		planID = p.BackupPlanID
	}

	selectionID := uuid.NewString()
	sel := &Selection{
		SelectionID:   selectionID,
		SelectionName: selectionName,
		BackupPlanID:  planID,
		IAMRoleArn:    iamRoleArn,
		Resources:     resources,
		NotResources:  notResources,
		ListOfTags:    listOfTags,
		Conditions:    conditions,
		CreationTime:  time.Now().UTC(),
	}
	b.selections.Put(sel)
	cp := *sel

	return &cp, nil
}

// GetBackupSelection returns a specific backup selection.
func (b *InMemoryBackend) GetBackupSelection(planID, selectionID string) (*Selection, error) {
	b.mu.RLock("GetBackupSelection")
	defer b.mu.RUnlock()

	// Resolve planID to canonical ID if needed.
	if _, found := b.planIDIndex[planID]; !found {
		if p, exists := b.plans.Get(planID); exists {
			planID = p.BackupPlanID
		} else {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
	}

	sel, ok := b.selections.Get(selectionKey(planID, selectionID))
	if !ok {
		return nil, fmt.Errorf("%w: backup selection %s not found", ErrNotFound, selectionID)
	}

	cp := *sel

	return &cp, nil
}

// ListBackupSelections returns all backup selections for a plan.
func (b *InMemoryBackend) ListBackupSelections(planID string) ([]*Selection, error) {
	b.mu.RLock("ListBackupSelections")
	defer b.mu.RUnlock()

	// Resolve planID.
	if _, found := b.planIDIndex[planID]; !found {
		if p, exists := b.plans.Get(planID); exists {
			planID = p.BackupPlanID
		} else {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
	}

	sels := b.selectionsByPlan.Get(planID)
	list := make([]*Selection, 0, len(sels))
	for _, sel := range sels {
		cp := *sel
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Selection) int {
		if a.SelectionName < b.SelectionName {
			return -1
		}
		if a.SelectionName > b.SelectionName {
			return 1
		}

		return 0
	})

	return list, nil
}

// DeleteBackupSelection deletes a backup selection.
func (b *InMemoryBackend) DeleteBackupSelection(planID, selectionID string) error {
	b.mu.Lock("DeleteBackupSelection")
	defer b.mu.Unlock()

	// Resolve planID.
	if _, found := b.planIDIndex[planID]; !found {
		if p, exists := b.plans.Get(planID); exists {
			planID = p.BackupPlanID
		} else {
			return fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
	}

	key := selectionKey(planID, selectionID)
	if !b.selections.Has(key) {
		return fmt.Errorf("%w: backup selection %s not found", ErrNotFound, selectionID)
	}

	b.selections.Delete(key)

	return nil
}

// --- Copy Job methods ---
