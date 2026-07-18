package cloudformation

import (
	"fmt"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateStackRefactor(
	description string,
	stackDefinitions []string,
) (string, error) {
	b.mu.Lock("CreateStackRefactor")
	defer b.mu.Unlock()
	refactorID := uuid.New().String()
	b.stackRefactors.Put(&StackRefactor{
		RefactorID:       refactorID,
		Description:      description,
		Status:           "CREATE_COMPLETE",
		StackDefinitions: stackDefinitions,
	})

	return refactorID, nil
}

func (b *InMemoryBackend) DescribeStackRefactor(stackRefactorID string) (string, error) {
	b.mu.RLock("DescribeStackRefactor")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		// Unlike CreateStackRefactor/ExecuteStackRefactor/List*, DescribeStackRefactor's
		// SDK-modeled error set includes StackRefactorNotFoundException — it is not
		// fire-and-forget, so an unknown ID must be a real error, not an empty 200.
		return "", fmt.Errorf("%w: %s", ErrStackRefactorNotFound, stackRefactorID)
	}

	return r.Status, nil
}

func (b *InMemoryBackend) ExecuteStackRefactor(stackRefactorID string) error {
	b.mu.Lock("ExecuteStackRefactor")
	defer b.mu.Unlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return nil
	}
	r.Status = "EXECUTE_COMPLETE"

	return nil
}

func (b *InMemoryBackend) ListStackRefactors(_ string) ([]StackRefactorSummary, error) {
	b.mu.RLock("ListStackRefactors")
	defer b.mu.RUnlock()
	summaries := make([]StackRefactorSummary, 0, b.stackRefactors.Len())
	for _, r := range b.stackRefactors.All() {
		summaries = append(summaries, StackRefactorSummary{
			StackRefactorID: r.RefactorID,
			Status:          r.Status,
			Description:     r.Description,
		})
	}

	return summaries, nil
}

func (b *InMemoryBackend) ListStackRefactorActions(
	stackRefactorID string,
) ([]StackRefactorAction, error) {
	b.mu.RLock("ListStackRefactorActions")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return []StackRefactorAction{}, nil
	}
	actions := make([]StackRefactorAction, 0, len(r.StackDefinitions))
	for _, def := range r.StackDefinitions {
		actions = append(actions, StackRefactorAction{
			Action:      "MOVE",
			Description: def,
		})
	}

	return actions, nil
}
