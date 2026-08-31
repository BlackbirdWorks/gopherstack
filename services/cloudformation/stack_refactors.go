package cloudformation

import (
	"fmt"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateStackRefactor(
	description string,
	resourceMappings []ResourceMapping,
	enableStackCreation bool,
) (string, error) {
	b.mu.Lock("CreateStackRefactor")
	defer b.mu.Unlock()
	refactorID := uuid.New().String()
	b.stackRefactors.Put(&StackRefactor{
		RefactorID:          refactorID,
		Description:         description,
		Status:              "CREATE_COMPLETE",
		ResourceMappings:    resourceMappings,
		EnableStackCreation: enableStackCreation,
	})

	return refactorID, nil
}

func (b *InMemoryBackend) DescribeStackRefactor(stackRefactorID string) (*StackRefactor, error) {
	b.mu.RLock("DescribeStackRefactor")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		// Unlike CreateStackRefactor/List*, DescribeStackRefactor's SDK-modeled
		// error set includes StackRefactorNotFoundException — it is not
		// fire-and-forget, so an unknown ID must be a real error, not an empty 200.
		return nil, fmt.Errorf("%w: %s", ErrStackRefactorNotFound, stackRefactorID)
	}

	return r, nil
}

type stackRefactorMove struct {
	srcStack *Stack
	dstStack *Stack
	res      *StackResource
	mapping  ResourceMapping
}

// resolveStackRefactorMoves validates every mapping before any mutation, so a
// refactor either moves every resource or none of them.
func (b *InMemoryBackend) resolveStackRefactorMoves(mappings []ResourceMapping) ([]stackRefactorMove, error) {
	moves := make([]stackRefactorMove, 0, len(mappings))
	for _, m := range mappings {
		srcStack, ok := b.resolveStack(m.Source.StackName)
		if !ok {
			return nil, fmt.Errorf("%w: source stack %s", ErrStackNotFound, m.Source.StackName)
		}
		dstStack, ok := b.resolveStack(m.Destination.StackName)
		if !ok {
			return nil, fmt.Errorf("%w: destination stack %s", ErrStackNotFound, m.Destination.StackName)
		}
		res, ok := b.resources[srcStack.StackID][m.Source.LogicalResourceID]
		if !ok {
			return nil, fmt.Errorf(
				"%w: %s in stack %s", ErrResourceNotFound, m.Source.LogicalResourceID, m.Source.StackName,
			)
		}
		moves = append(moves, stackRefactorMove{srcStack: srcStack, dstStack: dstStack, res: res, mapping: m})
	}

	return moves, nil
}

// ExecuteStackRefactor moves each mapped resource out of its source stack's
// resource table and into its destination stack's — observable afterward
// through DescribeStackResources on both stacks.
func (b *InMemoryBackend) ExecuteStackRefactor(stackRefactorID string) error {
	b.mu.Lock("ExecuteStackRefactor")
	defer b.mu.Unlock()

	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStackRefactorNotFound, stackRefactorID)
	}

	moves, err := b.resolveStackRefactorMoves(r.ResourceMappings)
	if err != nil {
		return err
	}

	for _, mv := range moves {
		delete(b.resources[mv.srcStack.StackID], mv.mapping.Source.LogicalResourceID)

		moved := *mv.res
		moved.LogicalID = mv.mapping.Destination.LogicalResourceID
		moved.StackID = mv.dstStack.StackID
		moved.StackName = mv.dstStack.StackName

		if b.resources[mv.dstStack.StackID] == nil {
			b.resources[mv.dstStack.StackID] = make(map[string]*StackResource)
		}
		b.resources[mv.dstStack.StackID][moved.LogicalID] = &moved

		b.addEvent(
			mv.dstStack.StackID, mv.dstStack.StackName, moved.LogicalID, moved.PhysicalID, moved.Type,
			"UPDATE_COMPLETE", "Resource refactored from stack "+mv.srcStack.StackName,
		)
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
	actions := make([]StackRefactorAction, 0, len(r.ResourceMappings))
	for _, m := range r.ResourceMappings {
		var resType, physicalID string
		if srcStack, found := b.resolveStack(m.Source.StackName); found {
			if res, resFound := b.resources[srcStack.StackID][m.Source.LogicalResourceID]; resFound {
				resType = res.Type
				physicalID = res.PhysicalID
			}
		}
		actions = append(actions, StackRefactorAction{
			Action:             "MOVE",
			Description:        r.Description,
			StackName:          m.Destination.StackName,
			LogicalResourceID:  m.Destination.LogicalResourceID,
			PhysicalResourceID: physicalID,
			ResourceType:       resType,
			ResourceMapping:    m,
		})
	}

	return actions, nil
}
