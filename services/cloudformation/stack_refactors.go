package cloudformation

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) CreateStackRefactor(
	description string,
	stackDefinitions []StackDefinition,
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
		StackDefinitions:    stackDefinitions,
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

// createMissingRefactorStacks creates, from r.StackDefinitions, any mapping
// destination stack that doesn't already exist. Only reached when
// EnableStackCreation is set (CreateStackRefactorInput.StackDefinitions,
// cloudformation@v1.76.1 api_op_CreateStackRefactor.go); without it, a
// missing destination stays a genuine ErrStackNotFound, matching AWS.
func (b *InMemoryBackend) createMissingRefactorStacks(ctx context.Context, r *StackRefactor) error {
	defs := make(map[string]StackDefinition, len(r.StackDefinitions))
	for _, d := range r.StackDefinitions {
		defs[d.StackName] = d
	}
	created := make(map[string]bool)
	for _, m := range r.ResourceMappings {
		name := m.Destination.StackName
		if created[name] {
			continue
		}
		if _, ok := b.resolveStack(name); ok {
			continue
		}
		def, ok := defs[name]
		if !ok {
			continue
		}
		if _, err := b.createStackLocked(ctx, name, def.TemplateBody, nil, StackOptions{}, ""); err != nil {
			return fmt.Errorf("creating refactor destination stack %s: %w", name, err)
		}
		created[name] = true
	}

	return nil
}

// ExecuteStackRefactor moves each mapped resource out of its source stack's
// resource table and into its destination stack's — observable afterward
// through DescribeStackResources on both stacks.
func (b *InMemoryBackend) ExecuteStackRefactor(ctx context.Context, stackRefactorID string) error {
	b.mu.Lock("ExecuteStackRefactor")
	defer b.mu.Unlock()

	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStackRefactorNotFound, stackRefactorID)
	}

	if r.EnableStackCreation {
		if err := b.createMissingRefactorStacks(ctx, r); err != nil {
			return err
		}
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

// ListStackRefactors returns stack refactors, paginated by
// MaxResults/NextToken (real query-protocol form fields,
// api_op_ListStackRefactors.go). Snapshot (not All) for a deterministic,
// sortable-by-RefactorID order -- required for stable pagination.
func (b *InMemoryBackend) ListStackRefactors(
	maxResults int, nextToken string,
) (page.Page[StackRefactorSummary], error) {
	b.mu.RLock("ListStackRefactors")
	defer b.mu.RUnlock()
	summaries := make([]StackRefactorSummary, 0, b.stackRefactors.Len())
	for _, r := range b.stackRefactors.Snapshot() {
		summaries = append(summaries, StackRefactorSummary{
			StackRefactorID: r.RefactorID,
			Status:          r.Status,
			Description:     r.Description,
		})
	}

	return page.New(summaries, nextToken, maxResults, cfnDefaultPageSize), nil
}

// ListStackRefactorActions returns a stack refactor's actions, paginated by
// MaxResults/NextToken (real query-protocol form fields,
// api_op_ListStackRefactorActions.go).
func (b *InMemoryBackend) ListStackRefactorActions(
	stackRefactorID string, maxResults int, nextToken string,
) (page.Page[StackRefactorAction], error) {
	b.mu.RLock("ListStackRefactorActions")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return page.New([]StackRefactorAction{}, nextToken, maxResults, cfnDefaultPageSize), nil
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

	return page.New(actions, nextToken, maxResults, cfnDefaultPageSize), nil
}
