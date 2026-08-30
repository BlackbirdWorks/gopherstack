package bedrock

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

func agentActionGroupKey(
	agentID, actionGroupID string,
) string {
	return agentID + "/" + actionGroupID
}

// CreateAgentActionGroup creates an action group for an agent.
func (b *InMemoryBackend) CreateAgentActionGroup(
	agentID, actionGroupName, description string,
	executor map[string]any,
) (*AgentActionGroup, error) {
	return b.CreateAgentActionGroupWithSchemas(agentID, actionGroupName, description, executor, nil, nil)
}

// CreateAgentActionGroupWithSchemas creates an action group preserving either supported schema.
func (b *InMemoryBackend) CreateAgentActionGroupWithSchemas(
	agentID, actionGroupName, description string,
	executor, apiSchema, functionSchema map[string]any,
) (*AgentActionGroup, error) {
	b.mu.Lock("CreateAgentActionGroup")
	defer b.mu.Unlock()

	if _, ok := b.agents.Get(agentID); !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.actionGroupCounter++
	id := fmt.Sprintf("ag-%08d", b.actionGroupCounter)
	now := time.Now()

	ag := &AgentActionGroup{
		CreatedAt:           now,
		UpdatedAt:           now,
		ActionGroupID:       id,
		ActionGroupName:     actionGroupName,
		AgentID:             agentID,
		AgentVersion:        agentStatusDraft,
		ActionGroupState:    actionGroupEnabled,
		Description:         description,
		ActionGroupExecutor: maps.Clone(executor),
		APISchema:           maps.Clone(apiSchema),
		FunctionSchema:      maps.Clone(functionSchema),
	}
	b.agentActionGroups.Put(ag)
	cp := *ag

	return &cp, nil
}

// GetAgentActionGroup returns an action group by ID.
func (b *InMemoryBackend) GetAgentActionGroup(
	agentID, actionGroupID string,
) (*AgentActionGroup, error) {
	b.mu.RLock("GetAgentActionGroup")
	defer b.mu.RUnlock()

	ag, ok := b.agentActionGroups.Get(agentActionGroupKey(agentID, actionGroupID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: action group %q not found for agent %q",
			ErrNotFound,
			actionGroupID,
			agentID,
		)
	}

	cp := *ag

	return &cp, nil
}

// ListAgentActionGroups lists action groups for an agent.
func (b *InMemoryBackend) ListAgentActionGroups(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentActionGroup, string) {
	b.mu.RLock("ListAgentActionGroups")
	defer b.mu.RUnlock()

	list := make([]*AgentActionGroup, 0, b.agentActionGroups.Len())

	for _, ag := range b.agentActionGroups.All() {
		if ag.AgentID == agentID {
			cp := *ag
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].ActionGroupName != list[j].ActionGroupName {
			return list[i].ActionGroupName < list[j].ActionGroupName
		}

		return list[i].ActionGroupID < list[j].ActionGroupID
	})

	return paginate(list, maxResults, nextToken)
}

// UpdateAgentActionGroup updates an action group.
func (b *InMemoryBackend) UpdateAgentActionGroup(
	agentID, actionGroupID, actionGroupName, description string,
	executor map[string]any,
) (*AgentActionGroup, error) {
	return b.UpdateAgentActionGroupWithSchemas(
		agentID, actionGroupID, actionGroupName, description, executor, nil, nil,
	)
}

// UpdateAgentActionGroupWithSchemas updates an action group and any submitted schemas.
// actionGroupName is required by the real UpdateAgentActionGroup API, but applied only
// when non-empty here to tolerate callers (and existing tests) built before this parameter
// existed.
func (b *InMemoryBackend) UpdateAgentActionGroupWithSchemas(
	agentID, actionGroupID, actionGroupName, description string,
	executor, apiSchema, functionSchema map[string]any,
) (*AgentActionGroup, error) {
	b.mu.Lock("UpdateAgentActionGroup")
	defer b.mu.Unlock()

	key := agentActionGroupKey(agentID, actionGroupID)

	ag, ok := b.agentActionGroups.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	if actionGroupName != "" {
		ag.ActionGroupName = actionGroupName
	}

	if description != "" {
		ag.Description = description
	}

	if executor != nil {
		ag.ActionGroupExecutor = maps.Clone(executor)
	}
	if apiSchema != nil {
		ag.APISchema = maps.Clone(apiSchema)
	}
	if functionSchema != nil {
		ag.FunctionSchema = maps.Clone(functionSchema)
	}

	ag.UpdatedAt = time.Now()
	cp := *ag

	return &cp, nil
}

// DeleteAgentActionGroup deletes an action group.
func (b *InMemoryBackend) DeleteAgentActionGroup(agentID, actionGroupID string) error {
	b.mu.Lock("DeleteAgentActionGroup")
	defer b.mu.Unlock()

	key := agentActionGroupKey(agentID, actionGroupID)

	if _, ok := b.agentActionGroups.Get(key); !ok {
		return fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	b.agentActionGroups.Delete(key)

	return nil
}
