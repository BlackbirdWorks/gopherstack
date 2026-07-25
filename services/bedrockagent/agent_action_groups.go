package bedrockagent

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Agent action group CRUD
// ---------------------------------------------------------------------------

func agActionGroupKey(agentID, agentVersion, actionGroupID string) string {
	return agentID + "/" + agentVersion + "/" + actionGroupID
}

// CreateAgentActionGroup creates an action group for an agent version.
//
// Real AWS constrains the {agentVersion} URI path parameter to the literal
// "DRAFT" (fixed length 5, pattern `DRAFT`, per the CreateAgentActionGroup
// API reference) -- action groups can only ever be created against the
// mutable DRAFT version; numbered versions are immutable snapshots. A
// request path with any other value fails validation.
func (b *InMemoryBackend) CreateAgentActionGroup(
	_ context.Context, agentID, agentVersion string, cfg ActionGroupConfig,
) (*AgentActionGroup, error) {
	if cfg.ActionGroupName == "" {
		return nil, fmt.Errorf("%w: actionGroupName is required", ErrValidation)
	}

	if agentVersion != defaultAgentVersion {
		return nil, fmt.Errorf(
			"%w: agentVersion must be %q, got %q", ErrValidation, defaultAgentVersion, agentVersion,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	id := b.nextID("ag", &b.actionGroupCounter)
	now := time.Now().UTC()

	ag := &AgentActionGroup{
		ActionGroupID:       id,
		ActionGroupName:     cfg.ActionGroupName,
		AgentID:             agentID,
		AgentVersion:        agentVersion,
		ActionGroupState:    actionGroupEnabled,
		Description:         cfg.Description,
		ActionGroupExecutor: cfg.ActionGroupExecutor,
		APISchema:           cfg.APISchema,
		FunctionSchema:      cfg.FunctionSchema,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if cfg.ActionGroupState != "" {
		ag.ActionGroupState = cfg.ActionGroupState
	}

	b.actionGroups.Put(ag)

	return actionGroupCopy(ag), nil
}

// GetAgentActionGroup returns an action group.
func (b *InMemoryBackend) GetAgentActionGroup(
	_ context.Context, agentID, agentVersion, actionGroupID string,
) (*AgentActionGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ag, ok := b.actionGroups.Get(agActionGroupKey(agentID, agentVersion, actionGroupID))
	if !ok {
		return nil, fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	return actionGroupCopy(ag), nil
}

// UpdateAgentActionGroup updates an action group.
func (b *InMemoryBackend) UpdateAgentActionGroup(
	_ context.Context, agentID, agentVersion, actionGroupID string, cfg ActionGroupConfig,
) (*AgentActionGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agActionGroupKey(agentID, agentVersion, actionGroupID)

	ag, ok := b.actionGroups.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	applyActionGroupConfig(ag, cfg)
	ag.UpdatedAt = time.Now().UTC()

	return actionGroupCopy(ag), nil
}

func applyActionGroupConfig(ag *AgentActionGroup, cfg ActionGroupConfig) {
	if cfg.ActionGroupName != "" {
		ag.ActionGroupName = cfg.ActionGroupName
	}

	if cfg.Description != "" {
		ag.Description = cfg.Description
	}

	if cfg.ActionGroupState != "" {
		ag.ActionGroupState = cfg.ActionGroupState
	}

	if cfg.ActionGroupExecutor != nil {
		ag.ActionGroupExecutor = cfg.ActionGroupExecutor
	}

	if cfg.APISchema != nil {
		ag.APISchema = cfg.APISchema
	}

	if cfg.FunctionSchema != nil {
		ag.FunctionSchema = cfg.FunctionSchema
	}
}

// DeleteAgentActionGroup deletes an action group.
func (b *InMemoryBackend) DeleteAgentActionGroup(
	_ context.Context, agentID, agentVersion, actionGroupID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agActionGroupKey(agentID, agentVersion, actionGroupID)

	if !b.actionGroups.Has(key) {
		return fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	b.actionGroups.Delete(key)

	return nil
}

// ListAgentActionGroups returns all action groups for an agent version.
func (b *InMemoryBackend) ListAgentActionGroups(
	_ context.Context, agentID, agentVersion string, maxResults int, nextToken string,
) ([]*ActionGroupSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.actionGroupsByAgentVersion.Get(agentVersionScope(agentID, agentVersion))
	ids := tableIDs(group, func(ag *AgentActionGroup) string { return ag.ActionGroupID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*ActionGroupSummary, 0, len(ids))

	for _, id := range ids {
		ag, _ := b.actionGroups.Get(agActionGroupKey(agentID, agentVersion, id))
		out = append(out, &ActionGroupSummary{
			ActionGroupID:    ag.ActionGroupID,
			ActionGroupName:  ag.ActionGroupName,
			ActionGroupState: ag.ActionGroupState,
			Description:      ag.Description,
		})
	}

	return out, outToken, nil
}

func actionGroupCopy(ag *AgentActionGroup) *AgentActionGroup {
	cp := *ag

	return &cp
}
