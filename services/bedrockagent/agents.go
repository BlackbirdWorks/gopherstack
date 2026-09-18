package bedrockagent

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

// ---------------------------------------------------------------------------
// Agent CRUD
// ---------------------------------------------------------------------------

// CreateAgent creates a new agent.
func (b *InMemoryBackend) CreateAgent(ctx context.Context, cfg AgentConfig) (*Agent, error) {
	if cfg.AgentName == "" {
		return nil, fmt.Errorf("%w: agentName is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateAgent")
	defer b.mu.Unlock()

	if _, exists := b.agentsByName[cfg.AgentName]; exists {
		return nil, fmt.Errorf("%w: agent %q already exists", ErrAlreadyExists, cfg.AgentName)
	}

	id := b.nextID("agent", &b.agentCounter)
	now := time.Now().UTC()

	collab := cfg.Collaboration
	if collab == "" {
		collab = "DISABLED"
	}

	orchestrationType := cfg.OrchestrationType
	if orchestrationType == "" {
		orchestrationType = "DEFAULT"
	}

	a := &Agent{
		AgentID:           id,
		AgentARN:          b.buildAgentARN(region, id),
		AgentName:         cfg.AgentName,
		AgentVersion:      defaultAgentVersion,
		AgentStatus:       agentStatusNotPrepared,
		Collaboration:     collab,
		Description:       cfg.Description,
		FoundationModel:   cfg.FoundationModel,
		Instruction:       cfg.Instruction,
		RoleARN:           cfg.RoleARN,
		OrchestrationType: orchestrationType,
		Guardrail:         cfg.Guardrail,
		Memory:            cfg.Memory,
		PromptOverrideConfiguration: map[string]any{
			"promptConfigurations": []any{},
		},
		IdleSessionTTLInSeconds: ttlOrDefault(cfg.IdleSessionTTLInSeconds),
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	b.agents.Put(a)
	b.agentsByName[cfg.AgentName] = id
	b.tags[a.AgentARN] = maps.Clone(cfg.Tags)

	return agentCopy(a), nil
}

// GetAgent returns an agent by ID.
func (b *InMemoryBackend) GetAgent(_ context.Context, agentID string) (*Agent, error) {
	b.mu.RLock("GetAgent")
	defer b.mu.RUnlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	return agentCopy(a), nil
}

// UpdateAgent updates an existing agent.
func (b *InMemoryBackend) UpdateAgent(_ context.Context, agentID string, cfg AgentConfig) (*Agent, error) {
	b.mu.Lock("UpdateAgent")
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if cfg.AgentName != "" && cfg.AgentName != a.AgentName {
		if _, exists := b.agentsByName[cfg.AgentName]; exists {
			return nil, fmt.Errorf("%w: agent name %q already in use", ErrAlreadyExists, cfg.AgentName)
		}

		delete(b.agentsByName, a.AgentName)
		b.agentsByName[cfg.AgentName] = agentID
		a.AgentName = cfg.AgentName
	}

	applyAgentConfig(a, cfg)
	a.UpdatedAt = time.Now().UTC()

	return agentCopy(a), nil
}

func ttlOrDefault(ttl int) int {
	if ttl > 0 {
		return ttl
	}

	return defaultIdleSessionTTLSeconds
}

func applyAgentConfig(a *Agent, cfg AgentConfig) {
	if cfg.Collaboration != "" {
		a.Collaboration = cfg.Collaboration
	}

	if cfg.Description != "" {
		a.Description = cfg.Description
	}

	if cfg.FoundationModel != "" {
		a.FoundationModel = cfg.FoundationModel
	}

	if cfg.Instruction != "" {
		a.Instruction = cfg.Instruction
	}

	if cfg.RoleARN != "" {
		a.RoleARN = cfg.RoleARN
	}

	if cfg.OrchestrationType != "" {
		a.OrchestrationType = cfg.OrchestrationType
	}

	if cfg.Guardrail != nil {
		a.Guardrail = cfg.Guardrail
	}

	if cfg.Memory != nil {
		a.Memory = cfg.Memory
	}

	if cfg.IdleSessionTTLInSeconds > 0 {
		a.IdleSessionTTLInSeconds = cfg.IdleSessionTTLInSeconds
	}
}

// DeleteAgent deletes an agent.
//
// Note: this only removes the agent itself, its name-lookup entry, its
// versions (agentVersions), its version counter, every action group / agent
// collaborator / KB association scoped under any of those versions
// (including DRAFT), every alias, and the agent's own tags entry.
//
// This used to leave every one of those behind as ghost rows (documented
// historically as a "preserved as-is" no-op cleanup bug from the
// pre-Phase-3.3 map-based backend) -- fixed here: actionGroups,
// agentCollaborators, and agentKBAssocs are indexed by the composite
// "agentID/agentVersion" scope, so cascading them requires walking DRAFT
// plus every numbered AgentVersion row and clearing each scope; agentAliases
// carries a plain byAgent index so no version walk is needed there.
//
// Real AWS (api_op_DeleteAgent.go): "By default, this value is false and
// deletion is stopped if the resource is in use. If you set it to true, the
// resource will be deleted even if the resource is in use." An agent is "in
// use" when it has any alias at all -- an alias always routes to a numbered
// snapshot of this agent (CreateAgentAlias), so any alias existing means a
// caller-visible reference into this agent would otherwise be cascade-deleted
// out from under them. Same wire-visible-relationship reasoning as
// DeleteAgentVersion's per-version alias check.
func (b *InMemoryBackend) DeleteAgent(_ context.Context, agentID string, skipResourceInUseCheck bool) error {
	b.mu.Lock("DeleteAgent")
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if !skipResourceInUseCheck {
		if aliases := b.agentAliasesByAgent.Get(agentID); len(aliases) > 0 {
			return fmt.Errorf(
				"%w: agent %q has %d alias(es)", ErrResourceInUse, agentID, len(aliases),
			)
		}
	}

	delete(b.agentsByName, a.AgentName)
	b.agents.Delete(agentID)
	delete(b.tags, a.AgentARN)

	versions := []string{defaultAgentVersion}

	for _, av := range slices.Clone(b.agentVersionsByAgent.Get(agentID)) {
		b.agentVersions.Delete(agentVersionKey(av.AgentID, av.AgentVersion))
		versions = append(versions, av.AgentVersion)
	}

	delete(b.agentVersionCtrs, agentID)

	for _, v := range versions {
		b.deleteSubResourcesLocked(agentID, v)
	}

	for _, al := range slices.Clone(b.agentAliasesByAgent.Get(agentID)) {
		b.agentAliases.Delete(aliasKey(al.AgentID, al.AgentAliasID))
		delete(b.tags, al.AgentAliasARN)
	}

	return nil
}

// ListAgents returns a paginated list of agent summaries.
func (b *InMemoryBackend) ListAgents(
	_ context.Context, maxResults int, nextToken string,
) ([]*AgentSummary, string, error) {
	b.mu.RLock("ListAgents")
	defer b.mu.RUnlock()

	ids := tableIDs(b.agents.Snapshot(), func(a *Agent) string { return a.AgentID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentSummary, 0, len(ids))

	for _, id := range ids {
		a, _ := b.agents.Get(id)
		out = append(out, &AgentSummary{
			AgentID:     a.AgentID,
			AgentName:   a.AgentName,
			AgentStatus: a.AgentStatus,
			Description: a.Description,
			UpdatedAt:   a.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// PrepareAgent transitions agent to PREPARED status.
func (b *InMemoryBackend) PrepareAgent(_ context.Context, agentID string) (*Agent, error) {
	b.mu.Lock("PrepareAgent")
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	now := time.Now().UTC()
	a.AgentStatus = agentStatusPrepared
	a.UpdatedAt = now
	a.PreparedAt = &now

	return agentCopy(a), nil
}

func agentCopy(a *Agent) *Agent {
	cp := *a

	return &cp
}
