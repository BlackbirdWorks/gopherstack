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

	b.mu.Lock()
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

	a := &Agent{
		AgentID:         id,
		AgentARN:        b.buildAgentARN(region, id),
		AgentName:       cfg.AgentName,
		AgentVersion:    defaultAgentVersion,
		AgentStatus:     agentStatusNotPrepared,
		Collaboration:   collab,
		Description:     cfg.Description,
		FoundationModel: cfg.FoundationModel,
		Instruction:     cfg.Instruction,
		RoleARN:         cfg.RoleARN,
		Tags:            maps.Clone(cfg.Tags),
		Guardrail:       cfg.Guardrail,
		Memory:          cfg.Memory,
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	return agentCopy(a), nil
}

// UpdateAgent updates an existing agent.
func (b *InMemoryBackend) UpdateAgent(_ context.Context, agentID string, cfg AgentConfig) (*Agent, error) {
	b.mu.Lock()
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
// versions (agentVersions), and its version counter -- it does NOT clean up
// actionGroups, agentAliases, or agentCollaborators for the deleted agent
// (the pre-Phase-3.3 map version keyed agentCollaborators by "agentID/"+
// agentVersion and called delete(b.agentCollaborators, agentID) here, which
// never matched any real key -- a no-op cleanup bug preserved as-is by this
// conversion; see the DeleteAgent doc in .claude/memories for the
// byte-for-byte preservation rule).
func (b *InMemoryBackend) DeleteAgent(_ context.Context, agentID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	delete(b.agentsByName, a.AgentName)
	b.agents.Delete(agentID)

	for _, av := range slices.Clone(b.agentVersionsByAgent.Get(agentID)) {
		b.agentVersions.Delete(agentVersionKey(av.AgentID, av.AgentVersion))
	}

	delete(b.agentVersionCtrs, agentID)

	return nil
}

// ListAgents returns a paginated list of agent summaries.
func (b *InMemoryBackend) ListAgents(
	_ context.Context, maxResults int, nextToken string,
) ([]*AgentSummary, string, error) {
	b.mu.RLock()
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
	b.mu.Lock()
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
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}
