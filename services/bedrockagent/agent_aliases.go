package bedrockagent

import (
	"context"
	"fmt"
	"maps"
	"time"
)

// ---------------------------------------------------------------------------
// Agent alias CRUD
// ---------------------------------------------------------------------------

func aliasKey(agentID, aliasID string) string { return agentID + "/" + aliasID }

// CreateAgentAlias creates an alias for an agent.
func (b *InMemoryBackend) CreateAgentAlias(
	ctx context.Context, agentID string, cfg AliasConfig,
) (*AgentAlias, error) {
	if cfg.AliasName == "" {
		return nil, fmt.Errorf("%w: agentAliasName is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	routing := cfg.RoutingConfiguration

	// Real AWS: when CreateAgentAlias is called with no routingConfiguration,
	// Bedrock automatically creates a new numbered agent version (a snapshot
	// of DRAFT) and points the alias at it -- there is no public
	// CreateAgentVersion API; this is the only wire-visible way a numbered
	// version comes into existence. See newAgentVersionLocked's doc comment
	// on CreateAgentVersion.
	if len(routing) == 0 {
		av, err := b.newAgentVersionLocked(agentID, "")
		if err != nil {
			return nil, err
		}

		routing = []AliasRouting{{AgentVersion: av.AgentVersion}}
	}

	id := b.nextID("alias", &b.aliasCounter)
	now := time.Now().UTC()

	al := &AgentAlias{
		AgentAliasID:         id,
		AgentAliasARN:        b.buildAliasARN(region, agentID, id),
		AgentAliasName:       cfg.AliasName,
		AgentAliasStatus:     aliasStatusPrepared,
		AgentID:              agentID,
		Description:          cfg.Description,
		RoutingConfiguration: routing,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	b.agentAliases.Put(al)
	// Real AWS: CreateAgentAliasInput accepts a "tags" member, but the
	// AgentAlias response shape never echoes tags back -- they are only
	// readable via ListTagsForResource(AgentAliasArn). Was previously
	// dropped entirely (stored only on the now-removed invented
	// AgentAlias.Tags wire field, so ListTagsForResource on a
	// freshly-created alias incorrectly returned empty).
	b.tags[al.AgentAliasARN] = maps.Clone(cfg.Tags)

	return aliasCopy(al), nil
}

// GetAgentAlias returns an agent alias.
func (b *InMemoryBackend) GetAgentAlias(_ context.Context, agentID, aliasID string) (*AgentAlias, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	al, ok := b.agentAliases.Get(aliasKey(agentID, aliasID))
	if !ok {
		return nil, fmt.Errorf("%w: alias %q not found", ErrNotFound, aliasID)
	}

	return aliasCopy(al), nil
}

// UpdateAgentAlias updates an agent alias.
func (b *InMemoryBackend) UpdateAgentAlias(
	_ context.Context, agentID, aliasID string, cfg AliasConfig,
) (*AgentAlias, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	al, ok := b.agentAliases.Get(aliasKey(agentID, aliasID))
	if !ok {
		return nil, fmt.Errorf("%w: alias %q not found", ErrNotFound, aliasID)
	}

	if cfg.AliasName != "" {
		al.AgentAliasName = cfg.AliasName
	}

	if cfg.Description != "" {
		al.Description = cfg.Description
	}

	if cfg.RoutingConfiguration != nil {
		al.RoutingConfiguration = cfg.RoutingConfiguration
	}

	al.UpdatedAt = time.Now().UTC()

	return aliasCopy(al), nil
}

// DeleteAgentAlias deletes an agent alias and its tags map entry.
func (b *InMemoryBackend) DeleteAgentAlias(_ context.Context, agentID, aliasID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := aliasKey(agentID, aliasID)

	al, ok := b.agentAliases.Get(key)
	if !ok {
		return fmt.Errorf("%w: alias %q not found", ErrNotFound, aliasID)
	}

	b.agentAliases.Delete(key)
	delete(b.tags, al.AgentAliasARN)

	return nil
}

// ListAgentAliases returns paginated alias summaries for an agent.
func (b *InMemoryBackend) ListAgentAliases(
	_ context.Context, agentID string, maxResults int, nextToken string,
) ([]*AgentAliasSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.agentAliasesByAgent.Get(agentID)
	ids := tableIDs(group, func(al *AgentAlias) string { return al.AgentAliasID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentAliasSummary, 0, len(ids))

	for _, id := range ids {
		al, _ := b.agentAliases.Get(aliasKey(agentID, id))
		out = append(out, &AgentAliasSummary{
			AgentAliasID:     al.AgentAliasID,
			AgentAliasName:   al.AgentAliasName,
			AgentAliasStatus: al.AgentAliasStatus,
			Description:      al.Description,
		})
	}

	return out, outToken, nil
}

func aliasCopy(al *AgentAlias) *AgentAlias {
	cp := *al

	if al.RoutingConfiguration != nil {
		cp.RoutingConfiguration = append([]AliasRouting{}, al.RoutingConfiguration...)
	}

	return &cp
}
