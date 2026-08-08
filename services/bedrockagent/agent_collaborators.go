package bedrockagent

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Agent collaborator CRUD
// ---------------------------------------------------------------------------

// AssociateAgentCollaborator creates a collaborator association.
//
// Real AWS constrains the {agentVersion} URI path parameter to the literal
// "DRAFT" (fixed length 5, pattern `DRAFT`, per the
// AssociateAgentCollaborator API reference) -- same constraint as
// CreateAgentActionGroup.
func (b *InMemoryBackend) AssociateAgentCollaborator(
	_ context.Context, agentID, agentVersion string, cfg CollaboratorConfig,
) (*AgentCollaborator, error) {
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

	id := b.nextID("collab", &b.collabCounter)

	now := time.Now().UTC()
	c := &AgentCollaborator{
		AgentID:                  agentID,
		AgentVersion:             agentVersion,
		CollaboratorID:           id,
		CollaboratorName:         cfg.CollaboratorName,
		CollaborationInstruction: cfg.CollaborationInstruction,
		RelayConversationHistory: cfg.RelayConversationHistory,
		AgentDescriptor:          cfg.AgentDescriptor,
		CollaboratorStatus:       collabEnabled,
		CreatedAt:                now,
		UpdatedAt:                now,
	}

	b.agentCollaborators.Put(c)

	return collabCopy(c), nil
}

// GetAgentCollaborator returns a collaborator by ID.
func (b *InMemoryBackend) GetAgentCollaborator(
	_ context.Context, agentID, agentVersion, collaboratorID string,
) (*AgentCollaborator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	c, ok := b.agentCollaborators.Get(agentCollabKey(agentID, agentVersion, collaboratorID))
	if !ok {
		return nil, fmt.Errorf("%w: collaborator %q not found", ErrNotFound, collaboratorID)
	}

	return collabCopy(c), nil
}

// UpdateAgentCollaborator updates a collaborator.
//
// Real AWS constrains {agentVersion} to the literal "DRAFT" here too (API
// reference: Pattern `DRAFT`, fixed length 5) -- numbered versions are
// immutable snapshots, so this must reject them same as Associate.
func (b *InMemoryBackend) UpdateAgentCollaborator(
	_ context.Context, agentID, agentVersion, collaboratorID string, cfg CollaboratorConfig,
) (*AgentCollaborator, error) {
	if agentVersion != defaultAgentVersion {
		return nil, fmt.Errorf(
			"%w: agentVersion must be %q, got %q", ErrValidation, defaultAgentVersion, agentVersion,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.agentCollaborators.Get(agentCollabKey(agentID, agentVersion, collaboratorID))
	if !ok {
		return nil, fmt.Errorf("%w: collaborator %q not found", ErrNotFound, collaboratorID)
	}

	if cfg.CollaboratorName != "" {
		c.CollaboratorName = cfg.CollaboratorName
	}

	if cfg.CollaborationInstruction != "" {
		c.CollaborationInstruction = cfg.CollaborationInstruction
	}

	if cfg.RelayConversationHistory != "" {
		c.RelayConversationHistory = cfg.RelayConversationHistory
	}

	if cfg.AgentDescriptor != nil {
		c.AgentDescriptor = cfg.AgentDescriptor
	}

	c.UpdatedAt = time.Now().UTC()

	return collabCopy(c), nil
}

// DisassociateAgentCollaborator removes a collaborator.
//
// Real AWS constrains {agentVersion} to the literal "DRAFT" here too (API
// reference: Pattern `DRAFT`, fixed length 5) -- numbered versions are
// immutable snapshots, so this must reject them same as Associate.
func (b *InMemoryBackend) DisassociateAgentCollaborator(
	_ context.Context, agentID, agentVersion, collaboratorID string,
) error {
	if agentVersion != defaultAgentVersion {
		return fmt.Errorf(
			"%w: agentVersion must be %q, got %q", ErrValidation, defaultAgentVersion, agentVersion,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := agentCollabKey(agentID, agentVersion, collaboratorID)
	if !b.agentCollaborators.Has(key) {
		return fmt.Errorf("%w: collaborator %q not found", ErrNotFound, collaboratorID)
	}

	b.agentCollaborators.Delete(key)

	return nil
}

// ListAgentCollaborators returns paginated collaborators.
func (b *InMemoryBackend) ListAgentCollaborators(
	_ context.Context, agentID, agentVersion string, maxResults int, nextToken string,
) ([]*AgentCollaborator, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.agentCollaboratorsByAgentVersion.Get(agentVersionScope(agentID, agentVersion))
	ids := tableIDs(group, func(c *AgentCollaborator) string { return c.CollaboratorID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentCollaborator, 0, len(ids))

	for _, id := range ids {
		c, _ := b.agentCollaborators.Get(agentCollabKey(agentID, agentVersion, id))
		out = append(out, collabCopy(c))
	}

	return out, outToken, nil
}

func collabCopy(c *AgentCollaborator) *AgentCollaborator {
	cp := *c

	return &cp
}
