package bedrockagent

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Agent–knowledge base association CRUD
// ---------------------------------------------------------------------------

func agKBKey(agentID, agentVersion, kbID string) string {
	return agentID + "/" + agentVersion + "/" + kbID
}

// AssociateAgentKnowledgeBase creates an agent–KB association.
//
// Real AWS constrains the {agentVersion} URI path parameter to the literal
// "DRAFT" (fixed length 5, pattern `DRAFT`, per the
// AssociateAgentKnowledgeBase API reference) -- same constraint as
// CreateAgentActionGroup.
func (b *InMemoryBackend) AssociateAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID, description, kbState string,
) (*AgentKnowledgeBase, error) {
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

	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	now := time.Now().UTC()
	state := "ENABLED"

	if kbState != "" {
		state = kbState
	}

	assoc := &AgentKnowledgeBase{
		AgentID:         agentID,
		AgentVersion:    agentVersion,
		KnowledgeBaseID: kbID,
		KBState:         state,
		Description:     description,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	b.agentKBAssocs.Put(assoc)

	return agKBCopy(assoc), nil
}

// GetAgentKnowledgeBase returns an agent–KB association.
func (b *InMemoryBackend) GetAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID string,
) (*AgentKnowledgeBase, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	assoc, ok := b.agentKBAssocs.Get(agKBKey(agentID, agentVersion, kbID))
	if !ok {
		return nil, fmt.Errorf("%w: association for kb %q not found", ErrNotFound, kbID)
	}

	return agKBCopy(assoc), nil
}

// UpdateAgentKnowledgeBase updates an agent–KB association.
//
// Real AWS constrains {agentVersion} to the literal "DRAFT" here too (API
// reference: Pattern `DRAFT`, fixed length 5) -- numbered versions are
// immutable snapshots, so this must reject them same as Associate.
func (b *InMemoryBackend) UpdateAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID, description, kbState string,
) (*AgentKnowledgeBase, error) {
	if agentVersion != defaultAgentVersion {
		return nil, fmt.Errorf(
			"%w: agentVersion must be %q, got %q", ErrValidation, defaultAgentVersion, agentVersion,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := agKBKey(agentID, agentVersion, kbID)

	assoc, ok := b.agentKBAssocs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: association for kb %q not found", ErrNotFound, kbID)
	}

	if description != "" {
		assoc.Description = description
	}

	if kbState != "" {
		assoc.KBState = kbState
	}

	assoc.UpdatedAt = time.Now().UTC()

	return agKBCopy(assoc), nil
}

// DisassociateAgentKnowledgeBase removes an agent–KB association.
//
// Real AWS constrains {agentVersion} to the literal "DRAFT" here too (API
// reference: Pattern `DRAFT`, fixed length 5) -- numbered versions are
// immutable snapshots, so this must reject them same as Associate.
func (b *InMemoryBackend) DisassociateAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID string,
) error {
	if agentVersion != defaultAgentVersion {
		return fmt.Errorf(
			"%w: agentVersion must be %q, got %q", ErrValidation, defaultAgentVersion, agentVersion,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := agKBKey(agentID, agentVersion, kbID)

	if !b.agentKBAssocs.Has(key) {
		return fmt.Errorf("%w: association for kb %q not found", ErrNotFound, kbID)
	}

	b.agentKBAssocs.Delete(key)

	return nil
}

// ListAgentKnowledgeBases returns paginated agent–KB associations.
func (b *InMemoryBackend) ListAgentKnowledgeBases(
	_ context.Context, agentID, agentVersion string, maxResults int, nextToken string,
) ([]*AgentKnowledgeBase, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.agentKBAssocsByAgentVersion.Get(agentVersionScope(agentID, agentVersion))
	ids := tableIDs(group, func(a *AgentKnowledgeBase) string { return a.KnowledgeBaseID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentKnowledgeBase, 0, len(ids))

	for _, id := range ids {
		assoc, _ := b.agentKBAssocs.Get(agKBKey(agentID, agentVersion, id))
		out = append(out, agKBCopy(assoc))
	}

	return out, outToken, nil
}

func agKBCopy(a *AgentKnowledgeBase) *AgentKnowledgeBase {
	cp := *a

	return &cp
}
