package bedrock

import (
	"fmt"
	"sort"
)

func agentKBKey(agentID, kbID string) string { return agentID + "/" + kbID }

// AssociateAgentKnowledgeBase links a knowledge base to an agent.
func (b *InMemoryBackend) AssociateAgentKnowledgeBase(
	agentID, kbID, description string,
) (*AgentKnowledgeBaseAssociation, error) {
	b.mu.Lock("AssociateAgentKnowledgeBase")
	defer b.mu.Unlock()

	if _, ok := b.agents.Get(agentID); !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if _, ok := b.knowledgeBases.Get(kbID); !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	assoc := &AgentKnowledgeBaseAssociation{
		AgentID:         agentID,
		AgentVersion:    agentStatusDraft,
		KnowledgeBaseID: kbID,
		Description:     description,
		KBState:         actionGroupEnabled,
	}
	b.agentKBAssociations.Put(assoc)
	cp := *assoc

	return &cp, nil
}

// DisassociateAgentKnowledgeBase removes a knowledge base association from an agent.
func (b *InMemoryBackend) DisassociateAgentKnowledgeBase(agentID, kbID string) error {
	b.mu.Lock("DisassociateAgentKnowledgeBase")
	defer b.mu.Unlock()

	key := agentKBKey(agentID, kbID)

	if _, ok := b.agentKBAssociations.Get(key); !ok {
		return fmt.Errorf(
			"%w: association not found for agent %q and kb %q",
			ErrNotFound,
			agentID,
			kbID,
		)
	}

	b.agentKBAssociations.Delete(key)

	return nil
}

// UpdateAgentKnowledgeBase updates an existing association.
func (b *InMemoryBackend) UpdateAgentKnowledgeBase(
	agentID, kbID, description, state string,
) (*AgentKnowledgeBaseAssociation, error) {
	b.mu.Lock("UpdateAgentKnowledgeBase")
	defer b.mu.Unlock()

	assoc, ok := b.agentKBAssociations.Get(agentKBKey(agentID, kbID))
	if !ok {
		return nil, fmt.Errorf("%w: association not found for agent %q and kb %q", ErrNotFound, agentID, kbID)
	}

	if description != "" {
		assoc.Description = description
	}
	if state != "" {
		assoc.KBState = state
	}

	cp := *assoc

	return &cp, nil
}

// GetAgentKnowledgeBase returns an agent knowledge base association.
func (b *InMemoryBackend) GetAgentKnowledgeBase(
	agentID, kbID string,
) (*AgentKnowledgeBaseAssociation, error) {
	b.mu.RLock("GetAgentKnowledgeBase")
	defer b.mu.RUnlock()

	assoc, ok := b.agentKBAssociations.Get(agentKBKey(agentID, kbID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: association not found for agent %q and kb %q",
			ErrNotFound,
			agentID,
			kbID,
		)
	}

	cp := *assoc

	return &cp, nil
}

// ListAgentKnowledgeBases returns all knowledge base associations for an agent.
func (b *InMemoryBackend) ListAgentKnowledgeBases(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentKnowledgeBaseAssociation, string) {
	b.mu.RLock("ListAgentKnowledgeBases")
	defer b.mu.RUnlock()

	list := make([]*AgentKnowledgeBaseAssociation, 0, b.agentKBAssociations.Len())

	for _, assoc := range b.agentKBAssociations.All() {
		if assoc.AgentID == agentID {
			cp := *assoc
			list = append(list, &cp)
		}
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].KnowledgeBaseID < list[j].KnowledgeBaseID },
	)

	return paginate(list, maxResults, nextToken)
}
