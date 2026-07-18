package bedrock

import (
	"fmt"
	"sort"
	"time"
)

// AssociateAgentCollaborator associates a collaborator agent with an agent.
func (b *InMemoryBackend) AssociateAgentCollaborator(
	agentID, agentVersion, collaboratorArn, relayConversation string,
) (*AgentCollaborator, error) {
	b.mu.Lock("AssociateAgentCollaborator")
	defer b.mu.Unlock()

	if _, ok := b.agents.Get(agentID); !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.agentCollabCounter++
	collabID := fmt.Sprintf("collab-%08d", b.agentCollabCounter)

	ac := &AgentCollaborator{
		CreatedAt:         time.Now(),
		CollaboratorID:    collabID,
		AgentID:           agentID,
		AgentVersion:      agentVersion,
		CollaboratorArn:   collaboratorArn,
		RelayConversation: relayConversation,
	}

	b.agentCollaboratorsStore(agentID).Put(ac)
	cp := *ac

	return &cp, nil
}

// GetAgentCollaborator returns an agent collaborator by ID.
func (b *InMemoryBackend) GetAgentCollaborator(
	agentID, collaboratorID string,
) (*AgentCollaborator, error) {
	b.mu.RLock("GetAgentCollaborator")
	defer b.mu.RUnlock()

	collabs := b.agentCollaborators[agentID]

	var (
		ac *AgentCollaborator
		ok bool
	)

	if collabs != nil {
		ac, ok = collabs.Get(collaboratorID)
	}

	if !ok {
		return nil, fmt.Errorf(
			"%w: collaborator %q not found for agent %q",
			ErrNotFound,
			collaboratorID,
			agentID,
		)
	}

	cp := *ac

	return &cp, nil
}

// ListAgentCollaborators lists collaborators for an agent.
func (b *InMemoryBackend) ListAgentCollaborators(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentCollaborator, string) {
	b.mu.RLock("ListAgentCollaborators")
	defer b.mu.RUnlock()

	collabs := b.agentCollaborators[agentID]
	list := make([]*AgentCollaborator, 0)

	if collabs != nil {
		for _, ac := range collabs.All() {
			cp := *ac
			list = append(list, &cp)
		}
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].CollaboratorID < list[j].CollaboratorID },
	)

	return paginate(list, maxResults, nextToken)
}

// UpdateAgentCollaborator updates an agent collaborator.
func (b *InMemoryBackend) UpdateAgentCollaborator(
	agentID, collaboratorID, relayConversation string,
) (*AgentCollaborator, error) {
	b.mu.Lock("UpdateAgentCollaborator")
	defer b.mu.Unlock()

	collabs := b.agentCollaborators[agentID]

	var (
		ac *AgentCollaborator
		ok bool
	)

	if collabs != nil {
		ac, ok = collabs.Get(collaboratorID)
	}

	if !ok {
		return nil, fmt.Errorf(
			"%w: collaborator %q not found for agent %q",
			ErrNotFound,
			collaboratorID,
			agentID,
		)
	}

	if relayConversation != "" {
		ac.RelayConversation = relayConversation
	}

	cp := *ac

	return &cp, nil
}

// DisassociateAgentCollaborator removes a collaborator from an agent.
func (b *InMemoryBackend) DisassociateAgentCollaborator(agentID, collaboratorID string) error {
	b.mu.Lock("DisassociateAgentCollaborator")
	defer b.mu.Unlock()

	collabs := b.agentCollaborators[agentID]

	if collabs == nil || !collabs.Has(collaboratorID) {
		return fmt.Errorf(
			"%w: collaborator %q not found for agent %q",
			ErrNotFound,
			collaboratorID,
			agentID,
		)
	}

	collabs.Delete(collaboratorID)

	return nil
}
