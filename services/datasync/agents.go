package datasync

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateAgent creates a new DataSync agent.
func (b *InMemoryBackend) CreateAgent(name, _ string, tags map[string]string) (*Agent, error) {
	b.mu.Lock("CreateAgent")
	defer b.mu.Unlock()

	id := newID()
	agentArn := b.agentARN(id)
	now := time.Now().UTC()

	agentTags := make(map[string]string)
	maps.Copy(agentTags, tags)

	a := &storedAgent{
		AgentArn:     agentArn,
		Name:         name,
		Status:       agentStatusOnline,
		EndpointType: "PUBLIC",
		CreationTime: now,
		Tags:         agentTags,
	}
	b.agents.Put(a)

	if len(agentTags) > 0 {
		b.tags[agentArn] = make(map[string]string)
		maps.Copy(b.tags[agentArn], agentTags)
	}

	cp := a.toAgent()

	return &cp, nil
}

// DescribeAgent returns agent details.
func (b *InMemoryBackend) DescribeAgent(agentArn string) (*Agent, error) {
	b.mu.RLock("DescribeAgent")
	defer b.mu.RUnlock()

	a, ok := b.agents.Get(agentArn)
	if !ok {
		return nil, ErrNotFound
	}

	cp := a.toAgent()

	return &cp, nil
}

// UpdateAgent updates the agent's name.
func (b *InMemoryBackend) UpdateAgent(agentArn, name string) error {
	b.mu.Lock("UpdateAgent")
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentArn)
	if !ok {
		return ErrNotFound
	}

	a.Name = name

	return nil
}

// DeleteAgent deletes an agent.
func (b *InMemoryBackend) DeleteAgent(agentArn string) error {
	b.mu.Lock("DeleteAgent")
	defer b.mu.Unlock()

	if !b.agents.Has(agentArn) {
		return ErrNotFound
	}

	b.agents.Delete(agentArn)
	delete(b.tags, agentArn)

	return nil
}

// ListAgents returns agents, sorted by ARN.
func (b *InMemoryBackend) ListAgents(maxResults int32, nextToken string) ([]*AgentListEntry, string, error) {
	b.mu.RLock("ListAgents")
	defer b.mu.RUnlock()

	sorted := b.agents.Snapshot()

	all := make([]*AgentListEntry, 0, len(sorted))
	for _, ag := range sorted {
		all = append(all, &AgentListEntry{
			AgentArn: ag.AgentArn,
			Name:     ag.Name,
			Status:   ag.Status,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}
