package bedrockagent

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Agent version CRUD
// ---------------------------------------------------------------------------

// CreateAgentVersion creates a numbered snapshot of an agent.
//
// Note: real Bedrock Agents has no CreateAgentVersion wire operation --
// numbered agent versions are created as a side effect of CreateAgentAlias
// when called with no routingConfiguration (see the doAgentAlias helper in
// CreateAgentAlias below, which calls newAgentVersionLocked directly while
// already holding b.mu). This method stays exported on InMemoryBackend/
// StorageBackend for internal/programmatic use (e.g. tests seeding a
// version directly) but is deliberately not reachable from any HTTP route.
func (b *InMemoryBackend) CreateAgentVersion(
	_ context.Context, agentID, description string,
) (*AgentVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	av, err := b.newAgentVersionLocked(agentID, description)
	if err != nil {
		return nil, err
	}

	return agentVersionCopy(av), nil
}

// newAgentVersionLocked creates a numbered snapshot of an agent. Callers
// must hold b.mu.Lock.
func (b *InMemoryBackend) newAgentVersionLocked(agentID, description string) (*AgentVersion, error) {
	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.agentVersionCtrs[agentID]++
	versionNum := b.agentVersionCtrs[agentID]
	version := strconv.Itoa(versionNum)

	now := time.Now().UTC()
	av := &AgentVersion{
		AgentID:         agentID,
		AgentARN:        a.AgentARN,
		AgentName:       a.AgentName,
		AgentVersion:    version,
		AgentStatus:     agentStatusPrepared,
		FoundationModel: a.FoundationModel,
		Instruction:     a.Instruction,
		RoleARN:         a.RoleARN,
		Description:     description,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	b.agentVersions.Put(av)

	return av, nil
}

// GetAgentVersion returns a specific agent version.
//
// Not-found precedence note: the pre-Phase-3.3 map checked for the presence
// of the per-agent inner map (created lazily by the first
// CreateAgentVersion call and never removed except by DeleteAgent) to decide
// between "agent not found" and "agent version not found". That inner-map
// presence check is not reproducible with a flat store.Table + secondary
// Index (an Index prunes a group the moment its last member is deleted, see
// pkgs/store's Index.remove), so this checks b.agents.Has(agentID) instead --
// identical result in every case except the one where every version of a
// still-existing agent has been deleted via DeleteAgentVersion, where the
// pre-conversion code returned "agent not found" (arguably itself a
// mislabeled error) and this returns "agent version not found".
func (b *InMemoryBackend) GetAgentVersion(
	_ context.Context, agentID, agentVersion string,
) (*AgentVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	av, ok := b.agentVersions.Get(agentVersionKey(agentID, agentVersion))
	if !ok {
		return nil, fmt.Errorf("%w: agent version %q not found", ErrNotFound, agentVersion)
	}

	return agentVersionCopy(av), nil
}

// DeleteAgentVersion deletes an agent version. See the not-found precedence
// note on GetAgentVersion.
func (b *InMemoryBackend) DeleteAgentVersion(
	_ context.Context, agentID, agentVersion string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	key := agentVersionKey(agentID, agentVersion)
	if !b.agentVersions.Has(key) {
		return fmt.Errorf("%w: agent version %q not found", ErrNotFound, agentVersion)
	}

	b.agentVersions.Delete(key)

	return nil
}

// ListAgentVersions returns paginated agent version summaries.
func (b *InMemoryBackend) ListAgentVersions(
	_ context.Context, agentID string, maxResults int, nextToken string,
) ([]*AgentVersionSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.agents.Has(agentID) {
		return nil, "", fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	group := b.agentVersionsByAgent.Get(agentID)
	keys := tableIDs(group, func(av *AgentVersion) string { return av.AgentVersion })
	keys, outToken := paginate(keys, nextToken, maxResults)

	out := make([]*AgentVersionSummary, 0, len(keys))

	for _, k := range keys {
		av, _ := b.agentVersions.Get(agentVersionKey(agentID, k))
		out = append(out, &AgentVersionSummary{
			AgentName:    av.AgentName,
			AgentVersion: av.AgentVersion,
			AgentStatus:  av.AgentStatus,
			Description:  av.Description,
			UpdatedAt:    av.UpdatedAt,
		})
	}

	return out, outToken, nil
}

func agentVersionCopy(av *AgentVersion) *AgentVersion {
	cp := *av

	return &cp
}
