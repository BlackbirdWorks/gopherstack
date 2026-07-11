package bedrockagent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving names/IDs.
type persistenceFixtureIDs struct {
	agentID        string
	agentVersion   string
	actionGroupID  string
	aliasID        string
	collaboratorID string
	kbID           string
	dataSourceID   string
	ingestionJobID string
	docID          string
	flowID         string
	flowVersion    string
	flowAliasID    string
	promptID       string
	promptVersion  string
	agentARN       string
	kbARN          string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus
// every raw map/counter left un-converted (agentsByName, kbsByName,
// flowsByName, promptsByName, agentVersionCtrs, flowVersionCtrs,
// promptVersionCtrs, tags, and the ID counters), so a Snapshot from it
// exercises the entire persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) (*bedrockagent.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	ctx := context.Background()
	b := bedrockagent.NewTestBackend("us-east-1", "000000000000")

	agent, err := b.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "test-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::000000000000:role/BedrockRole",
		Tags:            map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	_, err = b.PrepareAgent(ctx, agent.AgentID)
	require.NoError(t, err)

	av, err := b.CreateAgentVersion(ctx, agent.AgentID, "v1 snapshot")
	require.NoError(t, err)

	ag, err := b.CreateAgentActionGroup(ctx, agent.AgentID, bedrockagent.ActionGroupConfig{
		ActionGroupName: "test-action-group",
	})
	require.NoError(t, err)

	alias, err := b.CreateAgentAlias(ctx, agent.AgentID, bedrockagent.AliasConfig{
		AliasName: "test-alias",
	})
	require.NoError(t, err)

	collab, err := b.AssociateAgentCollaborator(ctx, agent.AgentID, "DRAFT", bedrockagent.CollaboratorConfig{
		CollaboratorName: "test-collaborator",
	})
	require.NoError(t, err)

	kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "test-kb",
		RoleARN: "arn:aws:iam::000000000000:role/BedrockRole",
		Tags:    map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	_, err = b.AssociateAgentKnowledgeBase(ctx, agent.AgentID, "DRAFT", kb.KnowledgeBaseID, "assoc", "")
	require.NoError(t, err)

	ds, err := b.CreateDataSource(ctx, kb.KnowledgeBaseID, bedrockagent.DataSourceConfig{
		Name: "test-ds",
	})
	require.NoError(t, err)

	job, err := b.StartIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, "test job")
	require.NoError(t, err)

	docs, err := b.IngestKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, []bedrockagent.KBDocument{
		{DocID: "doc-1"},
	})
	require.NoError(t, err)
	require.Len(t, docs, 1)

	flow, err := b.CreateFlow(ctx, bedrockagent.FlowConfig{
		Name:    "test-flow",
		RoleARN: "arn:aws:iam::000000000000:role/BedrockRole",
		Tags:    map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	fv, err := b.CreateFlowVersion(ctx, flow.FlowID, "v1 snapshot")
	require.NoError(t, err)

	falias, err := b.CreateFlowAlias(ctx, flow.FlowID, bedrockagent.FlowAliasConfig{
		Name: "test-flow-alias",
	})
	require.NoError(t, err)

	prompt, err := b.CreatePrompt(ctx, bedrockagent.PromptConfig{
		Name: "test-prompt",
		Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	pv, err := b.CreatePromptVersion(ctx, prompt.PromptID, "v1 snapshot")
	require.NoError(t, err)

	return b, persistenceFixtureIDs{
		agentID:        agent.AgentID,
		agentVersion:   av.AgentVersion,
		actionGroupID:  ag.ActionGroupID,
		aliasID:        alias.AgentAliasID,
		collaboratorID: collab.CollaboratorID,
		kbID:           kb.KnowledgeBaseID,
		dataSourceID:   ds.DataSourceID,
		ingestionJobID: job.IngestionJobID,
		docID:          docs[0].DocumentID,
		flowID:         flow.FlowID,
		flowVersion:    fv.Version,
		flowAliasID:    falias.AliasID,
		promptID:       prompt.PromptID,
		promptVersion:  pv.Version,
		agentARN:       agent.AgentARN,
		kbARN:          kb.KnowledgeBaseARN,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (agents, agentVersions, actionGroups, agentAliases,
// agentCollaborators, knowledgeBases, agentKBAssocs, dataSources,
// ingestionJobs, flows, flowVersions, flowAliases, prompts, promptVersions,
// kbDocuments), every secondary store.Index derived from them, and every raw
// map/counter left un-converted (agentsByName, kbsByName, flowsByName,
// promptsByName, agentVersionCtrs, flowVersionCtrs, promptVersionCtrs, tags,
// and the ID counters) through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := bedrockagent.NewTestBackend("us-east-1", "000000000000")
	require.NoError(t, fresh.Restore(ctx, snap))

	// agents table.
	agent, err := fresh.GetAgent(ctx, ids.agentID)
	require.NoError(t, err)
	assert.Equal(t, ids.agentID, agent.AgentID)
	assert.Equal(t, "test-agent", agent.AgentName)
	assert.Equal(t, ids.agentARN, agent.AgentARN)

	// agentsByName raw map: a duplicate CreateAgent by the same name must
	// still conflict after restore.
	_, err = fresh.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "test-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::000000000000:role/BedrockRole",
	})
	require.ErrorIs(t, err, bedrockagent.ErrAlreadyExists)

	// agentVersions table + byAgent index.
	av, err := fresh.GetAgentVersion(ctx, ids.agentID, ids.agentVersion)
	require.NoError(t, err)
	assert.Equal(t, ids.agentVersion, av.AgentVersion)

	avList, _, err := fresh.ListAgentVersions(ctx, ids.agentID, 0, "")
	require.NoError(t, err)
	require.Len(t, avList, 1)

	// actionGroups table + byAgentVersion index.
	ag, err := fresh.GetAgentActionGroup(ctx, ids.agentID, "DRAFT", ids.actionGroupID)
	require.NoError(t, err)
	assert.Equal(t, ids.actionGroupID, ag.ActionGroupID)

	agList, _, err := fresh.ListAgentActionGroups(ctx, ids.agentID, "DRAFT", 0, "")
	require.NoError(t, err)
	require.Len(t, agList, 1)

	// agentAliases table + byAgent index.
	alias, err := fresh.GetAgentAlias(ctx, ids.agentID, ids.aliasID)
	require.NoError(t, err)
	assert.Equal(t, ids.aliasID, alias.AgentAliasID)

	aliasList, _, err := fresh.ListAgentAliases(ctx, ids.agentID, 0, "")
	require.NoError(t, err)
	require.Len(t, aliasList, 1)

	// agentCollaborators table + byAgentVersion index.
	collab, err := fresh.GetAgentCollaborator(ctx, ids.agentID, "DRAFT", ids.collaboratorID)
	require.NoError(t, err)
	assert.Equal(t, ids.collaboratorID, collab.CollaboratorID)

	collabList, _, err := fresh.ListAgentCollaborators(ctx, ids.agentID, "DRAFT", 0, "")
	require.NoError(t, err)
	require.Len(t, collabList, 1)

	// knowledgeBases table.
	kb, err := fresh.GetKnowledgeBase(ctx, ids.kbID)
	require.NoError(t, err)
	assert.Equal(t, ids.kbID, kb.KnowledgeBaseID)
	assert.Equal(t, ids.kbARN, kb.KnowledgeBaseARN)

	// kbsByName raw map.
	_, err = fresh.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{Name: "test-kb"})
	require.ErrorIs(t, err, bedrockagent.ErrAlreadyExists)

	// agentKBAssocs table + byAgentVersion index.
	assoc, err := fresh.GetAgentKnowledgeBase(ctx, ids.agentID, "DRAFT", ids.kbID)
	require.NoError(t, err)
	assert.Equal(t, ids.kbID, assoc.KnowledgeBaseID)

	assocList, _, err := fresh.ListAgentKnowledgeBases(ctx, ids.agentID, "DRAFT", 0, "")
	require.NoError(t, err)
	require.Len(t, assocList, 1)

	// dataSources table + byKB index.
	ds, err := fresh.GetDataSource(ctx, ids.kbID, ids.dataSourceID)
	require.NoError(t, err)
	assert.Equal(t, ids.dataSourceID, ds.DataSourceID)

	dsList, _, err := fresh.ListDataSources(ctx, ids.kbID, 0, "")
	require.NoError(t, err)
	require.Len(t, dsList, 1)

	// ingestionJobs table + byDataSource index.
	job, err := fresh.GetIngestionJob(ctx, ids.kbID, ids.dataSourceID, ids.ingestionJobID)
	require.NoError(t, err)
	assert.Equal(t, ids.ingestionJobID, job.IngestionJobID)

	jobList, _, err := fresh.ListIngestionJobs(ctx, ids.kbID, ids.dataSourceID, 0, "")
	require.NoError(t, err)
	require.Len(t, jobList, 1)

	// kbDocuments table + byDataSource index.
	docs, err := fresh.GetKnowledgeBaseDocuments(ctx, ids.kbID, ids.dataSourceID, []string{ids.docID})
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, ids.docID, docs[0].DocumentID)

	docList, _, err := fresh.ListKnowledgeBaseDocuments(ctx, ids.kbID, ids.dataSourceID, 0, "")
	require.NoError(t, err)
	require.Len(t, docList, 1)

	// flows table.
	flow, err := fresh.GetFlow(ctx, ids.flowID)
	require.NoError(t, err)
	assert.Equal(t, ids.flowID, flow.FlowID)

	// flowsByName raw map.
	_, err = fresh.CreateFlow(ctx, bedrockagent.FlowConfig{Name: "test-flow"})
	require.ErrorIs(t, err, bedrockagent.ErrAlreadyExists)

	// flowVersions table + byFlow index.
	fv, err := fresh.GetFlowVersion(ctx, ids.flowID, ids.flowVersion)
	require.NoError(t, err)
	assert.Equal(t, ids.flowVersion, fv.Version)

	fvList, _, err := fresh.ListFlowVersions(ctx, ids.flowID, 0, "")
	require.NoError(t, err)
	require.Len(t, fvList, 1)

	// flowAliases table + byFlow index.
	falias, err := fresh.GetFlowAlias(ctx, ids.flowID, ids.flowAliasID)
	require.NoError(t, err)
	assert.Equal(t, ids.flowAliasID, falias.AliasID)

	faliasList, _, err := fresh.ListFlowAliases(ctx, ids.flowID, 0, "")
	require.NoError(t, err)
	require.Len(t, faliasList, 1)

	// prompts table.
	prompt, err := fresh.GetPrompt(ctx, ids.promptID)
	require.NoError(t, err)
	assert.Equal(t, ids.promptID, prompt.PromptID)

	// promptsByName raw map.
	_, err = fresh.CreatePrompt(ctx, bedrockagent.PromptConfig{Name: "test-prompt"})
	require.ErrorIs(t, err, bedrockagent.ErrAlreadyExists)

	// promptVersions table + byPrompt index.
	pv, err := fresh.GetPromptVersion(ctx, ids.promptID, ids.promptVersion)
	require.NoError(t, err)
	assert.Equal(t, ids.promptVersion, pv.Version)

	// tags raw map.
	tags, err := fresh.ListTagsForResource(ctx, ids.agentARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	tags, err = fresh.ListTagsForResource(ctx, ids.kbARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	// ID counters: a newly created agent after restore must not collide
	// with the agent created before the snapshot.
	agent2, err := fresh.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "post-restore-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::000000000000:role/BedrockRole",
	})
	require.NoError(t, err)
	assert.NotEqual(t, ids.agentID, agent2.AgentID)
	assert.True(t, strings.HasPrefix(agent2.AgentARN, "arn:aws:bedrock:us-east-1:000000000000:agent/"))
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including a snapshot with no
// version field at all, which decodes as Version == 0) is discarded cleanly
// rather than partially decoded: the backend resets to empty state and
// Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b, ids := newPersistenceTestBackend(t)

	err := b.Restore(ctx, []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	_, err = b.GetAgent(ctx, ids.agentID)
	require.ErrorIs(t, err, bedrockagent.ErrNotFound)

	_, err = b.GetFlow(ctx, ids.flowID)
	require.ErrorIs(t, err, bedrockagent.ErrNotFound)

	_, err = b.GetPrompt(ctx, ids.promptID)
	require.ErrorIs(t, err, bedrockagent.ErrNotFound)

	_, err = b.GetKnowledgeBase(ctx, ids.kbID)
	require.ErrorIs(t, err, bedrockagent.ErrNotFound)

	tags, err := b.ListTagsForResource(ctx, ids.agentARN)
	require.NoError(t, err) // tags reset to an empty map, not an error.
	assert.Empty(t, tags)

	// Reset ID counters too: the next agent minted after a discarded restore
	// must reuse the very first ID, exactly like a brand-new backend.
	agent, err := b.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "post-reset-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::000000000000:role/BedrockRole",
	})
	require.NoError(t, err)
	assert.Equal(t, "agent-00000001", agent.AgentID)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := bedrockagent.NewTestBackend("us-east-1", "000000000000")

	err := b.Restore(context.Background(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend. Before Phase 3.3, BedrockAgent had no such
// delegation, and no persistence underneath it either -- dead wiring, fixed
// for the first time by this conversion (see persistence.go).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, ids := newPersistenceTestBackend(t)
	h := bedrockagent.NewTestHandler(backend)

	snap := h.Snapshot(ctx)
	require.NotNil(t, snap)

	h2 := bedrockagent.NewTestHandler(bedrockagent.NewTestBackend("us-east-1", "000000000000"))
	require.NoError(t, h2.Restore(ctx, snap))

	agent, err := h2.Backend.GetAgent(ctx, ids.agentID)
	require.NoError(t, err)
	assert.Equal(t, ids.agentID, agent.AgentID)
}
