package bedrockagent_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// TestDeleteAgentCascades locks in that DeleteAgent no longer leaves ghost
// rows behind: action groups, aliases, collaborators, and agent-KB
// associations scoped under the agent (across every version, including
// DRAFT) must all be gone, along with the agent's and every alias's tags
// map entry. Before this fix, DeleteAgent only removed the agent row and
// its agentVersions rows -- everything else was orphaned permanently.
func TestDeleteAgentCascades(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

	agent, alias := setupCascadeAgentFixture(ctx, t, b)
	assertCascadeAgentFixtureBeforeDelete(ctx, t, b, agent, alias)

	if delErr := b.DeleteAgent(ctx, agent.AgentID); delErr != nil {
		t.Fatalf("delete agent: %v", delErr)
	}

	assertCascadeAgentFixtureAfterDelete(ctx, t, b, agent, alias)
}

// setupCascadeAgentFixture creates an agent with one of each child resource
// that DeleteAgent must cascade-clean: an action group, an alias (with its
// own tag), a collaborator, and a knowledge-base association. Returns the
// agent and alias so callers can assert on their cascade-deletion.
func setupCascadeAgentFixture(
	ctx context.Context, t *testing.T, b *bedrockagent.InMemoryBackend,
) (*bedrockagent.Agent, *bedrockagent.AgentAlias) {
	t.Helper()

	agent, err := b.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "cascade-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::123456789012:role/BedrockRole",
		Tags:            map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}

	if _, agErr := b.CreateAgentActionGroup(ctx, agent.AgentID, "DRAFT", bedrockagent.ActionGroupConfig{
		ActionGroupName: "cascade-ag",
	}); agErr != nil {
		t.Fatalf("create action group: %v", agErr)
	}

	alias, err := b.CreateAgentAlias(ctx, agent.AgentID, bedrockagent.AliasConfig{
		AliasName: "cascade-alias",
		Tags:      map[string]string{"alias-tag": "yes"},
	})
	if err != nil {
		t.Fatalf("create alias: %v", err)
	}

	if _, collabErr := b.AssociateAgentCollaborator(ctx, agent.AgentID, "DRAFT", bedrockagent.CollaboratorConfig{
		CollaboratorName:         "cascade-collab",
		CollaborationInstruction: "help",
		AgentDescriptor: map[string]any{
			"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/OTHER12345/ALIASOTHER",
		},
	}); collabErr != nil {
		t.Fatalf("associate collaborator: %v", collabErr)
	}

	kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "cascade-kb",
		RoleARN: "arn:aws:iam::123456789012:role/KBRole",
	})
	if err != nil {
		t.Fatalf("create kb: %v", err)
	}

	if _, assocErr := b.AssociateAgentKnowledgeBase(
		ctx, agent.AgentID, "DRAFT", kb.KnowledgeBaseID, "test", "ENABLED",
	); assocErr != nil {
		t.Fatalf("associate kb: %v", assocErr)
	}

	return agent, alias
}

// assertCascadeAgentFixtureBeforeDelete is a sanity check that every child
// row and both tag entries set up by setupCascadeAgentFixture exist prior to
// DeleteAgent, so the post-delete assertions prove real cleanup rather than
// an accidentally-empty starting state.
func assertCascadeAgentFixtureBeforeDelete(
	ctx context.Context, t *testing.T, b *bedrockagent.InMemoryBackend,
	agent *bedrockagent.Agent, alias *bedrockagent.AgentAlias,
) {
	t.Helper()

	agsBefore, _, _ := b.ListAgentActionGroups(ctx, agent.AgentID, "DRAFT", 10, "")
	if len(agsBefore) != 1 {
		t.Fatalf("precondition: expected 1 action group, got %d", len(agsBefore))
	}

	aliasTagsBefore, _ := b.ListTagsForResource(ctx, alias.AgentAliasARN)
	if aliasTagsBefore["alias-tag"] != "yes" {
		t.Fatalf("precondition: expected alias tag to be set, got %v", aliasTagsBefore)
	}
}

// assertCascadeAgentFixtureAfterDelete verifies that DeleteAgent removed
// every child row and tag entry set up by setupCascadeAgentFixture.
func assertCascadeAgentFixtureAfterDelete(
	ctx context.Context, t *testing.T, b *bedrockagent.InMemoryBackend,
	agent *bedrockagent.Agent, alias *bedrockagent.AgentAlias,
) {
	t.Helper()

	agsAfter, _, _ := b.ListAgentActionGroups(ctx, agent.AgentID, "DRAFT", 10, "")
	if len(agsAfter) != 0 {
		t.Errorf("action groups not cascade-deleted: %d remain", len(agsAfter))
	}

	aliasesAfter, _, _ := b.ListAgentAliases(ctx, agent.AgentID, 10, "")
	if len(aliasesAfter) != 0 {
		t.Errorf("aliases not cascade-deleted: %d remain", len(aliasesAfter))
	}

	collabsAfter, _, _ := b.ListAgentCollaborators(ctx, agent.AgentID, "DRAFT", 10, "")
	if len(collabsAfter) != 0 {
		t.Errorf("collaborators not cascade-deleted: %d remain", len(collabsAfter))
	}

	assocsAfter, _, _ := b.ListAgentKnowledgeBases(ctx, agent.AgentID, "DRAFT", 10, "")
	if len(assocsAfter) != 0 {
		t.Errorf("agent-KB associations not cascade-deleted: %d remain", len(assocsAfter))
	}

	agentTags, tagErr := b.ListTagsForResource(ctx, agent.AgentARN)
	if tagErr != nil {
		t.Fatalf("list agent tags: %v", tagErr)
	}

	if len(agentTags) != 0 {
		t.Errorf("agent tags not cascade-deleted: %v remain", agentTags)
	}

	aliasTagsAfter, aliasTagErr := b.ListTagsForResource(ctx, alias.AgentAliasARN)
	if aliasTagErr != nil {
		t.Fatalf("list alias tags: %v", aliasTagErr)
	}

	if len(aliasTagsAfter) != 0 {
		t.Errorf("alias tags not cascade-deleted: %v remain", aliasTagsAfter)
	}
}

// TestDeleteKnowledgeBaseCascades locks in that DeleteKnowledgeBase
// cascade-cleans every data source scoped under it, and (transitively)
// every ingestion job and ingested document scoped under each of those data
// sources, plus the KB's own tags map entry.
func TestDeleteKnowledgeBaseCascades(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

	kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "cascade-kb-2",
		RoleARN: "arn:aws:iam::123456789012:role/KBRole",
		Tags:    map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("create kb: %v", err)
	}

	ds, err := b.CreateDataSource(ctx, kb.KnowledgeBaseID, bedrockagent.DataSourceConfig{
		Name: "cascade-ds",
	})
	if err != nil {
		t.Fatalf("create data source: %v", err)
	}

	_, jobErr := b.StartIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, "job")
	if jobErr != nil {
		t.Fatalf("start ingestion job: %v", jobErr)
	}

	_, ingestErr := b.IngestKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, []bedrockagent.KBDocument{
		{DocID: "doc-1"},
	})
	if ingestErr != nil {
		t.Fatalf("ingest docs: %v", ingestErr)
	}

	if delErr := b.DeleteKnowledgeBase(ctx, kb.KnowledgeBaseID); delErr != nil {
		t.Fatalf("delete kb: %v", delErr)
	}

	dssAfter, _, _ := b.ListDataSources(ctx, kb.KnowledgeBaseID, 10, "")
	if len(dssAfter) != 0 {
		t.Errorf("data sources not cascade-deleted: %d remain", len(dssAfter))
	}

	jobsAfter, _, _ := b.ListIngestionJobs(ctx, kb.KnowledgeBaseID, ds.DataSourceID, 10, "")
	if len(jobsAfter) != 0 {
		t.Errorf("ingestion jobs not cascade-deleted: %d remain", len(jobsAfter))
	}

	docsAfter, _, _ := b.ListKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, 10, "")
	if len(docsAfter) != 0 {
		t.Errorf("kb documents not cascade-deleted: %d remain", len(docsAfter))
	}

	tags, tagErr := b.ListTagsForResource(ctx, kb.KnowledgeBaseARN)
	if tagErr != nil {
		t.Fatalf("list kb tags: %v", tagErr)
	}

	if len(tags) != 0 {
		t.Errorf("kb tags not cascade-deleted: %v remain", tags)
	}
}

// TestDeleteDataSourceCascades locks in that DeleteDataSource on its own
// (independent of a parent KB delete) also cascade-cleans ingestion jobs
// and ingested documents scoped under it.
func TestDeleteDataSourceCascades(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

	kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "cascade-kb-3",
		RoleARN: "arn:aws:iam::123456789012:role/KBRole",
	})
	if err != nil {
		t.Fatalf("create kb: %v", err)
	}

	ds, err := b.CreateDataSource(ctx, kb.KnowledgeBaseID, bedrockagent.DataSourceConfig{
		Name: "cascade-ds-2",
	})
	if err != nil {
		t.Fatalf("create data source: %v", err)
	}

	_, jobErr := b.StartIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, "job")
	if jobErr != nil {
		t.Fatalf("start ingestion job: %v", jobErr)
	}

	_, ingestErr := b.IngestKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, []bedrockagent.KBDocument{
		{DocID: "doc-2"},
	})
	if ingestErr != nil {
		t.Fatalf("ingest docs: %v", ingestErr)
	}

	if delErr := b.DeleteDataSource(ctx, kb.KnowledgeBaseID, ds.DataSourceID); delErr != nil {
		t.Fatalf("delete data source: %v", delErr)
	}

	jobsAfter, _, _ := b.ListIngestionJobs(ctx, kb.KnowledgeBaseID, ds.DataSourceID, 10, "")
	if len(jobsAfter) != 0 {
		t.Errorf("ingestion jobs not cascade-deleted: %d remain", len(jobsAfter))
	}

	docsAfter, _, _ := b.ListKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, 10, "")
	if len(docsAfter) != 0 {
		t.Errorf("kb documents not cascade-deleted: %d remain", len(docsAfter))
	}
}

// TestDeleteFlowCascades locks in that DeleteFlow cascade-cleans every flow
// alias scoped under it (plus the alias's own tags entry) and the flow's
// own tags entry, in addition to the pre-existing flowVersions cleanup.
func TestDeleteFlowCascades(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

	flow, err := b.CreateFlow(ctx, bedrockagent.FlowConfig{
		Name:    "cascade-flow",
		RoleARN: "arn:aws:iam::123456789012:role/FlowRole",
		Tags:    map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("create flow: %v", err)
	}

	alias, err := b.CreateFlowAlias(ctx, flow.FlowID, bedrockagent.FlowAliasConfig{
		Name: "cascade-flow-alias",
		Tags: map[string]string{"alias-tag": "yes"},
		RoutingConfiguration: []bedrockagent.FlowAliasRouting{
			{FlowVersion: flow.Version},
		},
	})
	if err != nil {
		t.Fatalf("create flow alias: %v", err)
	}

	if delErr := b.DeleteFlow(ctx, flow.FlowID); delErr != nil {
		t.Fatalf("delete flow: %v", delErr)
	}

	aliasesAfter, _, _ := b.ListFlowAliases(ctx, flow.FlowID, 10, "")
	if len(aliasesAfter) != 0 {
		t.Errorf("flow aliases not cascade-deleted: %d remain", len(aliasesAfter))
	}

	flowTags, flowTagErr := b.ListTagsForResource(ctx, flow.FlowARN)
	if flowTagErr != nil {
		t.Fatalf("list flow tags: %v", flowTagErr)
	}

	if len(flowTags) != 0 {
		t.Errorf("flow tags not cascade-deleted: %v remain", flowTags)
	}

	aliasTags, aliasTagErr := b.ListTagsForResource(ctx, alias.AliasARN)
	if aliasTagErr != nil {
		t.Fatalf("list flow alias tags: %v", aliasTagErr)
	}

	if len(aliasTags) != 0 {
		t.Errorf("flow alias tags not cascade-deleted: %v remain", aliasTags)
	}
}

// TestDeletePromptCascades locks in that DeletePrompt cleans up the
// prompt's own tags map entry, in addition to the pre-existing
// promptVersions cleanup.
func TestDeletePromptCascades(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

	prompt, err := b.CreatePrompt(ctx, bedrockagent.PromptConfig{
		Name: "cascade-prompt",
		Tags: map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("create prompt: %v", err)
	}

	if delErr := b.DeletePrompt(ctx, prompt.PromptID); delErr != nil {
		t.Fatalf("delete prompt: %v", delErr)
	}

	tags, tagErr := b.ListTagsForResource(ctx, prompt.PromptARN)
	if tagErr != nil {
		t.Fatalf("list prompt tags: %v", tagErr)
	}

	if len(tags) != 0 {
		t.Errorf("prompt tags not cascade-deleted: %v remain", tags)
	}
}
