package bedrockagent_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAgents_MaxResultsHonoured proves that ListAgents' real SDK
// serializer binds maxResults/nextToken to the POST body (confirmed against
// aws-sdk-go-v2/service/bedrockagent@v1.58.4's
// awsRestjson1_serializeOpHttpBindingsListAgentsInput, which has no query
// bindings at all) -- so a handler that only reads the URL query string, as
// this one did before the fix, silently ignores every real client's
// maxResults/nextToken and always returns everything on one page.
func TestListAgents_MaxResultsHonoured(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	const agentCount = 3

	for i := range agentCount {
		_, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
			AgentName:            aws.String(fmt.Sprintf("agent-%d", i)),
			FoundationModel:      aws.String("anthropic.claude-v2"),
			AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/AmazonBedrockRole"),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListAgents(t.Context(), &bedrockagentsdk.ListAgentsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.AgentSummaries, 1, "maxResults=1 must limit the page to 1 item")
	require.NotNil(t, page1.NextToken, "a partial page must return a nextToken")

	page2, err := client.ListAgents(t.Context(), &bedrockagentsdk.ListAgentsInput{
		MaxResults: aws.Int32(agentCount),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AgentSummaries, agentCount-1, "second page must return the remainder")
}

// TestListAgentAliases_MaxResultsHonoured is the same binding proof for
// ListAgentAliases (also body-bound aside from its agentId path parameter,
// per that op's own httpBindings serializer).
func TestListAgentAliases_MaxResultsHonoured(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	agentOut, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName:            aws.String("alias-parent-agent"),
		FoundationModel:      aws.String("anthropic.claude-v2"),
		AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/AmazonBedrockRole"),
	})
	require.NoError(t, err)

	agentID := aws.ToString(agentOut.Agent.AgentId)

	const aliasCount = 3

	for i := range aliasCount {
		_, createErr := client.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
			AgentId:        aws.String(agentID),
			AgentAliasName: aws.String(fmt.Sprintf("alias-%d", i)),
		})
		require.NoError(t, createErr)
	}

	page1, err := client.ListAgentAliases(t.Context(), &bedrockagentsdk.ListAgentAliasesInput{
		AgentId:    aws.String(agentID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.AgentAliasSummaries, 1, "maxResults=1 must limit the page to 1 item")
	require.NotEmpty(t, aws.ToString(page1.NextToken), "a partial page must return a nextToken")

	page2, err := client.ListAgentAliases(t.Context(), &bedrockagentsdk.ListAgentAliasesInput{
		AgentId:    aws.String(agentID),
		MaxResults: aws.Int32(aliasCount),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AgentAliasSummaries, aliasCount-1, "second page must return the remainder")
}

// TestListAgents_StaleCursorTerminates proves ListAgents' pagination no
// longer loops forever on a stale NextToken (gopherstack pagination-arithmetic
// Class B: the shared paginate() helper searched for the token's agent by
// equality and left start at its zero value on a miss, so a client resuming
// with a cursor naming a since-deleted agent got page one again, forever).
// Deleting the agent the first page's cursor names and resuming with that
// cursor must return a real (possibly empty) response, not loop.
func TestListAgents_StaleCursorTerminates(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	const agentCount = 3

	agentIDs := make([]string, 0, agentCount)

	for i := range agentCount {
		out, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
			AgentName:            aws.String(fmt.Sprintf("stale-agent-%d", i)),
			FoundationModel:      aws.String("anthropic.claude-v2"),
			AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/AmazonBedrockRole"),
		})
		require.NoError(t, err)
		agentIDs = append(agentIDs, aws.ToString(out.Agent.AgentId))
	}

	page1, err := client.ListAgents(t.Context(), &bedrockagentsdk.ListAgentsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.NotNil(t, page1.NextToken)
	staleToken := aws.ToString(page1.NextToken)

	// Delete every agent so the cursor's named agent is gone, then resume.
	for _, id := range agentIDs {
		_, delErr := client.DeleteAgent(t.Context(), &bedrockagentsdk.DeleteAgentInput{
			AgentId:                aws.String(id),
			SkipResourceInUseCheck: true,
		})
		require.NoError(t, delErr)
	}

	page2, err := client.ListAgents(t.Context(), &bedrockagentsdk.ListAgentsInput{
		MaxResults: aws.Int32(agentCount),
		NextToken:  aws.String(staleToken),
	})
	require.NoError(t, err, "resuming with a stale cursor must not error or hang")
	require.NotNil(t, page2, "must return a real response instead of looping")
	assert.Empty(t, page2.AgentSummaries, "every agent was deleted, so the resumed page must be empty")
}
