package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlowPromptArn_UseBedrockServiceSegment proves FlowArn, FlowAliasArn and
// PromptArn are built with the "bedrock" service segment, matching
// bedrock-agent@2023-06-05's FlowArn/FlowAliasArn/PromptArn patterns
// (service-2.json: "arn:aws:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:flow/...",
// not "bedrock-agent" -- the control-plane API is named bedrock-agent but
// its resource ARNs live under the bedrock service). Each expected ARN is
// built as a literal from the documented pattern, never via the handler's
// own arn.Build call, so a regression in that call cannot pass silently.
func TestFlowPromptArn_UseBedrockServiceSegment(t *testing.T) {
	t.Parallel()

	t.Run("create flow", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestAgentsHandler(t)

		rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{
			"name":             "arn-flow",
			"executionRoleArn": "arn:aws:iam::000000000000:role/role",
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		var body struct {
			Flow struct {
				FlowID  string `json:"flowId"`
				FlowArn string `json:"flowArn"`
			} `json:"flow"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

		wantArn := "arn:aws:bedrock:us-east-1:000000000000:flow/" + body.Flow.FlowID
		assert.Equal(t, wantArn, body.Flow.FlowArn)
	})

	t.Run("create flow alias", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestAgentsHandler(t)

		flowRec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{
			"name":             "arn-flow-for-alias",
			"executionRoleArn": "arn:aws:iam::000000000000:role/role",
		})
		require.Equal(t, http.StatusCreated, flowRec.Code)

		var flowBody struct {
			Flow struct {
				FlowID string `json:"flowId"`
			} `json:"flow"`
		}
		require.NoError(t, json.Unmarshal(flowRec.Body.Bytes(), &flowBody))

		aliasRec := doAgentRequest(t, h, http.MethodPost,
			"/flows/"+flowBody.Flow.FlowID+"/aliases", map[string]any{
				"name": "arn-alias",
			})
		require.Equal(t, http.StatusCreated, aliasRec.Code)

		var aliasBody struct {
			FlowAlias struct {
				FlowAliasID  string `json:"flowAliasId"`
				FlowAliasArn string `json:"flowAliasArn"`
			} `json:"flowAlias"`
		}
		require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &aliasBody))

		wantArn := "arn:aws:bedrock:us-east-1:000000000000:flow/" + flowBody.Flow.FlowID +
			"/alias/" + aliasBody.FlowAlias.FlowAliasID
		assert.Equal(t, wantArn, aliasBody.FlowAlias.FlowAliasArn)
	})

	t.Run("create prompt", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestAgentsHandler(t)

		rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
			"name": "arn-prompt",
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		var body struct {
			Prompt struct {
				PromptID  string `json:"promptId"`
				PromptArn string `json:"promptArn"`
			} `json:"prompt"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

		wantArn := "arn:aws:bedrock:us-east-1:000000000000:prompt/" + body.Prompt.PromptID
		assert.Equal(t, wantArn, body.Prompt.PromptArn)
	})
}
