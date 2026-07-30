package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

func seedTestFlow(b *quicksight.InMemoryBackend, flowID, name string) {
	quicksight.SeedFlow(b, testAccountID, &quicksight.Flow{
		CreatedTime:     time.Now().UTC(),
		LastUpdatedTime: time.Now().UTC(),
		FlowID:          flowID,
		Name:            name,
		PublishState:    "PUBLISHED",
	})
}

// ---- GetFlowMetadata: found and not-found ----

func TestQuickSight_GetFlowMetadata(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	seedTestFlow(backend, "flow1", "My Flow")
	h := quicksight.NewHandler(backend)

	rec := doRequest(t, h, http.MethodGet, accountPath("/flows/flow1/metadata"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	assert.Equal(t, "flow1", body["FlowId"])
	assert.Equal(t, "My Flow", body["Name"])
	assert.Equal(t, "PUBLISHED", body["PublishState"])
	assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:flow/flow1")

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/flows/notexist/metadata"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])
}

// ---- ListFlows pagination ----

func TestQuickSight_ListFlows_Pagination(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		seedTestFlow(backend, id, id)
	}
	h := quicksight.NewHandler(backend)

	rec := doRequest(t, h, http.MethodGet, accountPath("/flows?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["FlowSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["FlowId"].(string)] = true
	}

	page2 := doRequest(t, h, http.MethodGet, accountPath("/flows?max-results=2&next-token="+next), nil)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["FlowSummaryList"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["FlowId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- SearchFlows filters by assetName ----

func TestQuickSight_SearchFlows(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	seedTestFlow(backend, "f1", "Onboarding Flow")
	seedTestFlow(backend, "f2", "Onboarding Follow-up")
	seedTestFlow(backend, "f3", "Billing Flow")
	h := quicksight.NewHandler(backend)

	rec := doRequest(t, h, http.MethodPost, accountPath("/flows/searchFlows"), map[string]any{
		"Filters": []any{
			map[string]any{"Name": "assetName", "Operator": "StringLike", "Value": "Onboarding"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	items := parseBody(t, rec)["FlowSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---- Flow permissions grant/revoke ----

func TestQuickSight_FlowPermissions(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	seedTestFlow(backend, "flow1", "My Flow")
	h := quicksight.NewHandler(backend)

	updateRec := doRequest(t, h, http.MethodPut, accountPath("/flows/flow1/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:GetFlow"},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	perms := parseBody(t, updateRec)["Permissions"].([]any)
	require.Len(t, perms, 1)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/flows/flow1/permissions"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describePerms := parseBody(t, describeRec)["Permissions"].([]any)
	require.Len(t, describePerms, 1)

	revokeRec := doRequest(t, h, http.MethodPut, accountPath("/flows/flow1/permissions"), map[string]any{
		"RevokePermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:GetFlow"},
			},
		},
	})
	require.Equal(t, http.StatusOK, revokeRec.Code)
	assert.Empty(t, parseBody(t, revokeRec)["Permissions"])

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/flows/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}

// ---- CreateFlow/DescribeFlow/UpdateFlow/DeleteFlow: added to the SDK after
// the prior parity pass (see PARITY.md). Each case builds its own handler so
// subtests can run in parallel with no shared backend state. ----

func TestQuickSight_FlowCreateDescribeUpdateDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create and describe returns the stored definition",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/flows"), map[string]any{
					"Name":           "Onboarding",
					"Description":    "onboarding flow",
					"FlowDefinition": map[string]any{"steps": []any{"step1"}},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				createBody := parseBody(t, createRec)
				flowID, ok := createBody["FlowId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, flowID)
				assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:flow/"+flowID)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/flows/"+flowID), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				flow, ok := parseBody(t, describeRec)["Flow"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Onboarding", flow["Name"])
				assert.Equal(t, "onboarding flow", flow["Description"])
				assert.Equal(t, "PUBLISHED", flow["PublishState"])
				def, ok := flow["FlowDefinition"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, []any{"step1"}, def["steps"])
				// FlowDetail carries no RunCount/UserCount/LastPublishedAt/
				// LastPublishedBy (unlike FlowSummary) -- confirmed against
				// types.FlowDetail.
				assert.NotContains(t, flow, "RunCount")
				assert.NotContains(t, flow, "UserCount")
			},
		},
		{
			name: "create with initial permissions",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/flows"), map[string]any{
					"Name":           "Perms Flow",
					"FlowDefinition": map[string]any{},
					"Permissions": []any{
						map[string]any{
							"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
							"Actions":   []any{"quicksight:DescribeFlowPermissions"},
						},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				flowID := parseBody(t, createRec)["FlowId"].(string)

				permsRec := doRequest(t, h, http.MethodGet, accountPath("/flows/"+flowID+"/permissions"), nil)
				require.Equal(t, http.StatusOK, permsRec.Code)
				perms := parseBody(t, permsRec)["Permissions"].([]any)
				require.Len(t, perms, 1)
			},
		},
		{
			name: "update replaces definition and name",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/flows"), map[string]any{
					"Name": "Original", "FlowDefinition": map[string]any{"steps": []any{}},
				})
				flowID := parseBody(t, createRec)["FlowId"].(string)

				updateRec := doRequest(t, h, http.MethodPut, accountPath("/flows/"+flowID), map[string]any{
					"Name": "Updated", "FlowDefinition": map[string]any{"steps": []any{"a", "b"}},
				})
				require.Equal(t, http.StatusOK, updateRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/flows/"+flowID), nil)
				flow := parseBody(t, describeRec)["Flow"].(map[string]any)
				assert.Equal(t, "Updated", flow["Name"])
				def := flow["FlowDefinition"].(map[string]any)
				assert.Equal(t, []any{"a", "b"}, def["steps"])
			},
		},
		{
			name: "delete removes the flow and its tags",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/flows"), map[string]any{
					"Name": "ToDelete", "FlowDefinition": map[string]any{},
				})
				flowID := parseBody(t, createRec)["FlowId"].(string)
				flowArn := parseBody(t, createRec)["Arn"].(string)

				tagRec := doRequest(t, h, http.MethodPost, "/resources/"+flowArn+"/tags", map[string]any{
					"Tags": []any{map[string]any{"Key": "env", "Value": "test"}},
				})
				require.Equal(t, http.StatusOK, tagRec.Code)

				deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/flows/"+flowID), nil)
				require.Equal(t, http.StatusOK, deleteRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/flows/"+flowID), nil)
				assert.Equal(t, http.StatusNotFound, describeRec.Code)

				// Cascade: the deleted flow's tags are gone too (arnExists
				// no longer recognizes its ARN).
				tagsAfterRec := doRequest(t, h, http.MethodGet, "/resources/"+flowArn+"/tags", nil)
				assert.Equal(t, http.StatusNotFound, tagsAfterRec.Code)
			},
		},
		{
			name: "describe/update/delete on a missing flow all 404",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/flows/notexist"), nil)
				assert.Equal(t, http.StatusNotFound, describeRec.Code)

				updateRec := doRequest(
					t, h, http.MethodPut, accountPath("/flows/notexist"), map[string]any{"Name": "x"},
				)
				assert.Equal(t, http.StatusNotFound, updateRec.Code)

				deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/flows/notexist"), nil)
				assert.Equal(t, http.StatusNotFound, deleteRec.Code)
			},
		},
		{
			name: "create without a FlowDefinition is a validation error",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				rec := doRequest(t, h, http.MethodPost, accountPath("/flows"), map[string]any{"Name": "NoDefinition"})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Equal(t, "InvalidParameterValueException", parseBody(t, rec)["Code"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// ---- Agents: CreateAgent/DescribeAgent/UpdateAgent/DeleteAgent/
// ListAgents/SearchAgents/permissions, all added to the SDK after the prior
// parity pass. Each case builds its own handler so subtests can run in
// parallel with no shared backend state. ----

func TestQuickSight_Agents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create defaults AgentLifecycle to PREVIEW and describes back",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
					"AgentId": "agent1", "Name": "My Agent", "Description": "an agent",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				createBody := parseBody(t, createRec)
				assert.Equal(t, "agent1", createBody["AgentId"])
				assert.Equal(t, "My Agent", createBody["AgentName"])
				assert.Equal(t, "ACTIVE", createBody["AgentStatus"])
				assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:agent/agent1")

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/agents/agent1"), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				agent := parseBody(t, describeRec)["Agent"].(map[string]any)
				assert.Equal(t, "My Agent", agent["Name"])
				assert.Equal(t, "an agent", agent["Description"])
				assert.Equal(t, "PREVIEW", agent["AgentLifecycle"])
			},
		},
		{
			name: "duplicate create is a conflict",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				body := map[string]any{"AgentId": "dup1", "Name": "A"}
				first := doRequest(t, h, http.MethodPost, accountPath("/agents"), body)
				require.Equal(t, http.StatusOK, first.Code)

				second := doRequest(t, h, http.MethodPost, accountPath("/agents"), body)
				assert.Equal(t, http.StatusConflict, second.Code)
				assert.Equal(t, "ResourceExistsException", parseBody(t, second)["Code"])
			},
		},
		{
			name: "update attaches a real action connector and space, rejects a bogus ARN",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				acRec := doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
					"ActionConnectorId": "ac1", "Name": "AC1", "Type": "GENERIC_HTTP",
					"AuthenticationConfig": map[string]any{"AuthenticationType": "NO_AUTH"},
				})
				require.Equal(t, http.StatusOK, acRec.Code)
				acArn := parseBody(t, acRec)["Arn"].(string)

				spaceRec := doRequest(t, h, http.MethodPost, v1AccountPath("/spaces"), map[string]any{
					"SpaceId": "space1", "Name": "Space1",
				})
				require.Equal(t, http.StatusOK, spaceRec.Code)
				spaceArn := parseBody(t, spaceRec)["spaceArn"].(string)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
					"AgentId": "agent1", "Name": "My Agent",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				bogusArn := "arn:aws:quicksight:us-east-1:000000000000:action-connector/does-not-exist"
				updateRec := doRequest(t, h, http.MethodPut, accountPath("/agents/agent1"), map[string]any{
					"Name":                  "Renamed Agent",
					"ActionConnectorsToAdd": []any{acArn, bogusArn},
					"SpacesToAdd":           []any{spaceArn},
				})
				require.Equal(t, http.StatusOK, updateRec.Code)
				updateBody := parseBody(t, updateRec)
				failedAdd := updateBody["FailedToAddActionConnectors"].([]any)
				require.Len(t, failedAdd, 1)
				assert.Equal(t, bogusArn, failedAdd[0].(map[string]any)["Arn"])
				assert.Empty(t, updateBody["FailedToAddSpaces"])

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/agents/agent1"), nil)
				agent := parseBody(t, describeRec)["Agent"].(map[string]any)
				assert.Equal(t, "Renamed Agent", agent["Name"])
				assert.Equal(t, []any{acArn}, agent["ActionConnectors"])
				assert.Equal(t, []any{spaceArn}, agent["Spaces"])
			},
		},
		{
			name: "search filters by AGENT_NAME",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				for _, n := range []string{"Support Bot", "Support Escalation", "Billing Bot"} {
					rec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
						"AgentId": n, "Name": n,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				searchRec := doRequest(t, h, http.MethodPost, accountPath("/search/agents"), map[string]any{
					"Filters": []any{
						map[string]any{"Name": "AGENT_NAME", "Operator": "StringLike", "Value": "Support"},
					},
				})
				require.Equal(t, http.StatusOK, searchRec.Code)
				items := parseBody(t, searchRec)["AgentSummaries"].([]any)
				assert.Len(t, items, 2)
			},
		},
		{
			name: "permissions grant then revoke",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				doRequest(
					t, h, http.MethodPost, accountPath("/agents"), map[string]any{"AgentId": "agent1", "Name": "A"},
				)

				grantRec := doRequest(t, h, http.MethodPut, accountPath("/agents/agent1/permissions"), map[string]any{
					"GrantPermissions": []any{
						map[string]any{
							"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
							"Actions":   []any{"quicksight:DescribeAgent"},
						},
					},
				})
				require.Equal(t, http.StatusOK, grantRec.Code)
				require.Len(t, parseBody(t, grantRec)["Permissions"].([]any), 1)

				revokeRec := doRequest(t, h, http.MethodPut, accountPath("/agents/agent1/permissions"), map[string]any{
					"RevokePermissions": []any{
						map[string]any{
							"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
							"Actions":   []any{"quicksight:DescribeAgent"},
						},
					},
				})
				require.Equal(t, http.StatusOK, revokeRec.Code)
				assert.Empty(t, parseBody(t, revokeRec)["Permissions"])
			},
		},
		{
			name: "delete removes the agent and its tags",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
					"AgentId": "agent1", "Name": "A",
				})
				agentArn := parseBody(t, createRec)["Arn"].(string)

				tagRec := doRequest(t, h, http.MethodPost, "/resources/"+agentArn+"/tags", map[string]any{
					"Tags": []any{map[string]any{"Key": "env", "Value": "test"}},
				})
				require.Equal(t, http.StatusOK, tagRec.Code)

				deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/agents/agent1"), nil)
				require.Equal(t, http.StatusOK, deleteRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/agents/agent1"), nil)
				assert.Equal(t, http.StatusNotFound, describeRec.Code)

				tagsAfterRec := doRequest(t, h, http.MethodGet, "/resources/"+agentArn+"/tags", nil)
				assert.Equal(t, http.StatusNotFound, tagsAfterRec.Code)
			},
		},
		{
			name: "ListAgents pagination",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				for _, id := range []string{"a", "b", "c", "d", "e"} {
					rec := doRequest(
						t, h, http.MethodPost, accountPath("/agents"), map[string]any{"AgentId": id, "Name": id},
					)
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := doRequest(t, h, http.MethodGet, accountPath("/agents?max-results=2"), nil)
				require.Equal(t, http.StatusOK, rec.Code)
				body := parseBody(t, rec)
				items := body["AgentSummaries"].([]any)
				assert.Len(t, items, 2)
				next, ok := body["NextToken"].(string)
				require.True(t, ok)
				require.NotEmpty(t, next)
			},
		},
		{
			// CustomPromptInput.ExistingPrompt's three fields (ModelProfileId/
			// QbsAwsAccountId/SubscriptionId) are caller-supplied references to an
			// already-provisioned Amazon Q Business profile, not values this
			// backend has to mint -- so round-tripping them is a genuine, real
			// capability, not fabrication (see agents.go's CreateAgent doc
			// comment and PARITY.md).
			name: "CustomPromptInput ExistingPrompt round-trips on create and update",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
					"AgentId": "agent1", "Name": "My Agent",
					"CustomPromptInput": map[string]any{
						"ExistingPrompt": map[string]any{
							"ModelProfileId":  "profile-1",
							"QbsAwsAccountId": "111111111111",
							"SubscriptionId":  "sub-1",
						},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/agents/agent1"), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				agent := parseBody(t, describeRec)["Agent"].(map[string]any)
				cp := agent["CustomPromptInterface"].(map[string]any)
				assert.Equal(t, "profile-1", cp["ModelProfileId"])
				assert.Equal(t, "111111111111", cp["QbsAwsAccountId"])
				assert.Equal(t, "sub-1", cp["SubscriptionId"])

				updateRec := doRequest(t, h, http.MethodPut, accountPath("/agents/agent1"), map[string]any{
					"CustomPromptInput": map[string]any{
						"ExistingPrompt": map[string]any{
							"ModelProfileId":  "profile-2",
							"QbsAwsAccountId": "222222222222",
							"SubscriptionId":  "sub-2",
						},
					},
				})
				require.Equal(t, http.StatusOK, updateRec.Code)

				describeAfterRec := doRequest(t, h, http.MethodGet, accountPath("/agents/agent1"), nil)
				agentAfter := parseBody(t, describeAfterRec)["Agent"].(map[string]any)
				cpAfter := agentAfter["CustomPromptInterface"].(map[string]any)
				assert.Equal(t, "profile-2", cpAfter["ModelProfileId"])
				assert.Equal(t, "222222222222", cpAfter["QbsAwsAccountId"])
				assert.Equal(t, "sub-2", cpAfter["SubscriptionId"])
			},
		},
		{
			name: "CustomPromptInput ExistingPrompt missing a required field is rejected",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				rec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
					"AgentId": "agent1", "Name": "My Agent",
					"CustomPromptInput": map[string]any{
						"ExistingPrompt": map[string]any{
							"ModelProfileId": "profile-1",
							// QbsAwsAccountId/SubscriptionId omitted -- both are
							// "This member is required" on the real
							// CustomPromptProfile.
						},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			// NewPrompt asks AWS to mint a brand-new profile (and fresh
			// ModelProfileId/QbsAwsAccountId/SubscriptionId) via a live Amazon Q
			// Business subscription this backend has no state for -- accepted
			// without error (real AWS would accept it too, given a real
			// subscription) but honestly produces no CustomPromptInterface,
			// since synthesizing those three IDs would be fabrication.
			name: "CustomPromptInput NewPrompt is accepted but not echoed back",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, accountPath("/agents"), map[string]any{
					"AgentId": "agent1", "Name": "My Agent",
					"CustomPromptInput": map[string]any{
						"NewPrompt": map[string]any{
							"Identity": "a helpful assistant",
						},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, accountPath("/agents/agent1"), nil)
				agent := parseBody(t, describeRec)["Agent"].(map[string]any)
				assert.NotContains(t, agent, "CustomPromptInterface")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// ---- Knowledge Bases: CreateKnowledgeBase/DescribeKnowledgeBase/
// UpdateKnowledgeBase/DeleteKnowledgeBase/BatchDeleteKnowledgeBase/
// ListKnowledgeBases/SearchKnowledgeBases/permissions, minted under the
// "/v1/accounts/..." prefix (see classifyRequest's v1-strip). ----

func TestQuickSight_KnowledgeBases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create and describe round-trips DataSourceArn and configuration",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), map[string]any{
					"KnowledgeBaseId": "kb1",
					"Name":            "My KB",
					"DataSourceArn":   "arn:aws:s3:::my-bucket",
					"KnowledgeBaseConfiguration": map[string]any{
						"TemplateConfiguration": map[string]any{"foo": "bar"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				createBody := parseBody(t, createRec)
				assert.Equal(t, "kb1", createBody["KnowledgeBaseId"])
				assert.Equal(t, "ACTIVE", createBody["CreationStatus"])
				assert.Contains(
					t, createBody["KnowledgeBaseArn"],
					"arn:aws:quicksight:us-east-1:000000000000:knowledge-base/kb1",
				)

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/knowledge-bases/kb1"), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				kb := parseBody(t, describeRec)["KnowledgeBase"].(map[string]any)
				assert.Equal(t, "My KB", kb["Name"])
				assert.Equal(t, "arn:aws:s3:::my-bucket", kb["DataSourceArn"])
				assert.Equal(t, "ACTIVE", kb["Status"])
				cfg := kb["KnowledgeBaseConfiguration"].(map[string]any)
				assert.Equal(t, map[string]any{"foo": "bar"}, cfg["TemplateConfiguration"])
			},
		},
		{
			name: "duplicate create is a conflict",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				body := map[string]any{"KnowledgeBaseId": "dup1", "Name": "A", "DataSourceArn": "arn:aws:s3:::b"}
				first := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), body)
				require.Equal(t, http.StatusOK, first.Code)

				second := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), body)
				assert.Equal(t, http.StatusConflict, second.Code)
			},
		},
		{
			// Real AWS routes UpdateKnowledgeBase as POST (not PUT) to the
			// same path as Describe/Delete -- confirmed against
			// serializers.go.
			name: "update via POST renames the knowledge base",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), map[string]any{
					"KnowledgeBaseId": "kb1", "Name": "Original", "DataSourceArn": "arn:aws:s3:::b",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				updateRec := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases/kb1"), map[string]any{
					"Name": "Renamed",
				})
				require.Equal(t, http.StatusOK, updateRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/knowledge-bases/kb1"), nil)
				kb := parseBody(t, describeRec)["KnowledgeBase"].(map[string]any)
				assert.Equal(t, "Renamed", kb["Name"])
			},
		},
		{
			name: "BatchDeleteKnowledgeBase partitions per-item success/failure",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), map[string]any{
					"KnowledgeBaseId": "kb1", "Name": "A", "DataSourceArn": "arn:aws:s3:::b",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				batchRec := doRequest(
					t, h, http.MethodPost, v1AccountPath("/knowledge-bases/batch-delete"),
					map[string]any{"KnowledgeBaseIds": []any{"kb1", "does-not-exist"}},
				)
				require.Equal(t, http.StatusOK, batchRec.Code)
				batchBody := parseBody(t, batchRec)
				deleted := batchBody["Deleted"].([]any)
				require.Len(t, deleted, 1)
				assert.Equal(t, "kb1", deleted[0].(map[string]any)["KnowledgeBaseId"])
				errs := batchBody["Errors"].([]any)
				require.Len(t, errs, 1)
				assert.Equal(t, "does-not-exist", errs[0].(map[string]any)["KnowledgeBaseId"])

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/knowledge-bases/kb1"), nil)
				assert.Equal(t, http.StatusNotFound, describeRec.Code)
			},
		},
		{
			name: "search filters by KNOWLEDGE_BASE_NAME",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				for _, n := range []string{"Support KB", "Support Archive", "Billing KB"} {
					rec := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), map[string]any{
						"KnowledgeBaseId": n, "Name": n, "DataSourceArn": "arn:aws:s3:::b",
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				searchRec := doRequest(t, h, http.MethodPost, v1AccountPath("/search/knowledge-bases"), map[string]any{
					"Filters": []any{
						map[string]any{"Name": "KNOWLEDGE_BASE_NAME", "Operator": "StringLike", "Value": "Support"},
					},
				})
				require.Equal(t, http.StatusOK, searchRec.Code)
				items := parseBody(t, searchRec)["KnowledgeBaseSummaries"].([]any)
				assert.Len(t, items, 2)
			},
		},
		{
			// UpdateKnowledgeBasePermissions is also POST, not PUT.
			name: "permissions grant via POST then revoke",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), map[string]any{
					"KnowledgeBaseId": "kb1", "Name": "A", "DataSourceArn": "arn:aws:s3:::b",
				})

				grantRec := doRequest(
					t,
					h,
					http.MethodPost,
					v1AccountPath("/knowledge-bases/kb1/permissions"),
					map[string]any{
						"GrantPermissions": []any{
							map[string]any{
								"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
								"Actions":   []any{"quicksight:DescribeKnowledgeBase"},
							},
						},
					},
				)
				require.Equal(t, http.StatusOK, grantRec.Code)
				require.Len(t, parseBody(t, grantRec)["Permissions"].([]any), 1)

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/knowledge-bases/kb1/permissions"), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				require.Len(t, parseBody(t, describeRec)["Permissions"].([]any), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// ---- Spaces: CreateSpace/DescribeSpace/UpdateSpace/DeleteSpace/
// ListSpaces/SearchSpaces/permissions/resources. The Space family is the
// one QuickSight resource type whose envelope is camelCase
// (spaceId/spaceArn) instead of PascalCase -- these tests assert on the
// lowercase keys deliberately, matching the real wire shape (see
// handler_spaces.go's wire-shape note). ----

func TestQuickSight_Spaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create and describe use camelCase spaceId/spaceArn",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(t, h, http.MethodPost, v1AccountPath("/spaces"), map[string]any{
					"SpaceId": "space1", "Name": "My Space", "Description": "a space",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				createBody := parseBody(t, createRec)
				assert.Equal(t, "space1", createBody["spaceId"])
				assert.Contains(t, createBody["spaceArn"], "arn:aws:quicksight:us-east-1:000000000000:space/space1")
				assert.NotContains(t, createBody, "SpaceId", "response must be camelCase, not PascalCase")

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/spaces/space1"), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				describeBody := parseBody(t, describeRec)
				assert.Equal(t, "space1", describeBody["spaceId"])
				space := describeBody["Space"].(map[string]any)
				assert.Equal(t, "My Space", space["name"])
				assert.Equal(t, "a space", space["description"])
				assert.NotContains(t, space, "spaceId", "nested SpaceDetails carries no id/arn of its own")
				assert.Equal(t, []any{}, describeBody["Contributors"])
			},
		},
		{
			name: "duplicate create is a conflict",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				body := map[string]any{"SpaceId": "dup1", "Name": "A"}
				first := doRequest(t, h, http.MethodPost, v1AccountPath("/spaces"), body)
				require.Equal(t, http.StatusOK, first.Code)

				second := doRequest(t, h, http.MethodPost, v1AccountPath("/spaces"), body)
				assert.Equal(t, http.StatusConflict, second.Code)
			},
		},
		{
			name: "update renames the space",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				doRequest(
					t,
					h,
					http.MethodPost,
					v1AccountPath("/spaces"),
					map[string]any{"SpaceId": "space1", "Name": "Original"},
				)

				updateRec := doRequest(
					t,
					h,
					http.MethodPut,
					v1AccountPath("/spaces/space1"),
					map[string]any{"Name": "Renamed"},
				)
				require.Equal(t, http.StatusOK, updateRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/spaces/space1"), nil)
				space := parseBody(t, describeRec)["Space"].(map[string]any)
				assert.Equal(t, "Renamed", space["name"])
			},
		},
		{
			name: "search filters by SPACE_NAME",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				for _, n := range []string{"Support Space", "Support Archive", "Billing Space"} {
					rec := doRequest(
						t,
						h,
						http.MethodPost,
						v1AccountPath("/spaces"),
						map[string]any{"SpaceId": n, "Name": n},
					)
					require.Equal(t, http.StatusOK, rec.Code)
				}

				searchRec := doRequest(t, h, http.MethodPost, v1AccountPath("/search/spaces"), map[string]any{
					"Filters": []any{
						map[string]any{"Name": "SPACE_NAME", "Operator": "StringLike", "Value": "Support"},
					},
				})
				require.Equal(t, http.StatusOK, searchRec.Code)
				items := parseBody(t, searchRec)["SpaceSummaries"].([]any)
				assert.Len(t, items, 2)
				assert.NotEmpty(t, items[0].(map[string]any)["spaceId"])
			},
		},
		{
			// UpdateSpacePermissionsOutput is uniquely fully-lowercase, even
			// for "permissions"/"requestId" -- confirmed against
			// deserializers.go.
			name: "permissions grant response is fully lowercase",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				doRequest(
					t,
					h,
					http.MethodPost,
					v1AccountPath("/spaces"),
					map[string]any{"SpaceId": "space1", "Name": "A"},
				)

				grantRec := doRequest(t, h, http.MethodPut, v1AccountPath("/spaces/space1/permissions"), map[string]any{
					"GrantPermissions": []any{
						map[string]any{
							"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
							"Actions":   []any{"quicksight:DescribeSpace"},
						},
					},
				})
				require.Equal(t, http.StatusOK, grantRec.Code)
				grantBody := parseBody(t, grantRec)
				assert.Equal(t, "space1", grantBody["spaceId"])
				require.Len(t, grantBody["permissions"].([]any), 1)
				assert.NotContains(t, grantBody, "Permissions")
				assert.NotContains(t, grantBody, "RequestId")

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/spaces/space1/permissions"), nil)
				require.Equal(t, http.StatusOK, describeRec.Code)
				// DescribeSpacePermissionsOutput keeps "Permissions" PascalCase
				// even though spaceId/spaceArn stay camelCase.
				require.Len(t, parseBody(t, describeRec)["Permissions"].([]any), 1)
			},
		},
		{
			name: "UpdateSpaceResources attaches a real dataset and rejects a bogus ARN",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				doRequest(
					t,
					h,
					http.MethodPost,
					v1AccountPath("/spaces"),
					map[string]any{"SpaceId": "space1", "Name": "A"},
				)

				dsRec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "ds1", "Name": "DS1", "ImportMode": "SPICE",
				})
				require.Equal(t, http.StatusCreated, dsRec.Code)
				dsArn := parseBody(t, dsRec)["Arn"].(string)
				bogusArn := "arn:aws:quicksight:us-east-1:000000000000:dataset/does-not-exist"

				updateRec := doRequest(t, h, http.MethodPut, v1AccountPath("/spaces/space1/resources"), map[string]any{
					"AddResources": []any{
						map[string]any{
							"ResourceDetails": map[string]any{"resourceArn": dsArn}, "ResourceType": "DATA_SET",
						},
						map[string]any{
							"ResourceDetails": map[string]any{"resourceArn": bogusArn}, "ResourceType": "DATA_SET",
						},
					},
				})
				require.Equal(t, http.StatusOK, updateRec.Code)
				failed := parseBody(t, updateRec)["FailedResourceOperations"].([]any)
				require.Len(t, failed, 1)
				failedDetails := failed[0].(map[string]any)["ResourceDetails"].(map[string]any)
				assert.Equal(t, bogusArn, failedDetails["resourceArn"])

				listRec := doRequest(t, h, http.MethodGet, v1AccountPath("/spaces/space1/resources"), nil)
				require.Equal(t, http.StatusOK, listRec.Code)
				resources := parseBody(t, listRec)["SpaceResources"].([]any)
				require.Len(t, resources, 1)
				details := resources[0].(map[string]any)["ResourceDetails"].(map[string]any)
				assert.Equal(t, dsArn, details["resourceArn"])

				removeRec := doRequest(t, h, http.MethodPut, v1AccountPath("/spaces/space1/resources"), map[string]any{
					"RemoveResources": []any{
						map[string]any{
							"ResourceDetails": map[string]any{"resourceArn": dsArn}, "ResourceType": "DATA_SET",
						},
					},
				})
				require.Equal(t, http.StatusOK, removeRec.Code)

				listAfterRec := doRequest(t, h, http.MethodGet, v1AccountPath("/spaces/space1/resources"), nil)
				assert.Empty(t, parseBody(t, listAfterRec)["SpaceResources"])
			},
		},
		{
			name: "delete removes the space and its tags",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				createRec := doRequest(
					t,
					h,
					http.MethodPost,
					v1AccountPath("/spaces"),
					map[string]any{"SpaceId": "space1", "Name": "A"},
				)
				spaceArn := parseBody(t, createRec)["spaceArn"].(string)

				tagRec := doRequest(t, h, http.MethodPost, "/resources/"+spaceArn+"/tags", map[string]any{
					"Tags": []any{map[string]any{"Key": "env", "Value": "test"}},
				})
				require.Equal(t, http.StatusOK, tagRec.Code)

				deleteRec := doRequest(t, h, http.MethodDelete, v1AccountPath("/spaces/space1"), nil)
				require.Equal(t, http.StatusOK, deleteRec.Code)

				describeRec := doRequest(t, h, http.MethodGet, v1AccountPath("/spaces/space1"), nil)
				assert.Equal(t, http.StatusNotFound, describeRec.Code)

				tagsAfterRec := doRequest(t, h, http.MethodGet, "/resources/"+spaceArn+"/tags", nil)
				assert.Equal(t, http.StatusNotFound, tagsAfterRec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// ---- ListUsersIndexCapacity: derived from real KnowledgeBase/Space
// ownership state, not fabricated. ----

func TestQuickSight_ListUsersIndexCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "no users returns an empty list",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				rec := doRequest(t, h, http.MethodPost, accountPath("/quick-index/user-capacity"), map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
				body := parseBody(t, rec)
				assert.Empty(t, body["users"])
				assert.Contains(t, body, "requestId")
			},
		},
		{
			name: "a user's owned knowledge base is counted for real",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)

				userRec := doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName":     "alice",
					"Email":        "alice@example.com",
					"IdentityType": "QUICKSIGHT",
					"UserRole":     "AUTHOR",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userArn := parseBody(t, userRec)["User"].(map[string]any)["Arn"].(string)

				kbRec := doRequest(t, h, http.MethodPost, v1AccountPath("/knowledge-bases"), map[string]any{
					"KnowledgeBaseId": "kb1", "Name": "KB1", "DataSourceArn": "arn:aws:s3:::b",
					"PrimaryOwnerArn": userArn,
				})
				require.Equal(t, http.StatusOK, kbRec.Code)

				rec := doRequest(
					t, h, http.MethodPost, accountPath("/quick-index/user-capacity"),
					map[string]any{"namespace": testNamespace},
				)
				require.Equal(t, http.StatusOK, rec.Code)
				users := parseBody(t, rec)["users"].([]any)
				require.Len(t, users, 1)
				u := users[0].(map[string]any)
				assert.Equal(t, "alice", u["userName"])
				assert.InEpsilon(t, float64(1), u["kbCount"], 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// v1AccountPath builds a "/v1/accounts/{id}{sub}" path, used by the
// KnowledgeBase and Space families (the only QuickSight operations minted
// under a "/v1" prefix -- see classifyRequest's v1-strip in
// handler_paths.go).
func v1AccountPath(sub string) string {
	return "/v1" + accountPath(sub)
}
