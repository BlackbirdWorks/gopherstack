package securityhub_test

// parity_d_test.go — fresh audit fixes: configuration policy association
// TargetType derivation, InviteMembers unprocessed-account validation, and
// UpdateAutomationRuleV2 Actions field type handling.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ConfigurationPolicyAssociation_TargetTypeDerived verifies that
// TargetType in the association response is derived from which key is
// present in the request's Target union (AccountId/OrganizationalUnitId/
// RootId), matching aws-sdk-go-v2's types.Target wire shape. The real
// request never sends a "TargetType" field alongside Target -- reading one
// left TargetType permanently empty in the response.
func TestParity_ConfigurationPolicyAssociation_TargetTypeDerived(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target         map[string]any
		name           string
		wantTargetType string
	}{
		{name: "account", target: map[string]any{"AccountId": "111111111111"}, wantTargetType: "ACCOUNT"},
		{
			name:           "organizational_unit",
			target:         map[string]any{"OrganizationalUnitId": "ou-abcd-12345678"},
			wantTargetType: "ORGANIZATIONAL_UNIT",
		},
		{name: "root", target: map[string]any{"RootId": "r-abcd"}, wantTargetType: "ROOT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/configurationPolicy/create", map[string]any{
				"Name":                "test-policy",
				"Description":         "test",
				"ConfigurationPolicy": map[string]any{},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			policyID, _ := createResp["Id"].(string)
			require.NotEmpty(t, policyID)

			assocRec := doRequest(t, h, http.MethodPost, "/configurationPolicyAssociation/associate", map[string]any{
				"ConfigurationPolicyIdentifier": policyID,
				"Target":                        tc.target,
			})
			require.Equal(t, http.StatusOK, assocRec.Code)

			var assocResp map[string]any
			require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
			assert.Equal(t, tc.wantTargetType, assocResp["TargetType"],
				"TargetType must be derived from the Target union key, not read from a nonexistent request field")

			// GetConfigurationPolicyAssociation must derive the same TargetType
			// when looking the association back up.
			getRec := doRequest(t, h, http.MethodPost, "/configurationPolicyAssociation/get", map[string]any{
				"Target": tc.target,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, tc.wantTargetType, getResp["TargetType"])
		})
	}
}

// TestParity_InviteMembers_UnknownAccountUnprocessed verifies InviteMembers
// rejects account IDs that were never created via CreateMembers instead of
// silently succeeding for every account ID, matching real AWS behavior
// (InviteMembers requires a prior CreateMembers call).
func TestParity_InviteMembers_UnknownAccountUnprocessed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantUnproc    int
		preCreate     bool
		wantListedMem bool
	}{
		{name: "known_member_invited", preCreate: true, wantUnproc: 0, wantListedMem: true},
		{name: "unknown_account_unprocessed", preCreate: false, wantUnproc: 1, wantListedMem: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			const accountID = "222222222222"

			if tc.preCreate {
				createRec := doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"AccountDetails": []any{
						map[string]any{"AccountId": accountID, "Email": "m@example.com"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
			}

			inviteRec := doRequest(t, h, http.MethodPost, "/members/invite", map[string]any{
				"AccountIds": []any{accountID},
			})
			require.Equal(t, http.StatusOK, inviteRec.Code)

			var inviteResp map[string]any
			require.NoError(t, json.Unmarshal(inviteRec.Body.Bytes(), &inviteResp))

			unprocessed, _ := inviteResp["UnprocessedAccounts"].([]any)
			assert.Len(t, unprocessed, tc.wantUnproc)

			if tc.wantListedMem {
				membersRec := doRequest(t, h, http.MethodGet, "/members", nil)
				require.Equal(t, http.StatusOK, membersRec.Code)

				var membersResp map[string]any
				require.NoError(t, json.Unmarshal(membersRec.Body.Bytes(), &membersResp))

				members, _ := membersResp["Members"].([]any)
				require.Len(t, members, 1)

				m, _ := members[0].(map[string]any)
				assert.Equal(t, "Invited", m["MemberStatus"])
			}
		})
	}
}

// TestParity_UpdateAutomationRuleV2_ActionsApplied verifies that updating a
// V2 automation rule's Actions field actually applies the new actions. JSON
// bodies decode array elements as []any (not []map[string]any); a direct
// type assertion to []map[string]any silently drops the update.
func TestParity_UpdateAutomationRuleV2_ActionsApplied(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/automationrulesv2/create", map[string]any{
		"RuleName":   "my-rule",
		"RuleStatus": "ENABLED",
		"Criteria":   map[string]any{},
		"Actions":    []any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	identifier, _ := createResp["Identifier"].(string)
	require.NotEmpty(t, identifier)

	updateRec := doRequest(t, h, http.MethodPatch, "/automationrulesv2/"+identifier, map[string]any{
		"Actions": []any{
			map[string]any{
				"Type": "FINDING_FIELDS_UPDATE",
				"FindingFieldsUpdate": map[string]any{
					"Comment": "updated by rule",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))

	actions, _ := updateResp["Actions"].([]any)
	require.Len(t, actions, 1, "Actions update must be applied, not silently dropped")

	action, _ := actions[0].(map[string]any)
	assert.Equal(t, "FINDING_FIELDS_UPDATE", action["Type"])
}
