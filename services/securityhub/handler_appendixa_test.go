package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Members tests ---

func TestAppendixA_Members(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "CreateMembers returns empty unprocessed on success",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "111111111111", "Email": "a@example.com"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, []any{}, resp["UnprocessedAccounts"])
					},
				},
			},
		},
		{
			name: "CreateDeleteGetMembers full cycle",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "222222222222", "Email": "b@example.com"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"AccountIds": []any{"222222222222"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						require.Len(t, members, 1)
						m, _ := members[0].(map[string]any)
						assert.Equal(t, "222222222222", m["AccountId"])
					},
				},
				{
					name:   "delete",
					method: http.MethodPost,
					path:   "/members/delete",
					body:   map[string]any{"AccountIds": []any{"222222222222"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after delete returns unprocessed",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"AccountIds": []any{"222222222222"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						assert.Empty(t, members)
						unprocessed, _ := resp["UnprocessedAccounts"].([]any)
						assert.Len(t, unprocessed, 1)
					},
				},
			},
		},
		{
			name: "InviteListMembers cycle",
			steps: []step{
				{
					name:   "create first",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "333333333333", "Email": "c@example.com"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"333333333333"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/members",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						assert.NotEmpty(t, members)
					},
				},
			},
		},
		{
			name: "DisassociateMembers sets status Removed",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "444444444444", "Email": "d@example.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "disassociate",
					method: http.MethodPost,
					path:   "/members/disassociate",
					body:   map[string]any{"AccountIds": []any{"444444444444"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get shows Removed status",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"AccountIds": []any{"444444444444"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						require.Len(t, members, 1)
						m, _ := members[0].(map[string]any)
						assert.Equal(t, "Removed", m["MemberStatus"])
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Invitations / Admin tests ---

func TestAppendixA_Invitations(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "AcceptAdministratorInvitation and GetAdministratorAccount",
			steps: []step{
				{
					name:   "accept",
					method: http.MethodPost,
					path:   "/administrator",
					body:   map[string]any{"AdministratorId": "999999999999", "InvitationId": "inv-001"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get admin account",
					method: http.MethodGet,
					path:   "/administrator",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						admin, _ := resp["Administrator"].(map[string]any)
						assert.Equal(t, "999999999999", admin["AccountId"])
						assert.Equal(t, "inv-001", admin["InvitationId"])
					},
				},
				{
					name:   "disassociate from admin",
					method: http.MethodPost,
					path:   "/administrator/disassociate",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after disassociate returns nil",
					method: http.MethodGet,
					path:   "/administrator",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Nil(t, resp["Administrator"])
					},
				},
			},
		},
		{
			name: "AcceptInvitation and GetMasterAccount",
			steps: []step{
				{
					name:   "accept via master endpoint",
					method: http.MethodPost,
					path:   "/master",
					body:   map[string]any{"MasterId": "888888888888", "InvitationId": "inv-002"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get master account",
					method: http.MethodGet,
					path:   "/master",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						master, _ := resp["Master"].(map[string]any)
						assert.Equal(t, "888888888888", master["AccountId"])
					},
				},
				{
					name:   "disassociate from master",
					method: http.MethodPost,
					path:   "/master/disassociate",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
		{
			name: "GetInvitationsCount and ListInvitations",
			steps: []step{
				{
					name:   "create members to invite",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "555555555555", "Email": "e@example.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite members",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"555555555555"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get invitations count",
					method: http.MethodGet,
					path:   "/invitations/count",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						count, _ := resp["InvitationsCount"].(float64)
						assert.Greater(t, int(count), 0)
					},
				},
				{
					name:   "list invitations",
					method: http.MethodGet,
					path:   "/invitations",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						invs, _ := resp["Invitations"].([]any)
						assert.NotEmpty(t, invs)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Organization tests ---

func TestAppendixA_Organization(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "DescribeOrganizationConfiguration returns defaults",
			steps: []step{
				{
					name:   "describe",
					method: http.MethodGet,
					path:   "/organization/configuration",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["AutoEnableStandards"])
					},
				},
			},
		},
		{
			name: "UpdateOrganizationConfiguration and read back",
			steps: []step{
				{
					name:   "update",
					method: http.MethodPost,
					path:   "/organization/configuration",
					body: map[string]any{
						"AutoEnable":          true,
						"AutoEnableStandards": "DEFAULT",
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "describe updated",
					method: http.MethodGet,
					path:   "/organization/configuration",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, true, resp["AutoEnable"])
						assert.Equal(t, "DEFAULT", resp["AutoEnableStandards"])
					},
				},
			},
		},
		{
			name: "EnableDisableOrganizationAdminAccount and ListOrganizationAdminAccounts",
			steps: []step{
				{
					name:   "enable admin",
					method: http.MethodPost,
					path:   "/organization/admin/enable",
					body:   map[string]any{"AdminAccountId": "777777777777"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "list admin accounts",
					method: http.MethodGet,
					path:   "/organization/admin",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						accounts, _ := resp["AdminAccounts"].([]any)
						assert.Len(t, accounts, 1)
						a, _ := accounts[0].(map[string]any)
						assert.Equal(t, "777777777777", a["AccountId"])
						assert.Equal(t, "ENABLED", a["Status"])
					},
				},
				{
					name:   "disable admin",
					method: http.MethodPost,
					path:   "/organization/admin/disable",
					body:   map[string]any{"AdminAccountId": "777777777777"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "list after disable is empty",
					method: http.MethodGet,
					path:   "/organization/admin",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						accounts, _ := resp["AdminAccounts"].([]any)
						assert.Empty(t, accounts)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Finding Aggregator tests ---

func TestAppendixA_FindingAggregator(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string // returns resource id
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete FindingAggregator",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/findingAggregator/create",
					body: map[string]any{
						"RegionLinkingMode": "SPECIFIED_REGIONS",
						"Regions":           []any{"us-west-2"},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						arn, _ := resp["FindingAggregatorArn"].(string)
						assert.NotEmpty(t, arn)
						assert.Equal(t, "SPECIFIED_REGIONS", resp["RegionLinkingMode"])

						return arn
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/findingAggregator/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						aggs, _ := resp["FindingAggregators"].([]any)
						assert.Len(t, aggs, 1)

						return ""
					},
				},
				{
					name:   "update",
					method: http.MethodPatch,
					path:   "/findingAggregator/update",
					body: map[string]any{
						"FindingAggregatorArn": "placeholder",
						"RegionLinkingMode":    "ALL_REGIONS",
						"Regions":              []any{},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						// Placeholder ARN won't be found
						if code == http.StatusOK {
							assert.Equal(t, "ALL_REGIONS", resp["RegionLinkingMode"])
						}

						return ""
					},
				},
			},
		},
		{
			name: "Delete non-existent FindingAggregator returns 404",
			steps: []step{
				{
					name:   "delete missing",
					method: http.MethodDelete,
					path:   "/findingAggregator/delete/arn:aws:securityhub:us-east-1:000000000000:finding-aggregator/missing",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Configuration Policy tests ---

func TestAppendixA_ConfigurationPolicy(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete ConfigurationPolicy",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/configurationPolicy/create",
					body: map[string]any{
						"Name":        "TestPolicy",
						"Description": "A test policy",
						"ConfigurationPolicy": map[string]any{
							"SecurityHub": map[string]any{"ServiceEnabled": true},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						id, _ := resp["Id"].(string)
						assert.NotEmpty(t, id)
						assert.Equal(t, "TestPolicy", resp["Name"])

						return id
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/configurationPolicy/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						policies, _ := resp["ConfigurationPolicySummaryList"].([]any)
						assert.Len(t, policies, 1)

						return ""
					},
				},
			},
		},
		{
			name: "ConfigurationPolicy association lifecycle",
			steps: []step{
				{
					name:   "create policy",
					method: http.MethodPost,
					path:   "/configurationPolicy/create",
					body: map[string]any{
						"Name":        "AssocPolicy",
						"Description": "for assoc test",
						"ConfigurationPolicy": map[string]any{
							"SecurityHub": map[string]any{"ServiceEnabled": true},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return resp["Id"].(string)
					},
				},
				{
					name:   "associate",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/associate",
					body: map[string]any{
						"ConfigurationPolicyIdentifier": "policy-1",
						"Target": map[string]any{
							"AccountId": "123456789012",
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "get association",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/get",
					body: map[string]any{
						"Target": map[string]any{
							"AccountId": "123456789012",
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "123456789012", resp["TargetId"])
						assert.Equal(t, "SUCCESS", resp["AssociationStatus"])

						return ""
					},
				},
				{
					name:   "list associations",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assocs, _ := resp["ConfigurationPolicyAssociationSummaryList"].([]any)
						assert.Len(t, assocs, 1)

						return ""
					},
				},
				{
					name:   "batch get",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/batchget",
					body: map[string]any{
						"ConfigurationPolicyAssociationIdentifiers": []any{
							map[string]any{"TargetId": "123456789012"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						found, _ := resp["ConfigurationPolicyAssociations"].([]any)
						assert.Len(t, found, 1)

						return ""
					},
				},
				{
					name:   "disassociate",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/disassociate",
					body: map[string]any{
						"ConfigurationPolicyIdentifier": "policy-1",
						"Target": map[string]any{
							"AccountId": "123456789012",
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "list after disassociate is empty",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assocs, _ := resp["ConfigurationPolicyAssociationSummaryList"].([]any)
						assert.Empty(t, assocs)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Hub V2 tests ---

func TestAppendixA_HubV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "EnableDescribeDisableSecurityHubV2",
			steps: []step{
				{
					name:   "describe before enable returns 400",
					method: http.MethodGet,
					path:   "/hubv2",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusBadRequest, code)
					},
				},
				{
					name:   "enable",
					method: http.MethodPost,
					path:   "/hubv2",
					body:   map[string]any{},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "enable again returns conflict",
					method: http.MethodPost,
					path:   "/hubv2",
					body:   map[string]any{},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusConflict, code)
					},
				},
				{
					name:   "describe returns hub",
					method: http.MethodGet,
					path:   "/hubv2",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotEmpty(t, resp["HubV2Arn"])
						assert.Contains(t, resp["HubV2Arn"].(string), "hub-v2/default")
					},
				},
				{
					name:   "disable",
					method: http.MethodDelete,
					path:   "/hubv2",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "describe after disable returns 400",
					method: http.MethodGet,
					path:   "/hubv2",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusBadRequest, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Aggregator V2 tests ---

func TestAppendixA_AggregatorV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete AggregatorV2",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/aggregatorv2/create",
					body: map[string]any{
						"RegionLinkingMode": "ALL_REGIONS",
						"Regions":           []any{},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						arn, _ := resp["AggregatorV2Arn"].(string)
						assert.NotEmpty(t, arn)
						assert.Equal(t, "ALL_REGIONS", resp["RegionLinkingMode"])

						return arn
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/aggregatorv2/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						aggs, _ := resp["Aggregators"].([]any)
						assert.Len(t, aggs, 1)
						a, _ := aggs[0].(map[string]any)

						return a["AggregatorV2Arn"].(string)
					},
				},
			},
		},
		{
			name: "Get non-existent AggregatorV2 returns 404",
			steps: []step{
				{
					name:   "get missing",
					method: http.MethodGet,
					path:   "/aggregatorv2/get/arn:aws:securityhub:us-east-1:000000000000:aggregator-v2/missing",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Automation Rules V2 tests ---

func TestAppendixA_AutomationRulesV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete AutomationRuleV2",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/automationrulesv2/create",
					body: map[string]any{
						"RuleName":   "TestRuleV2",
						"RuleStatus": "ENABLED",
						"RuleOrder":  1.0,
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						id, _ := resp["Identifier"].(string)
						assert.NotEmpty(t, id)
						assert.Equal(t, "TestRuleV2", resp["RuleName"])

						return id
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/automationrulesv2/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						rules, _ := resp["Rules"].([]any)
						assert.Len(t, rules, 1)
						r, _ := rules[0].(map[string]any)

						return r["Identifier"].(string)
					},
				},
				{
					name:   "get",
					method: http.MethodGet,
					path:   "/automationrulesv2/rule-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "TestRuleV2", resp["RuleName"])

						return ""
					},
				},
				{
					name:   "update",
					method: http.MethodPatch,
					path:   "/automationrulesv2/rule-v2-1",
					body: map[string]any{
						"RuleName": "UpdatedRuleV2",
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "UpdatedRuleV2", resp["RuleName"])

						return ""
					},
				},
				{
					name:   "delete",
					method: http.MethodDelete,
					path:   "/automationrulesv2/rule-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "get after delete returns 404",
					method: http.MethodGet,
					path:   "/automationrulesv2/rule-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Connectors V2 tests ---

func TestAppendixA_ConnectorsV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Register Delete ConnectorV2",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/connectorsv2",
					body: map[string]any{
						"Name":        "TestConnector",
						"Description": "A test connector",
						"Provider": map[string]any{
							"Type": "JIRA",
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						id, _ := resp["ConnectorId"].(string)
						assert.NotEmpty(t, id)
						assert.Equal(t, "TestConnector", resp["Name"])
						assert.Equal(t, "ACTIVE", resp["ConnectorStatus"])

						return id
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/connectorsv2",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						connectors, _ := resp["Connectors"].([]any)
						assert.Len(t, connectors, 1)

						return ""
					},
				},
				{
					name:   "get",
					method: http.MethodGet,
					path:   "/connectorsv2/connector-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "TestConnector", resp["Name"])

						return ""
					},
				},
				{
					name:   "update",
					method: http.MethodPatch,
					path:   "/connectorsv2/connector-v2-1",
					body:   map[string]any{"Name": "UpdatedConnector"},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "UpdatedConnector", resp["Name"])

						return ""
					},
				},
				{
					name:   "register",
					method: http.MethodPost,
					path:   "/connectorsv2/register",
					body:   map[string]any{"ConnectorId": "connector-v2-1"},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "REGISTERED", resp["ConnectorStatus"])

						return ""
					},
				},
				{
					name:   "delete",
					method: http.MethodDelete,
					path:   "/connectorsv2/connector-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "get after delete returns 404",
					method: http.MethodGet,
					path:   "/connectorsv2/connector-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Ticket V2 tests ---

func TestAppendixA_TicketV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		body  any
		check func(t *testing.T, code int, resp map[string]any)
	}{
		{
			name: "CreateTicketV2 returns ARN",
			body: map[string]any{
				"TicketConfiguration": map[string]any{
					"TicketDestination": "jira-project",
				},
			},
			check: func(t *testing.T, code int, resp map[string]any) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				arn, _ := resp["TicketConfigurationArn"].(string)
				assert.NotEmpty(t, arn)
				assert.Contains(t, arn, "ticket-v2")
				assert.NotEmpty(t, resp["CreatedAt"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/ticketsv2", tc.body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tc.check(t, rec.Code, resp)
		})
	}
}

// --- Findings V2 tests ---

func TestAppendixA_FindingsV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "GetFindingsV2 returns findings from same store as V1",
			steps: []step{
				{
					name:   "import finding via V1",
					method: http.MethodPost,
					path:   "/findings/import",
					body: map[string]any{
						"Findings": []any{
							map[string]any{
								"AwsAccountId":  "000000000000",
								"CreatedAt":     "2024-01-01T00:00:00Z",
								"Description":   "test",
								"GeneratorId":   "gen1",
								"Id":            "finding-001",
								"ProductArn":    "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
								"SchemaVersion": "2018-10-08",
								"Severity":      map[string]any{"Label": "HIGH"},
								"Title":         "Test Finding",
								"Types":         []any{"Software and Configuration Checks"},
								"UpdatedAt":     "2024-01-01T00:00:00Z",
								"Resources":     []any{map[string]any{"Id": "resource-001", "Type": "AwsEc2Instance"}},
							},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get via V2",
					method: http.MethodPost,
					path:   "/findingsv2",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						findings, _ := resp["Findings"].([]any)
						assert.NotEmpty(t, findings)
					},
				},
			},
		},
		{
			name: "BatchUpdateFindingsV2",
			steps: []step{
				{
					name:   "import finding",
					method: http.MethodPost,
					path:   "/findings/import",
					body: map[string]any{
						"Findings": []any{
							map[string]any{
								"AwsAccountId":  "000000000000",
								"CreatedAt":     "2024-01-01T00:00:00Z",
								"Description":   "test",
								"GeneratorId":   "gen1",
								"Id":            "finding-v2-001",
								"ProductArn":    "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
								"SchemaVersion": "2018-10-08",
								"Severity":      map[string]any{"Label": "HIGH"},
								"Title":         "Test Finding V2",
								"Types":         []any{"Software and Configuration Checks"},
								"UpdatedAt":     "2024-01-01T00:00:00Z",
								"Resources":     []any{},
							},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "batch update V2",
					method: http.MethodPatch,
					path:   "/findingsv2/batchupdatev2",
					body: map[string]any{
						"FindingIdentifiers": []any{
							map[string]any{
								"Id":         "finding-v2-001",
								"ProductArn": "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
							},
						},
						"FindingFieldsUpdate": map[string]any{
							"Note": map[string]any{
								"Text":      "Updated via V2",
								"UpdatedBy": "tester",
							},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
		{
			name: "GetFindingStatisticsV2 returns stats",
			steps: []step{
				{
					name:   "get stats on empty store",
					method: http.MethodPost,
					path:   "/findingsv2/statistics",
					body:   map[string]any{"GroupByAttributes": []any{"Severity.Label"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["FindingStatistics"])
					},
				},
			},
		},
		{
			name: "GetFindingsTrendsV2",
			steps: []step{
				{
					name:   "get trends",
					method: http.MethodPost,
					path:   "/findingsTrendsv2",
					body: map[string]any{
						"GroupByAttribute": "Severity.Label",
						"StartTime":        "2024-01-01T00:00:00Z",
						"EndTime":          "2024-12-31T23:59:59Z",
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["FindingsTrends"])
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Resources V2 tests ---

func TestAppendixA_ResourcesV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "GetResourcesV2 GetResourcesStatisticsV2 GetResourcesTrendsV2",
			steps: []step{
				{
					name:   "get resources empty",
					method: http.MethodPost,
					path:   "/resourcesv2",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						resources, _ := resp["Resources"].([]any)
						assert.Empty(t, resources)
					},
				},
				{
					name:   "get statistics",
					method: http.MethodPost,
					path:   "/resourcesv2/statistics",
					body:   map[string]any{"GroupByAttributes": []any{"Type"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["ResourceStatistics"])
					},
				},
				{
					name:   "get trends",
					method: http.MethodPost,
					path:   "/resourcesTrendsv2",
					body: map[string]any{
						"GroupByAttribute": "Type",
						"StartTime":        "2024-01-01T00:00:00Z",
						"EndTime":          "2024-12-31T23:59:59Z",
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["ResourcesTrends"])
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// --- Products V2 tests ---

func TestAppendixA_DescribeProductsV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		check func(t *testing.T, code int, resp map[string]any)
	}{
		{
			name: "DescribeProductsV2 returns products list",
			check: func(t *testing.T, code int, resp map[string]any) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				products, _ := resp["Products"].([]any)
				assert.NotNil(t, products)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/productsV2", nil)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tc.check(t, rec.Code, resp)
		})
	}
}

// --- Recommended Policy V2 tests ---

func TestAppendixA_RecommendedPolicyV2(t *testing.T) {
	t.Parallel()

	type step struct {
		name   string
		method string
		path   string
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "GenerateAndGetRecommendedPolicyV2",
			steps: []step{
				{
					name:   "generate",
					method: http.MethodPost,
					path:   "/recommendedPolicyV2/metadata-uid-001",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "metadata-uid-001", resp["MetadataUid"])
						assert.NotEmpty(t, resp["Policy"])
						assert.NotEmpty(t, resp["GenerationTime"])
					},
				},
				{
					name:   "get",
					method: http.MethodGet,
					path:   "/recommendedPolicyV2/metadata-uid-001",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "metadata-uid-001", resp["MetadataUid"])
					},
				},
				{
					name:   "get non-existent returns 404",
					method: http.MethodGet,
					path:   "/recommendedPolicyV2/does-not-exist",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}
