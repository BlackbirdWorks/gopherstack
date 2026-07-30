package route53resolver_test

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestFirewallRule_BlockOverrideFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     func(groupID, dlID string) map[string]any
		checkFn  func(t *testing.T, rule map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "block_nodata",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             100,
					"Action":               "BLOCK",
					"BlockResponse":        "NODATA",
					"Name":                 "block-nodata",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "BLOCK", rule["Action"])
				assert.Equal(t, "NODATA", rule["BlockResponse"])
			},
		},
		{
			name: "block_override_with_domain",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             200,
					"Action":               "BLOCK",
					"BlockResponse":        "OVERRIDE",
					"BlockOverrideDomain":  "safe.example.com",
					"BlockOverrideDnsType": "CNAME",
					"BlockOverrideTtl":     300,
					"Name":                 "block-override",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "OVERRIDE", rule["BlockResponse"])
				assert.Equal(t, "safe.example.com", rule["BlockOverrideDomain"])
				// Wire key is "BlockOverrideDnsType"/"BlockOverrideTtl" -- verified
				// against the real SDK's hand-rolled awsjson1.1 deserializer, which
				// does exact-case map-key matching (see firewallRuleOutput doc comment).
				assert.Equal(t, "CNAME", rule["BlockOverrideDnsType"])
				assert.EqualValues(t, 300, rule["BlockOverrideTtl"])
			},
		},
		{
			name: "block_override_missing_domain_rejected",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             300,
					"Action":               "BLOCK",
					"BlockResponse":        "OVERRIDE",
					"Name":                 "block-override-missing",
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "allow_action",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             400,
					"Action":               "ALLOW",
					"Name":                 "allow-rule",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "ALLOW", rule["Action"])
			},
		},
		{
			name: "with_qtype",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             500,
					"Action":               "BLOCK",
					"BlockResponse":        "NODATA",
					"Qtype":                "A",
					"Name":                 "qtype-rule",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "A", rule["Qtype"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "override-test-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec := doRequest(
				t,
				h,
				"CreateFirewallDomainList",
				map[string]any{"Name": "override-test-dl"},
			)
			require.Equal(t, http.StatusOK, dlRec.Code)
			var dlResp map[string]any
			require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
			dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "CreateFirewallRule", tt.body(groupID, dlID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.checkFn != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rule := resp["FirewallRule"].(map[string]any)
				tt.checkFn(t, rule)
			}
		})
	}
}

// --- Issue 22: UpdateFirewallRule with extended fields ---

// TestUpdateFirewallRule_ExtendedFields verifies UpdateFirewallRule applies
// extended fields. Per the real UpdateFirewallRuleInput (verified against
// api_op_UpdateFirewallRule.go) there is no FirewallRuleId member -- a rule
// is identified by the (FirewallRuleGroupId, FirewallDomainListId) pair it
// was created with, so each case supplies that pair instead of a synthetic ID.
func TestUpdateFirewallRule_ExtendedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  map[string]any
		checkFn func(t *testing.T, rule map[string]any)
		name    string
	}{
		{
			name: "update_action_and_block_response",
			update: map[string]any{
				"Action":        "BLOCK",
				"BlockResponse": "NXDOMAIN",
			},
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "BLOCK", rule["Action"])
				assert.Equal(t, "NXDOMAIN", rule["BlockResponse"])
			},
		},
		{
			name: "update_qtype",
			update: map[string]any{
				"Qtype": "AAAA",
			},
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "AAAA", rule["Qtype"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			// Re-setup: need same group/dl/rule in fresh handler.
			grpRec2 := doRequest(
				t,
				h2,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "upd-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec2.Code)
			var grpR2 map[string]any
			require.NoError(t, json.Unmarshal(grpRec2.Body.Bytes(), &grpR2))
			gID := grpR2["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec2 := doRequest(t, h2, "CreateFirewallDomainList", map[string]any{"Name": "upd-dl"})
			require.Equal(t, http.StatusOK, dlRec2.Code)
			var dlR2 map[string]any
			require.NoError(t, json.Unmarshal(dlRec2.Body.Bytes(), &dlR2))
			dID := dlR2["FirewallDomainList"].(map[string]any)["Id"].(string)

			createRec2 := doRequest(t, h2, "CreateFirewallRule", map[string]any{
				"FirewallRuleGroupId":  gID,
				"FirewallDomainListId": dID,
				"Priority":             100,
				"Action":               "ALLOW",
				"Name":                 "rule-to-upd",
			})
			require.Equal(t, http.StatusOK, createRec2.Code)

			upd := tt.update
			upd["FirewallRuleGroupId"] = gID
			upd["FirewallDomainListId"] = dID
			rec := doRequest(t, h2, "UpdateFirewallRule", upd)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rule := resp["FirewallRule"].(map[string]any)
			tt.checkFn(t, rule)
		})
	}
}

// --- Issue 23: FirewallRuleGroup ShareStatus + timestamps ---

func TestFirewallRule_MetadataFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "meta-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "meta-dl"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	var dlResp map[string]any
	require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
	dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

	createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  groupID,
		"FirewallDomainListId": dlID,
		"Priority":             100,
		"Action":               "ALLOW",
		"Name":                 "meta-rule",
		"CreatorRequestId":     "req-fw-rule-1",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &resp))
	rule := resp["FirewallRule"].(map[string]any)

	assert.Equal(t, "req-fw-rule-1", rule["CreatorRequestId"])
	assert.NotEmpty(t, rule["CreationTime"])
	assert.NotEmpty(t, rule["ModificationTime"])
}

// --- Backend: AssociateFirewallRuleGroup stores CreatorRequestId and timestamps ---

func TestBackend_FirewallRuleBlockOverrideRoundTrip(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	grp, _ := b.CreateFirewallRuleGroup(context.Background(), "grp-bor", "req-bor")
	dl, _ := b.CreateFirewallDomainList(context.Background(), "dl-bor", "req-bor")

	rule, err := b.CreateFirewallRule(context.Background(), route53resolver.CreateFirewallRuleParams{
		FirewallRuleGroupID:  grp.ID,
		FirewallDomainListID: dl.ID,
		Name:                 "rule-bor",
		Action:               "BLOCK",
		BlockResponse:        "OVERRIDE",
		BlockOverrideDomain:  "safe.example.com",
		BlockOverrideDNSType: "CNAME",
		BlockOverrideTTL:     600,
		Priority:             100,
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	rules := b2.ListFirewallRules(context.Background(), grp.ID)
	require.Len(t, rules, 1)
	assert.Equal(t, rule.BlockOverrideDomain, rules[0].BlockOverrideDomain)
	assert.Equal(t, rule.BlockOverrideDNSType, rules[0].BlockOverrideDNSType)
	assert.Equal(t, rule.BlockOverrideTTL, rules[0].BlockOverrideTTL)
}

// TestAudit_FirewallRule_BlockResponseStoredOnCreate ensures blockResponse is actually
// stored when creating a firewall rule (was previously dropped due to `_` param).
func TestFirewallRule_BlockResponseStoredOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		blockResponse string
		wantCode      int
	}{
		{name: "nodata", blockResponse: "NODATA", wantCode: http.StatusOK},
		{name: "nxdomain", blockResponse: "NXDOMAIN", wantCode: http.StatusOK},
		{name: "empty_allow", blockResponse: "", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "br-test-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec := doRequest(
				t,
				h,
				"CreateFirewallDomainList",
				map[string]any{"Name": "br-test-dl"},
			)
			require.Equal(t, http.StatusOK, dlRec.Code)
			var dlResp map[string]any
			require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
			dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

			body := map[string]any{
				"FirewallRuleGroupId":  groupID,
				"FirewallDomainListId": dlID,
				"Priority":             100,
				"Action":               "BLOCK",
				"Name":                 "br-rule",
			}
			if tt.blockResponse != "" {
				body["BlockResponse"] = tt.blockResponse
			}

			rec := doRequest(t, h, "CreateFirewallRule", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.blockResponse != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rule := resp["FirewallRule"].(map[string]any)
				assert.Equal(t, tt.blockResponse, rule["BlockResponse"])
			}
		})
	}
}

// Avoid unused import.
var _ = strings.HasPrefix

func TestCreateFirewallRule_ActionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{name: "allow", action: "ALLOW", wantCode: http.StatusOK},
		{name: "block", action: "BLOCK", wantCode: http.StatusOK},
		{name: "alert", action: "ALERT", wantCode: http.StatusOK},
		{name: "invalid", action: "DENY", wantCode: http.StatusBadRequest},
		{name: "empty", action: "", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// First create a firewall rule group.
			grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "grp-action"})
			require.Equal(t, http.StatusOK, grpRec.Code)

			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			grpID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "CreateFirewallRule", map[string]any{
				"FirewallRuleGroupId": grpID,
				"Name":                "test-rule",
				"Action":              tt.action,
				"Priority":            100,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Firewall rule has no independent Id/Arn ---

// TestCreateFirewallRule_NoIdOrArnInOutput verifies the FirewallRule
// response has no Id or Arn field. Per the real types.FirewallRule (verified
// against aws-sdk-go-v2/service/route53resolver/types/types.go) a firewall
// rule carries no independent identity -- it is addressed by the
// (FirewallRuleGroupId, FirewallDomainListId) pair it was created with. An
// earlier revision of gopherstack invented Id/Arn fields on this response;
// this test used to assert their presence and has been corrected to assert
// their absence instead (see PARITY.md for the fix note).
func TestCreateFirewallRule_NoIdOrArnInOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "id-test-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)

	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	grpID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "id-test-dl"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	var dlResp map[string]any
	require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
	dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

	ruleRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dlID,
		"Name":                 "my-fw-rule",
		"Action":               "ALLOW",
		"Priority":             10,
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	rule, ok := ruleResp["FirewallRule"].(map[string]any)
	require.True(t, ok)
	_, hasID := rule["Id"]
	_, hasArn := rule["Arn"]
	assert.False(t, hasID, "FirewallRule response must not carry an Id field")
	assert.False(t, hasArn, "FirewallRule response must not carry an Arn field")
	assert.Equal(t, grpID, rule["FirewallRuleGroupId"])
	assert.Equal(t, dlID, rule["FirewallDomainListId"])
}

// --- Export count helpers ---

func TestCreateFirewallRule_IncrementsRuleCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "count-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)

	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	grp := grpResp["FirewallRuleGroup"].(map[string]any)
	grpID := grp["Id"].(string)
	assert.InDelta(t, 0, grp["RuleCount"], 0)

	doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId": grpID,
		"Name":                "rule-1",
		"Action":              "ALLOW",
	})
	doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId": grpID,
		"Name":                "rule-2",
		"Action":              "BLOCK",
	})

	assert.Equal(t, 2, route53resolver.FirewallRuleBackendCount(h.Backend.(*route53resolver.InMemoryBackend)))
}

// --- AssociateResolverEndpointIPAddress ---

func TestCreateFirewallRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) (groupID, dlID string)
		body       func(groupID, dlID string) map[string]any
		name       string
		wantAction string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) (string, string) {
				t.Helper()
				grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "grp-rule"})
				require.Equal(t, http.StatusOK, grpRec.Code)
				var grpResp map[string]any
				require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
				groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

				dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "dl-for-rule"})
				require.Equal(t, http.StatusOK, dlRec.Code)
				var dlResp map[string]any
				require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
				dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

				return groupID, dlID
			},
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Name":                 "my-rule",
					"Action":               "ALLOW",
					"Priority":             100,
					"CreatorRequestId":     "req-rule-1",
				}
			},
			wantCode:   http.StatusOK,
			wantAction: "ALLOW",
		},
		{
			name:       "missing_group_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) (string, string) { return "", "" },
			body: func(_, _ string) map[string]any {
				return map[string]any{"Name": "r", "Action": "ALLOW"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) (string, string) { return "", "" },
			body: func(_, _ string) map[string]any {
				return map[string]any{"FirewallRuleGroupId": "rslvr-frg-x", "Action": "ALLOW"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "missing_action",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) (string, string) { return "", "" },
			body: func(_, _ string) map[string]any {
				return map[string]any{"FirewallRuleGroupId": "rslvr-frg-x", "Name": "r"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "group_not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) (string, string) { return "", "" },
			body: func(_, _ string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId": "rslvr-frg-notexist",
					"Name":                "rule",
					"Action":              "BLOCK",
					"Priority":            100,
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			groupID, dlID := tt.setupExtra(t, h)
			rec := doRequest(t, h, "CreateFirewallRule", tt.body(groupID, dlID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rule, ok := resp["FirewallRule"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantAction, rule["Action"])
				assert.Equal(t, groupID, rule["FirewallRuleGroupId"])
				assert.NotEmpty(t, rule["Name"])
			}
		})
	}
}

// --- CreateOutpostResolver ---

// TestR53R_FirewallRuleCRUD covers CreateFirewallRule, ListFirewallRules,
// UpdateFirewallRule, DeleteFirewallRule.
func TestFirewallRuleCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a rule group and domain list.
	rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "rule-test-group",
		"CreatorRequestId": "req-3",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rgResp := decodeJSON(t, rec)
	rg, _ := rgResp["FirewallRuleGroup"].(map[string]any)
	rgID, _ := rg["Id"].(string)

	rec = doRequest(t, h, "CreateFirewallDomainList", map[string]any{
		"Name":             "rule-test-domains",
		"CreatorRequestId": "req-4",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	dlResp := decodeJSON(t, rec)
	dl, _ := dlResp["FirewallDomainList"].(map[string]any)
	dlID, _ := dl["Id"].(string)

	// CreateFirewallRule.
	rec = doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  rgID,
		"FirewallDomainListId": dlID,
		"Priority":             100,
		"Action":               "BLOCK",
		"BlockResponse":        "NODATA",
		"Name":                 "test-rule",
		"CreatorRequestId":     "req-5",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListFirewallRules.
	rec = doRequest(t, h, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": rgID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateFirewallRule. Real AWS addresses a rule by the
	// (FirewallRuleGroupId, FirewallDomainListId) pair it was created with --
	// there is no FirewallRuleId (verified against api_op_UpdateFirewallRule.go).
	rec = doRequest(t, h, "UpdateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  rgID,
		"FirewallDomainListId": dlID,
		"Priority":             200,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteFirewallRule.
	rec = doRequest(t, h, "DeleteFirewallRule", map[string]any{
		"FirewallRuleGroupId":  rgID,
		"FirewallDomainListId": dlID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestFirewallRule_DnsThreatProtection covers the DNS Firewall Advanced
// DnsThreatProtection match source added this pass (gopherstack-3sgl):
// mutual exclusivity with FirewallDomainListId, the closed
// DGA/DNS_TUNNELING/DICTIONARY_DGA enum, and identifying/updating/deleting a
// DnsThreatProtection rule by its system-generated FirewallThreatProtectionId
// instead of a domain list (verified against CreateFirewallRuleInput's doc
// comment and api_op_{Update,Delete}FirewallRule.go in
// aws-sdk-go-v2/service/route53resolver@v1.48.0).
func TestFirewallRule_DnsThreatProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "dga",
			body: map[string]any{
				"Name":                "dga-rule",
				"Action":              "ALERT",
				"DnsThreatProtection": "DGA",
				"ConfidenceThreshold": "MEDIUM",
				"Priority":            100,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "dns_tunneling",
			body: map[string]any{
				"Name":                "tunneling-rule",
				"Action":              "ALERT",
				"DnsThreatProtection": "DNS_TUNNELING",
				"ConfidenceThreshold": "HIGH",
				"Priority":            100,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "dictionary_dga",
			body: map[string]any{
				"Name":                "dict-dga-rule",
				"Action":              "ALERT",
				"DnsThreatProtection": "DICTIONARY_DGA",
				"ConfidenceThreshold": "LOW",
				"Priority":            100,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_value_rejected",
			body: map[string]any{
				"Name":                "bad-value-rule",
				"Action":              "ALERT",
				"DnsThreatProtection": "NOT_A_REAL_DETECTOR",
				"ConfidenceThreshold": "MEDIUM",
				"Priority":            100,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// CreateFirewallRuleInput's doc comment: "You must provide this
			// value when you create a DNS Firewall Advanced rule" -- omitting
			// ConfidenceThreshold on a DnsThreatProtection rule must be
			// rejected, not silently accepted (this was previously an
			// unenforced required field).
			name: "missing_confidence_threshold_rejected",
			body: map[string]any{
				"Name":                "no-confidence-rule",
				"Action":              "ALERT",
				"DnsThreatProtection": "DGA",
				"Priority":            100,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_confidence_threshold_rejected",
			body: map[string]any{
				"Name":                "bad-confidence-rule",
				"Action":              "ALERT",
				"DnsThreatProtection": "DGA",
				"ConfidenceThreshold": "EXTREME",
				"Priority":            100,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "dtp-grp-" + tt.name})
			require.Equal(t, http.StatusOK, grpRec.Code)
			grpResp := decodeJSON(t, grpRec)
			grpID, _ := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			body := map[string]any{"FirewallRuleGroupId": grpID}
			maps.Copy(body, tt.body)

			rec := doRequest(t, h, "CreateFirewallRule", body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			resp := decodeJSON(t, rec)
			rule, ok := resp["FirewallRule"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.body["DnsThreatProtection"], rule["DnsThreatProtection"])
			assert.Equal(t, tt.body["ConfidenceThreshold"], rule["ConfidenceThreshold"])
			assert.NotEmpty(t, rule["FirewallThreatProtectionId"], "rule must get a FirewallThreatProtectionId")
			_, hasDomainList := rule["FirewallDomainListId"]
			assert.False(t, hasDomainList, "a DnsThreatProtection rule must not carry FirewallDomainListId")
		})
	}
}

// TestFirewallRule_DnsThreatProtectionMutuallyExclusiveWithDomainList
// verifies CreateFirewallRule rejects a request supplying both
// FirewallDomainListId and DnsThreatProtection, per the real API's
// documented "they are mutually exclusive" contract.
func TestFirewallRule_DnsThreatProtectionMutuallyExclusiveWithDomainList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "dtp-exclusive-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	grpResp := decodeJSON(t, grpRec)
	grpID, _ := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "dtp-exclusive-dl"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	dlResp := decodeJSON(t, dlRec)
	dlID, _ := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

	rec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dlID,
		"DnsThreatProtection":  "DGA",
		"Name":                 "conflicting-rule",
		"Action":               "ALERT",
		"Priority":             100,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestFirewallRule_UpdateDeleteByThreatProtectionId verifies
// Update/DeleteFirewallRule can identify a DnsThreatProtection rule by its
// FirewallThreatProtectionId (the DNS Firewall Advanced counterpart to
// FirewallDomainListId, since a DnsThreatProtection rule has no domain
// list) -- verified against api_op_{Update,Delete}FirewallRule.go.
func TestFirewallRule_UpdateDeleteByThreatProtectionId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "dtp-update-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	grpResp := decodeJSON(t, grpRec)
	grpID, _ := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId": grpID,
		"DnsThreatProtection": "DGA",
		"ConfidenceThreshold": "MEDIUM",
		"Name":                "dtp-update-rule",
		"Action":              "ALERT",
		"Priority":            100,
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := decodeJSON(t, createRec)
	threatProtectionID, _ := createResp["FirewallRule"].(map[string]any)["FirewallThreatProtectionId"].(string)
	require.NotEmpty(t, threatProtectionID)

	// An invalid ConfidenceThreshold must be rejected on update too (not
	// required-on-update like on create, but still validated against its
	// closed enum whenever supplied).
	badUpdateRec := doRequest(t, h, "UpdateFirewallRule", map[string]any{
		"FirewallRuleGroupId":        grpID,
		"FirewallThreatProtectionId": threatProtectionID,
		"ConfidenceThreshold":        "EXTREME",
	})
	assert.Equal(t, http.StatusBadRequest, badUpdateRec.Code)

	updateRec := doRequest(t, h, "UpdateFirewallRule", map[string]any{
		"FirewallRuleGroupId":        grpID,
		"FirewallThreatProtectionId": threatProtectionID,
		"Action":                     "BLOCK",
		"BlockResponse":              "NODATA",
		"ConfidenceThreshold":        "HIGH",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateResp := decodeJSON(t, updateRec)
	assert.Equal(t, "BLOCK", updateResp["FirewallRule"].(map[string]any)["Action"])
	assert.Equal(t, "HIGH", updateResp["FirewallRule"].(map[string]any)["ConfidenceThreshold"])

	deleteRec := doRequest(t, h, "DeleteFirewallRule", map[string]any{
		"FirewallRuleGroupId":        grpID,
		"FirewallThreatProtectionId": threatProtectionID,
	})
	assert.Equal(t, http.StatusOK, deleteRec.Code)

	// A second delete must now fail: the rule no longer exists.
	deleteAgainRec := doRequest(t, h, "DeleteFirewallRule", map[string]any{
		"FirewallRuleGroupId":        grpID,
		"FirewallThreatProtectionId": threatProtectionID,
	})
	assert.Equal(t, http.StatusNotFound, deleteAgainRec.Code)
}

// TestFirewallRule_DomainRedirectionAction covers
// FirewallDomainRedirectionAction (gopherstack-3sgl): defaults to the real
// API's documented INSPECT_REDIRECTION_DOMAIN for a domain-list rule when
// omitted, echoes an explicit TRUST_REDIRECTION_DOMAIN, and rejects an
// unrecognized value.
func TestFirewallRule_DomainRedirectionAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		redirectionAction string
		name              string
		wantValue         string
		wantStatus        int
	}{
		{
			name:              "defaults_when_omitted",
			redirectionAction: "",
			wantValue:         "INSPECT_REDIRECTION_DOMAIN",
			wantStatus:        http.StatusOK,
		},
		{
			name:              "explicit_inspect",
			redirectionAction: "INSPECT_REDIRECTION_DOMAIN",
			wantValue:         "INSPECT_REDIRECTION_DOMAIN",
			wantStatus:        http.StatusOK,
		},
		{
			name:              "explicit_trust",
			redirectionAction: "TRUST_REDIRECTION_DOMAIN",
			wantValue:         "TRUST_REDIRECTION_DOMAIN",
			wantStatus:        http.StatusOK,
		},
		{
			name:              "invalid_rejected",
			redirectionAction: "SOMETHING_ELSE",
			wantStatus:        http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "redir-grp-" + tt.name})
			require.Equal(t, http.StatusOK, grpRec.Code)
			grpResp := decodeJSON(t, grpRec)
			grpID, _ := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "redir-dl-" + tt.name})
			require.Equal(t, http.StatusOK, dlRec.Code)
			dlResp := decodeJSON(t, dlRec)
			dlID, _ := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

			body := map[string]any{
				"FirewallRuleGroupId":  grpID,
				"FirewallDomainListId": dlID,
				"Name":                 "redir-rule",
				"Action":               "ALERT",
				"Priority":             100,
			}
			if tt.redirectionAction != "" {
				body["FirewallDomainRedirectionAction"] = tt.redirectionAction
			}

			rec := doRequest(t, h, "CreateFirewallRule", body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			resp := decodeJSON(t, rec)
			rule, ok := resp["FirewallRule"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantValue, rule["FirewallDomainRedirectionAction"])
		})
	}
}

// TestListFirewallRules_ActionAndPriorityFilters verifies the optional
// Action/Priority filters on ListFirewallRules (verified against
// api_op_ListFirewallRules.go).
func TestListFirewallRules_ActionAndPriorityFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "filter-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	grpResp := decodeJSON(t, grpRec)
	grpID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dl1Rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "filter-dl-1"})
	require.Equal(t, http.StatusOK, dl1Rec.Code)
	dl1ID := decodeJSON(t, dl1Rec)["FirewallDomainList"].(map[string]any)["Id"].(string)

	dl2Rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "filter-dl-2"})
	require.Equal(t, http.StatusOK, dl2Rec.Code)
	dl2ID := decodeJSON(t, dl2Rec)["FirewallDomainList"].(map[string]any)["Id"].(string)

	rec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dl1ID,
		"Name":                 "allow-rule",
		"Action":               "ALLOW",
		"Priority":             100,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dl2ID,
		"Name":                 "block-rule",
		"Action":               "BLOCK",
		"Priority":             200,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body    map[string]any
		name    string
		wantLen int
	}{
		{
			name:    "no_filter_returns_both",
			body:    map[string]any{"FirewallRuleGroupId": grpID},
			wantLen: 2,
		},
		{
			name:    "action_filter",
			body:    map[string]any{"FirewallRuleGroupId": grpID, "Action": "BLOCK"},
			wantLen: 1,
		},
		{
			name:    "priority_filter",
			body:    map[string]any{"FirewallRuleGroupId": grpID, "Priority": float64(100)},
			wantLen: 1,
		},
		{
			name:    "action_and_priority_no_match",
			body:    map[string]any{"FirewallRuleGroupId": grpID, "Action": "ALLOW", "Priority": float64(200)},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			listRec := doRequest(t, h, "ListFirewallRules", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := decodeJSON(t, listRec)
			items, _ := resp["FirewallRules"].([]any)
			assert.Len(t, items, tt.wantLen)
		})
	}
}

// createTestFirewallDomainList creates a domain list and returns its Id, for
// use as setup fixture data by the Batch{Create,Update,Delete}FirewallRule
// table tests below.
func createTestFirewallDomainList(t *testing.T, h *route53resolver.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)

	return decodeJSON(t, rec)["FirewallDomainList"].(map[string]any)["Id"].(string)
}

// TestBatchCreateFirewallRule covers BatchCreateFirewallRule: multi-entry
// success, the envelope-required-field check, and -- critically -- the real
// API's partial-success atomicity (verified against
// api_op_BatchCreateFirewallRule.go and AWS's own published example response:
// BatchCreateFirewallRuleOutput carries both CreatedFirewallRules and
// CreateErrors from a single 200, there is no all-or-nothing rejection). A
// batch containing one invalid entry must still create the valid entries
// (proven below by both the response body's CreatedFirewallRules count and by
// a follow-up ListFirewallRules call against the SAME backend showing the
// successful rule actually landed in the shared FirewallRule store -- the
// same store CreateFirewallRule itself writes to).
func TestBatchCreateFirewallRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildEntries   func(t *testing.T, h *route53resolver.Handler, grpID string) []map[string]any
		name           string
		wantErrorCode  string
		wantTopCode    int
		wantCreatedLen int
		wantErrorLen   int
	}{
		{
			name: "all_valid_entries_may_span_different_groups",
			buildEntries: func(t *testing.T, h *route53resolver.Handler, grpID string) []map[string]any {
				t.Helper()
				dl1 := createTestFirewallDomainList(t, h, "bc-dl-1")
				dl2 := createTestFirewallDomainList(t, h, "bc-dl-2")

				return []map[string]any{
					{
						"FirewallRuleGroupId": grpID, "FirewallDomainListId": dl1,
						"Name": "bc-r1", "Action": "ALLOW", "Priority": 100,
					},
					{
						"FirewallRuleGroupId": grpID, "FirewallDomainListId": dl2,
						"Name": "bc-r2", "Action": "BLOCK", "BlockResponse": "NODATA", "Priority": 200,
					},
				}
			},
			wantTopCode:    http.StatusOK,
			wantCreatedLen: 2,
			wantErrorLen:   0,
		},
		{
			name: "partial_failure_does_not_block_or_roll_back_valid_entries",
			buildEntries: func(t *testing.T, h *route53resolver.Handler, grpID string) []map[string]any {
				t.Helper()
				dl1 := createTestFirewallDomainList(t, h, "bc-partial-dl-1")

				return []map[string]any{
					{
						"FirewallRuleGroupId": grpID, "FirewallDomainListId": dl1,
						"Name": "bc-partial-good", "Action": "ALLOW", "Priority": 100,
					},
					// Invalid Action -- fails handleCreateFirewallRule's Action
					// validation, exactly as it would for a standalone
					// CreateFirewallRule call.
					{"FirewallRuleGroupId": grpID, "Name": "bc-partial-bad", "Action": "DENY", "Priority": 200},
				}
			},
			wantTopCode:    http.StatusOK,
			wantCreatedLen: 1,
			wantErrorLen:   1,
			wantErrorCode:  "InvalidRequestException",
		},
		{
			name: "group_not_found_reported_per_entry_not_top_level",
			buildEntries: func(_ *testing.T, _ *route53resolver.Handler, _ string) []map[string]any {
				return []map[string]any{
					{
						"FirewallRuleGroupId": "rslvr-frg-doesnotexist",
						"Name":                "bc-nogroup", "Action": "ALLOW", "Priority": 100,
					},
				}
			},
			wantTopCode:    http.StatusOK,
			wantCreatedLen: 0,
			wantErrorLen:   1,
			wantErrorCode:  "ResourceNotFoundException",
		},
		{
			name: "missing_entries_rejected_as_ValidationException",
			buildEntries: func(_ *testing.T, _ *route53resolver.Handler, _ string) []map[string]any {
				return nil
			},
			wantTopCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "batch-create-grp"})
			require.Equal(t, http.StatusOK, grpRec.Code)
			grpID := decodeJSON(t, grpRec)["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			entries := tt.buildEntries(t, h, grpID)
			rec := doRequest(t, h, "BatchCreateFirewallRule", map[string]any{"CreateFirewallRuleEntries": entries})
			require.Equal(t, tt.wantTopCode, rec.Code)
			if tt.wantTopCode != http.StatusOK {
				return
			}

			resp := decodeJSON(t, rec)
			created, _ := resp["CreatedFirewallRules"].([]any)
			errs, _ := resp["CreateErrors"].([]any)
			assert.Len(t, created, tt.wantCreatedLen)
			assert.Len(t, errs, tt.wantErrorLen)
			if tt.wantErrorLen > 0 {
				errItem, ok := errs[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantErrorCode, errItem["Code"])
				assert.NotEmpty(t, errItem["Message"])
				assert.NotNil(t, errItem["FirewallRule"])
			}

			// Shared-state proof: the entries that succeeded are visible via
			// ListFirewallRules against the very same handler/backend -- i.e.
			// BatchCreateFirewallRule wrote through the same FirewallRule
			// store as CreateFirewallRule, not an isolated batch-only store.
			listRec := doRequest(t, h, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": grpID})
			require.Equal(t, http.StatusOK, listRec.Code)
			listed, _ := decodeJSON(t, listRec)["FirewallRules"].([]any)
			assert.Len(t, listed, tt.wantCreatedLen)
		})
	}
}

// TestBatchUpdateFirewallRule covers BatchUpdateFirewallRule: successful
// multi-entry update and partial-failure semantics (same atomicity model as
// BatchCreateFirewallRule -- see its doc comment / test above).
func TestBatchUpdateFirewallRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildEntries   func(t *testing.T, h *route53resolver.Handler, grpID, dlID string) []map[string]any
		name           string
		wantErrorCode  string
		wantTopCode    int
		wantUpdatedLen int
		wantErrorLen   int
	}{
		{
			name: "valid_update_applies_through_shared_store",
			buildEntries: func(_ *testing.T, _ *route53resolver.Handler, grpID, dlID string) []map[string]any {
				return []map[string]any{
					{
						"FirewallRuleGroupId": grpID, "FirewallDomainListId": dlID,
						"Priority": 999, "Action": "BLOCK", "BlockResponse": "NXDOMAIN",
					},
				}
			},
			wantTopCode:    http.StatusOK,
			wantUpdatedLen: 1,
			wantErrorLen:   0,
		},
		{
			name: "partial_failure_missing_domain_list_id_does_not_block_valid_entry",
			buildEntries: func(_ *testing.T, _ *route53resolver.Handler, grpID, dlID string) []map[string]any {
				return []map[string]any{
					{"FirewallRuleGroupId": grpID, "FirewallDomainListId": dlID, "Priority": 555},
					// Missing FirewallDomainListId -- fails handleUpdateFirewallRule's
					// required-field check, same as a standalone call would.
					{"FirewallRuleGroupId": grpID, "Priority": 100},
				}
			},
			wantTopCode:    http.StatusOK,
			wantUpdatedLen: 1,
			wantErrorLen:   1,
			wantErrorCode:  "InvalidRequestException",
		},
		{
			name: "missing_entries_rejected_as_ValidationException",
			buildEntries: func(_ *testing.T, _ *route53resolver.Handler, _, _ string) []map[string]any {
				return nil
			},
			wantTopCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "batch-update-grp"})
			require.Equal(t, http.StatusOK, grpRec.Code)
			grpID := decodeJSON(t, grpRec)["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlID := createTestFirewallDomainList(t, h, "batch-update-dl")

			createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
				"FirewallRuleGroupId": grpID, "FirewallDomainListId": dlID,
				"Name": "bu-rule", "Action": "ALLOW", "Priority": 100,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			entries := tt.buildEntries(t, h, grpID, dlID)
			rec := doRequest(t, h, "BatchUpdateFirewallRule", map[string]any{"UpdateFirewallRuleEntries": entries})
			require.Equal(t, tt.wantTopCode, rec.Code)
			if tt.wantTopCode != http.StatusOK {
				return
			}

			resp := decodeJSON(t, rec)
			updated, _ := resp["UpdatedFirewallRules"].([]any)
			errs, _ := resp["UpdateErrors"].([]any)
			assert.Len(t, updated, tt.wantUpdatedLen)
			assert.Len(t, errs, tt.wantErrorLen)
			if tt.wantErrorLen > 0 {
				errItem, ok := errs[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantErrorCode, errItem["Code"])
			}

			// Shared-state proof: read the rule back through ListFirewallRules
			// (the same read path GetFirewallRule-equivalent callers use) to
			// confirm the batch update actually mutated the one rule that
			// exists in the shared store.
			listRec := doRequest(t, h, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": grpID})
			require.Equal(t, http.StatusOK, listRec.Code)
			listed, _ := decodeJSON(t, listRec)["FirewallRules"].([]any)
			require.Len(t, listed, 1)
			if tt.wantUpdatedLen > 0 {
				rule, ok := listed[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, updated[0].(map[string]any)["Priority"], rule["Priority"])
			}
		})
	}
}

// TestBatchDeleteFirewallRule covers BatchDeleteFirewallRule: successful
// deletion and partial-failure semantics (same atomicity model as
// BatchCreateFirewallRule -- see its doc comment / test above). Proves that a
// batch with one not-found entry still deletes the entry that does exist,
// and that the deletion is visible via ListFirewallRules against the shared
// store.
func TestBatchDeleteFirewallRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantErrorCode  string
		wantTopCode    int
		wantDeletedLen int
		wantErrorLen   int
		deleteMissing  bool
	}{
		{
			name:           "valid_delete_removes_from_shared_store",
			wantTopCode:    http.StatusOK,
			wantDeletedLen: 1,
			wantErrorLen:   0,
		},
		{
			name:           "partial_failure_not_found_entry_does_not_block_valid_delete",
			wantTopCode:    http.StatusOK,
			wantDeletedLen: 1,
			wantErrorLen:   1,
			wantErrorCode:  "ResourceNotFoundException",
			deleteMissing:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "batch-delete-grp"})
			require.Equal(t, http.StatusOK, grpRec.Code)
			grpID := decodeJSON(t, grpRec)["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlID := createTestFirewallDomainList(t, h, "batch-delete-dl")

			createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
				"FirewallRuleGroupId": grpID, "FirewallDomainListId": dlID,
				"Name": "bd-rule", "Action": "ALLOW", "Priority": 100,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			entries := []map[string]any{
				{"FirewallRuleGroupId": grpID, "FirewallDomainListId": dlID},
			}
			if tt.deleteMissing {
				extraDL := createTestFirewallDomainList(t, h, "batch-delete-missing-dl")
				entries = append(entries, map[string]any{
					"FirewallRuleGroupId": grpID, "FirewallDomainListId": extraDL,
				})
			}

			rec := doRequest(t, h, "BatchDeleteFirewallRule", map[string]any{"DeleteFirewallRuleEntries": entries})
			require.Equal(t, tt.wantTopCode, rec.Code)

			resp := decodeJSON(t, rec)
			deleted, _ := resp["DeletedFirewallRules"].([]any)
			errs, _ := resp["DeleteErrors"].([]any)
			assert.Len(t, deleted, tt.wantDeletedLen)
			assert.Len(t, errs, tt.wantErrorLen)
			if tt.wantErrorLen > 0 {
				errItem, ok := errs[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantErrorCode, errItem["Code"])
			}

			// Shared-state proof: the rule that WAS successfully deleted must
			// be gone from ListFirewallRules against the shared store.
			listRec := doRequest(t, h, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": grpID})
			require.Equal(t, http.StatusOK, listRec.Code)
			listed, _ := decodeJSON(t, listRec)["FirewallRules"].([]any)
			assert.Empty(t, listed)
		})
	}
}

// TestListFirewallRuleTypes covers ListFirewallRuleTypes' catalog, sourced
// directly from the real SDK's types.DnsThreatProtection enum
// (DGA/DNS_TUNNELING/DICTIONARY_DGA) rather than hand-written. The other
// three RuleType variants (FirewallAdvancedContentCategory/
// FirewallAdvancedThreatCategory/PartnerThreatProtection) have no closed SDK
// enum to source concrete Values from (see firewallRuleTypeCatalog's doc
// comment) and are correctly absent -- verified here by asserting a filter on
// one of them returns zero results rather than invented ones.
func TestListFirewallRuleTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ruleType   string
		wantValues []string
	}{
		{
			name:       "no_filter_returns_dns_threat_protection_catalog",
			wantValues: []string{"DGA", "DNS_TUNNELING", "DICTIONARY_DGA"},
		},
		{
			name:       "explicit_dns_threat_protection_filter",
			ruleType:   "DnsThreatProtection",
			wantValues: []string{"DGA", "DNS_TUNNELING", "DICTIONARY_DGA"},
		},
		{
			name:       "content_category_filter_returns_none_not_invented_values",
			ruleType:   "FirewallAdvancedContentCategory",
			wantValues: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}
			if tt.ruleType != "" {
				body["RuleType"] = tt.ruleType
			}

			rec := doRequest(t, h, "ListFirewallRuleTypes", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := decodeJSON(t, rec)
			items, _ := resp["FirewallRuleTypes"].([]any)
			require.Len(t, items, len(tt.wantValues))

			gotValues := make([]string, 0, len(items))
			for _, raw := range items {
				item, ok := raw.(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, item["RuleType"])
				assert.NotEmpty(t, item["DisplayName"])
				gotValues = append(gotValues, item["Value"].(string))
			}
			assert.ElementsMatch(t, tt.wantValues, gotValues)
		})
	}
}
