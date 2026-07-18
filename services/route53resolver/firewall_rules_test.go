package route53resolver_test

import (
	"context"
	"encoding/json"
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
					"BlockOverrideDNSType": "CNAME",
					"BlockOverrideTTL":     300,
					"Name":                 "block-override",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "OVERRIDE", rule["BlockResponse"])
				assert.Equal(t, "safe.example.com", rule["BlockOverrideDomain"])
				assert.Equal(t, "CNAME", rule["BlockOverrideDNSType"])
				assert.EqualValues(t, 300, rule["BlockOverrideTTL"])
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

func TestUpdateFirewallRule_ExtendedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "update-rule-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "update-rule-dl"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	var dlResp map[string]any
	require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
	dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

	createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  groupID,
		"FirewallDomainListId": dlID,
		"Priority":             100,
		"Action":               "ALLOW",
		"Name":                 "rule-to-update",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ruleID := createResp["FirewallRule"].(map[string]any)["Id"].(string)

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
			var createR2 map[string]any
			require.NoError(t, json.Unmarshal(createRec2.Body.Bytes(), &createR2))
			rID := createR2["FirewallRule"].(map[string]any)["Id"].(string)

			upd := tt.update
			upd["FirewallRuleId"] = rID
			rec := doRequest(t, h2, "UpdateFirewallRule", upd)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rule := resp["FirewallRule"].(map[string]any)
			tt.checkFn(t, rule)
		})
	}

	_ = ruleID // used in setup only
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

// --- Firewall rule Id and Arn in output ---

func TestCreateFirewallRule_IdAndArnInOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "id-test-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)

	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	grpID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	ruleRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId": grpID,
		"Name":                "my-fw-rule",
		"Action":              "ALLOW",
		"Priority":            10,
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	rule, ok := ruleResp["FirewallRule"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, rule["Id"])
	assert.Contains(t, rule["Arn"].(string), "arn:aws:route53resolver:")
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
	ruleResp := decodeJSON(t, rec)
	rule, _ := ruleResp["FirewallRule"].(map[string]any)
	ruleID, _ := rule["Id"].(string)
	require.NotEmpty(t, ruleID)

	// ListFirewallRules.
	rec = doRequest(t, h, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": rgID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateFirewallRule.
	rec = doRequest(t, h, "UpdateFirewallRule", map[string]any{
		"FirewallRuleId": ruleID,
		"Priority":       200,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteFirewallRule.
	rec = doRequest(t, h, "DeleteFirewallRule", map[string]any{
		"FirewallRuleId": ruleID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
