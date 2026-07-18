package route53resolver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestResolverRuleAssociationOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "full_lifecycle", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create rule and associate.
			rec := doRequest(t, h, "CreateResolverRule", map[string]any{
				"Name":       "rule-assoc",
				"DomainName": "assoc.example.com",
				"RuleType":   "FORWARD",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			})
			require.Equal(t, tt.wantCode, rec.Code)
			ruleResp := decodeJSON(t, rec)
			ruleID := ruleResp["ResolverRule"].(map[string]any)["Id"].(string)

			rec = doRequest(t, h, "AssociateResolverRule", map[string]any{
				"ResolverRuleId": ruleID,
				"VPCId":          "vpc-rule-assoc",
				"Name":           "my-assoc",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assocResp := decodeJSON(t, rec)
			assocID := assocResp["ResolverRuleAssociation"].(map[string]any)["Id"].(string)

			// GetResolverRuleAssociation.
			rec = doRequest(t, h, "GetResolverRuleAssociation", map[string]any{
				"ResolverRuleAssociationId": assocID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			getResp := decodeJSON(t, rec)
			assocObj, _ := getResp["ResolverRuleAssociation"].(map[string]any)
			assert.Equal(t, assocID, assocObj["Id"])

			// ListResolverRuleAssociations.
			rec = doRequest(t, h, "ListResolverRuleAssociations", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			items, _ := listResp["ResolverRuleAssociations"].([]any)
			assert.Len(t, items, 1)

			// DisassociateResolverRule: the real API keys this by
			// (ResolverRuleId, VPCId), not the opaque association ID
			// returned by Associate/Get.
			rec = doRequest(t, h, "DisassociateResolverRule", map[string]any{
				"ResolverRuleId": ruleID,
				"VPCId":          "vpc-rule-assoc",
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify gone.
			rec = doRequest(t, h, "GetResolverRuleAssociation", map[string]any{
				"ResolverRuleAssociationId": assocID,
			})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestResolverRuleAssociationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_missing_id",
			action:   "GetResolverRuleAssociation",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetResolverRuleAssociation",
			body:     map[string]any{"ResolverRuleAssociationId": "rslvr-rrassoc-notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "disassociate_missing_id",
			action:   "DisassociateResolverRule",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "disassociate_not_found",
			action: "DisassociateResolverRule",
			body: map[string]any{
				"ResolverRuleId": "rslvr-rr-notexist",
				"VPCId":          "vpc-notexist",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- GetResolverRulePolicy / PutResolverRulePolicy ---

func TestAssociateResolverRule_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "AssociateResolverRule", map[string]any{
		"ResolverRuleId": "rslvr-rr-nonexistent",
		"VPCId":          "vpc-12345",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- DeleteFirewallDomainList not-found ---

func TestAssociateResolverRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(ruleID string) map[string]any
		name       string
		wantStatus string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateResolverRule", map[string]any{
					"Name": "my-rule", "DomainName": "example.com", "RuleType": "FORWARD",
					"TargetIps": []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ResolverRule"].(map[string]any)["Id"].(string)
			},
			body: func(ruleID string) map[string]any {
				return map[string]any{
					"ResolverRuleId": ruleID,
					"VPCId":          "vpc-12345",
					"Name":           "rule-assoc",
				}
			},
			wantCode:   http.StatusOK,
			wantStatus: "COMPLETE",
		},
		{
			name:       "missing_rule_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{"VPCId": "vpc-1"} },
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "missing_vpc_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"ResolverRuleId": "rslvr-rr-notexist"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "rule_not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"ResolverRuleId": "rslvr-rr-notexist",
					"VPCId":          "vpc-1",
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ruleID := tt.setupExtra(t, h)
			rec := doRequest(t, h, "AssociateResolverRule", tt.body(ruleID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc, ok := resp["ResolverRuleAssociation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantStatus, assoc["Status"])
				assert.NotEmpty(t, assoc["Id"])
				assert.Equal(t, ruleID, assoc["ResolverRuleId"])
			}
		})
	}
}

// --- CreateFirewallDomainList ---

// TestParity_ListResolverRuleAssociations_Pagination verifies NextToken/MaxResults on
// ListResolverRuleAssociations.
func TestListResolverRuleAssociations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "rule-pag",
		"DomainName": "pag.example.com",
		"RuleType":   "FORWARD",
		"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)
	var ruleOut map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleOut))
	ruleID := ruleOut["ResolverRule"].(map[string]any)["Id"].(string)

	for i := range 3 {
		assocRec := doRequest(t, h, "AssociateResolverRule", map[string]any{
			"ResolverRuleId": ruleID,
			"VPCId":          fmt.Sprintf("vpc-rassoc-%d", i),
			"Name":           fmt.Sprintf("assoc-%d", i),
		})
		require.Equal(t, http.StatusOK, assocRec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverRuleAssociations", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assocs, _ := out["ResolverRuleAssociations"].([]any)
			assert.Len(t, assocs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}
