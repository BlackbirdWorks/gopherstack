package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// --- UpdateFirewallConfig ---

func TestUpdateFirewallConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantFailOpen string
		wantCode     int
	}{
		{
			name:         "enable_success",
			body:         map[string]any{"ResourceId": "vpc-001", "FirewallFailOpen": "ENABLED"},
			wantCode:     http.StatusOK,
			wantFailOpen: "ENABLED",
		},
		{
			name:         "disable_success",
			body:         map[string]any{"ResourceId": "vpc-002", "FirewallFailOpen": "DISABLED"},
			wantCode:     http.StatusOK,
			wantFailOpen: "DISABLED",
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{"FirewallFailOpen": "ENABLED"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_fail_open_value",
			body:     map[string]any{"ResourceId": "vpc-003", "FirewallFailOpen": "INVALID"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateFirewallConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				cfg, ok := resp["FirewallConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantFailOpen, cfg["FirewallFailOpen"])
				assert.NotEmpty(t, cfg["Id"])
			}
		})
	}
}

// --- ListFirewallConfigs ---

func TestListFirewallConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupVPCs []string
		wantCount int
	}{
		{
			name:      "empty_list",
			setupVPCs: nil,
			wantCount: 0,
		},
		{
			name:      "multiple_configs",
			setupVPCs: []string{"vpc-a", "vpc-b"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, vpcID := range tt.setupVPCs {
				rec := doRequest(t, h, "GetFirewallConfig", map[string]any{"ResourceId": vpcID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListFirewallConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := decodeJSON(t, rec)
			items, _ := resp["FirewallConfigs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- GetResolverConfig ---

func TestGetResolverConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "creates_on_demand",
			body:     map[string]any{"ResourceId": "vpc-rc-1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetResolverConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				cfg, ok := resp["ResolverConfig"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, cfg["Id"])
				assert.Equal(t, "vpc-rc-1", cfg["ResourceId"])
			}
		})
	}
}

// --- UpdateResolverConfig ---

func TestUpdateResolverConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		name            string
		wantAutoReverse string
		wantCode        int
	}{
		{
			name:            "enable_success",
			body:            map[string]any{"ResourceId": "vpc-urc-1", "AutodefinedReverse": "ENABLE"},
			wantCode:        http.StatusOK,
			wantAutoReverse: "ENABLED",
		},
		{
			name:            "disable_success",
			body:            map[string]any{"ResourceId": "vpc-urc-2", "AutodefinedReverse": "DISABLE"},
			wantCode:        http.StatusOK,
			wantAutoReverse: "DISABLED",
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{"AutodefinedReverse": "ENABLE"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_value",
			body:     map[string]any{"ResourceId": "vpc-urc-3", "AutodefinedReverse": "INVALID"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateResolverConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				cfg, ok := resp["ResolverConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantAutoReverse, cfg["AutodefinedReverse"])
			}
		})
	}
}

// --- ListResolverConfigs ---

func TestListResolverConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupVPCs []string
		wantCount int
	}{
		{
			name:      "empty",
			setupVPCs: nil,
			wantCount: 0,
		},
		{
			name:      "two_configs",
			setupVPCs: []string{"vpc-lrc-1", "vpc-lrc-2"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, vpcID := range tt.setupVPCs {
				rec := doRequest(t, h, "GetResolverConfig", map[string]any{"ResourceId": vpcID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListResolverConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := decodeJSON(t, rec)
			items, _ := resp["ResolverConfigs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- ListResolverDnssecConfigs ---

func TestListResolverDnssecConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupVPCs []string
		wantCount int
	}{
		{
			name:      "empty",
			setupVPCs: nil,
			wantCount: 0,
		},
		{
			name:      "two_configs",
			setupVPCs: []string{"vpc-dnssec-1", "vpc-dnssec-2"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, vpcID := range tt.setupVPCs {
				rec := doRequest(t, h, "GetResolverDnssecConfig", map[string]any{"ResourceId": vpcID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListResolverDnssecConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := decodeJSON(t, rec)
			items, _ := resp["ResolverDnssecConfigs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- Outpost Resolver CRUD ---

func TestOutpostResolverCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "full_crud", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
				"Name":                  "op-res-1",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc",
				"PreferredInstanceType": "m5.xlarge",
			})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			r, ok := resp["OutpostResolver"].(map[string]any)
			require.True(t, ok)
			id := r["Id"].(string)

			// GetOutpostResolver.
			rec = doRequest(t, h, "GetOutpostResolver", map[string]any{"Id": id})
			assert.Equal(t, http.StatusOK, rec.Code)

			// ListOutpostResolvers.
			rec = doRequest(t, h, "ListOutpostResolvers", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			resolvers, _ := listResp["OutpostResolvers"].([]any)
			assert.Len(t, resolvers, 1)

			// UpdateOutpostResolver.
			rec = doRequest(t, h, "UpdateOutpostResolver", map[string]any{
				"Id":   id,
				"Name": "op-res-1-updated",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			updateResp := decodeJSON(t, rec)
			updated, _ := updateResp["OutpostResolver"].(map[string]any)
			assert.Equal(t, "op-res-1-updated", updated["Name"])

			// DeleteOutpostResolver.
			rec = doRequest(t, h, "DeleteOutpostResolver", map[string]any{"Id": id})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify deleted.
			rec = doRequest(t, h, "GetOutpostResolver", map[string]any{"Id": id})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestOutpostResolverErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_missing_id",
			action:   "GetOutpostResolver",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetOutpostResolver",
			body:     map[string]any{"Id": "rslvr-op-notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_missing_id",
			action:   "DeleteOutpostResolver",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteOutpostResolver",
			body:     map[string]any{"Id": "rslvr-op-notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update_missing_id",
			action:   "UpdateOutpostResolver",
			body:     map[string]any{"Name": "new"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update_not_found",
			action:   "UpdateOutpostResolver",
			body:     map[string]any{"Id": "rslvr-op-notexist", "Name": "new"},
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

// --- DeleteResolverQueryLogConfig / ListResolverQueryLogConfigs ---

func TestQueryLogConfigDeleteAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete_and_list", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a config.
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
				"Name":           "cfg-del",
				"DestinationArn": "arn:aws:s3:::my-bucket",
			})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			cfg, _ := resp["ResolverQueryLogConfig"].(map[string]any)
			cfgID := cfg["Id"].(string)

			// ListResolverQueryLogConfigs.
			rec = doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			items, _ := listResp["ResolverQueryLogConfigs"].([]any)
			assert.Len(t, items, 1)

			// DeleteResolverQueryLogConfig.
			rec = doRequest(t, h, "DeleteResolverQueryLogConfig", map[string]any{
				"ResolverQueryLogConfigId": cfgID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			delResp := decodeJSON(t, rec)
			deleted, _ := delResp["ResolverQueryLogConfig"].(map[string]any)
			assert.Equal(t, cfgID, deleted["Id"])

			// Verify gone.
			rec = doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			afterResp := decodeJSON(t, rec)
			afterItems, _ := afterResp["ResolverQueryLogConfigs"].([]any)
			assert.Empty(t, afterItems)
		})
	}
}

func TestQueryLogConfigErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "delete_missing_id",
			action:   "DeleteResolverQueryLogConfig",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteResolverQueryLogConfig",
			body:     map[string]any{"ResolverQueryLogConfigId": "rqlc-notexist"},
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

// --- GetResolverQueryLogConfigAssociation / ListResolverQueryLogConfigAssociations ---

func TestQueryLogConfigAssociationGetAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "get_and_list", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create config and associate.
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
				"Name":           "cfg-assoc",
				"DestinationArn": "arn:aws:s3:::bucket-assoc",
			})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			cfgID := resp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

			rec = doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
				"ResolverQueryLogConfigId": cfgID,
				"ResourceId":               "vpc-assoc-1",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assocResp := decodeJSON(t, rec)
			assocID := assocResp["ResolverQueryLogConfigAssociation"].(map[string]any)["Id"].(string)

			// GetResolverQueryLogConfigAssociation.
			rec = doRequest(t, h, "GetResolverQueryLogConfigAssociation", map[string]any{
				"ResolverQueryLogConfigAssociationId": assocID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			getResp := decodeJSON(t, rec)
			assocObj, _ := getResp["ResolverQueryLogConfigAssociation"].(map[string]any)
			assert.Equal(t, assocID, assocObj["Id"])

			// ListResolverQueryLogConfigAssociations.
			rec = doRequest(t, h, "ListResolverQueryLogConfigAssociations", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			items, _ := listResp["ResolverQueryLogConfigAssociations"].([]any)
			assert.Len(t, items, 1)
		})
	}
}

func TestQueryLogConfigAssociationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_missing_id",
			action:   "GetResolverQueryLogConfigAssociation",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetResolverQueryLogConfigAssociation",
			body:     map[string]any{"ResolverQueryLogConfigAssociationId": "rqlca-notexist"},
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

// --- GetResolverQueryLogConfigPolicy / PutResolverQueryLogConfigPolicy ---

func TestQueryLogConfigPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arn        string
		policy     string
		wantPolicy string
		wantCode   int
	}{
		{
			name:       "put_and_get",
			arn:        "arn:aws:route53resolver:us-east-1:000000000000:resolver-query-log-config/rqlc-abc",
			policy:     `{"Version":"2012-10-17"}`,
			wantPolicy: `{"Version":"2012-10-17"}`,
			wantCode:   http.StatusOK,
		},
		{
			name:     "put_missing_arn",
			arn:      "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_missing_arn",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "get_missing_arn" {
				rec := doRequest(t, h, "GetResolverQueryLogConfigPolicy", map[string]any{})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			if tt.arn == "" {
				rec := doRequest(t, h, "PutResolverQueryLogConfigPolicy", map[string]any{
					"ResolverQueryLogConfigPolicy": tt.policy,
				})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			// Put.
			rec := doRequest(t, h, "PutResolverQueryLogConfigPolicy", map[string]any{
				"Arn":                          tt.arn,
				"ResolverQueryLogConfigPolicy": tt.policy,
			})
			require.Equal(t, tt.wantCode, rec.Code)
			putResp := decodeJSON(t, rec)
			assert.Equal(t, true, putResp["ReturnValue"])

			// Get.
			rec = doRequest(t, h, "GetResolverQueryLogConfigPolicy", map[string]any{"Arn": tt.arn})
			assert.Equal(t, http.StatusOK, rec.Code)
			getResp := decodeJSON(t, rec)
			assert.Equal(t, tt.wantPolicy, getResp["ResolverQueryLogConfigPolicy"])
		})
	}
}

// --- GetResolverRuleAssociation / DisassociateResolverRule / ListResolverRuleAssociations ---

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

			// DisassociateResolverRule.
			rec = doRequest(t, h, "DisassociateResolverRule", map[string]any{
				"ResolverRuleAssociationId": assocID,
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
			name:     "disassociate_not_found",
			action:   "DisassociateResolverRule",
			body:     map[string]any{"ResolverRuleAssociationId": "rslvr-rrassoc-notexist"},
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

func TestResolverRulePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		policy   string
		wantCode int
	}{
		{
			name:     "put_and_get",
			arn:      "arn:aws:route53resolver:us-east-1:000000000000:resolver-rule/rslvr-rr-abc",
			policy:   `{"Version":"2012-10-17","Statement":[]}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "put_missing_arn",
			arn:      "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.arn == "" {
				rec := doRequest(t, h, "PutResolverRulePolicy", map[string]any{
					"ResolverRulePolicy": tt.policy,
				})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			rec := doRequest(t, h, "PutResolverRulePolicy", map[string]any{
				"Arn":                tt.arn,
				"ResolverRulePolicy": tt.policy,
			})
			require.Equal(t, tt.wantCode, rec.Code)
			putResp := decodeJSON(t, rec)
			assert.Equal(t, true, putResp["ReturnValue"])

			rec = doRequest(t, h, "GetResolverRulePolicy", map[string]any{"Arn": tt.arn})
			assert.Equal(t, http.StatusOK, rec.Code)
			getResp := decodeJSON(t, rec)
			assert.Equal(t, tt.policy, getResp["ResolverRulePolicy"])
		})
	}
}

func TestGetResolverRulePolicyMissingArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetResolverRulePolicy", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- DisassociateResolverEndpointIPAddress ---

func TestDisassociateResolverEndpointIPAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create endpoint with one IP.
			rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":        "ep-disassoc",
				"Direction":   "INBOUND",
				"IpAddresses": []map[string]string{{"SubnetId": "subnet-1", "Ip": "10.0.0.1"}},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			createResp := decodeJSON(t, rec)
			epID := createResp["ResolverEndpoint"].(map[string]any)["Id"].(string)

			// Add a second IP via AssociateResolverEndpointIpAddress.
			rec = doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": epID,
				"IpAddress":          map[string]string{"SubnetId": "subnet-2", "Ip": "10.0.0.2"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// List IPs to get the IPID of the second one.
			rec = doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
				"ResolverEndpointId": epID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			ips, _ := listResp["IpAddresses"].([]any)
			require.Len(t, ips, 2)

			// Get the IPID of the second IP (10.0.0.2).
			var ipID string
			for _, ipAny := range ips {
				ip, _ := ipAny.(map[string]any)
				if ip["Ip"] == "10.0.0.2" {
					ipID = ip["IpId"].(string)
				}
			}
			require.NotEmpty(t, ipID)

			// Disassociate.
			rec = doRequest(t, h, "DisassociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": epID,
				"IpAddress":          map[string]string{"IpId": ipID},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			// Verify one IP remains.
			rec = doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
				"ResolverEndpointId": epID,
			})
			afterResp := decodeJSON(t, rec)
			afterIPs, _ := afterResp["IpAddresses"].([]any)
			assert.Len(t, afterIPs, 1)
		})
	}
}

func TestDisassociateResolverEndpointIPAddressErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "missing_endpoint_id",
			body:     map[string]any{"IpAddress": map[string]string{"IpId": "ip-123"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_ip_id",
			body:     map[string]any{"ResolverEndpointId": "rslvr-in-abc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "endpoint_not_found",
			body: map[string]any{
				"ResolverEndpointId": "rslvr-in-notexist",
				"IpAddress":          map[string]string{"IpId": "ip-abc"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DisassociateResolverEndpointIpAddress", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- UpdateResolverRule ---

func TestUpdateResolverRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupRule func(t *testing.T, h *route53resolver.Handler) string
		body      func(ruleID string) map[string]any
		name      string
		wantName  string
		wantCode  int
	}{
		{
			name: "update_name_success",
			setupRule: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateResolverRule", map[string]any{
					"Name":       "original-name",
					"DomainName": "update.example.com",
					"RuleType":   "FORWARD",
					"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ResolverRule"].(map[string]any)["Id"].(string)
			},
			body: func(ruleID string) map[string]any {
				return map[string]any{
					"ResolverRuleId": ruleID,
					"Config":         map[string]any{"Name": "updated-name"},
				}
			},
			wantCode: http.StatusOK,
			wantName: "updated-name",
		},
		{
			name:      "missing_rule_id",
			setupRule: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"Config": map[string]any{"Name": "new"}}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "rule_not_found",
			setupRule: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"ResolverRuleId": "rslvr-rr-notexist",
					"Config":         map[string]any{"Name": "new"},
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ruleID := tt.setupRule(t, h)
			rec := doRequest(t, h, "UpdateResolverRule", tt.body(ruleID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				rule, ok := resp["ResolverRule"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, rule["Name"])
			}
		})
	}
}

// --- AddRuleInternalWithEndpoint / AddFirewallRuleInternal (backend helpers) ---

func TestBackendInternalHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *route53resolver.InMemoryBackend)
		name string
	}{
		{
			name: "add_rule_with_endpoint",
			run: func(t *testing.T, b *route53resolver.InMemoryBackend) {
				t.Helper()
				r := b.AddRuleInternalWithEndpoint("internal-rule", "internal.example.com", "FORWARD", "rslvr-in-ep01")
				require.NotNil(t, r)
				assert.Equal(t, "internal-rule", r.Name)
				assert.Equal(t, "rslvr-in-ep01", r.ResolverEndpointID)
				assert.Equal(t, 1, route53resolver.RuleCount(b))
			},
		},
		{
			name: "add_firewall_rule_internal_group_not_found",
			run: func(t *testing.T, b *route53resolver.InMemoryBackend) {
				t.Helper()
				r := b.AddFirewallRuleInternal("nonexistent-group", "rule", "BLOCK", "dl-id", 100)
				assert.Nil(t, r)
			},
		},
		{
			name: "add_firewall_rule_internal_success",
			run: func(t *testing.T, b *route53resolver.InMemoryBackend) {
				t.Helper()
				// First create a group via the handler-accessible method by creating via handler.
				// We need to create a group using the backend directly isn't available,
				// so use an indirect approach: create via handler then use backend directly.
				h := route53resolver.NewHandler(b)
				rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "grp-internal"})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := decodeJSON(t, rec)
				groupID := resp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

				rule := b.AddFirewallRuleInternal(groupID, "fw-rule", "ALLOW", "dl-id-1", 50)
				require.NotNil(t, rule)
				assert.Equal(t, "fw-rule", rule.Name)
				assert.Equal(t, "ALLOW", rule.Action)
				assert.Equal(t, int32(50), rule.Priority)
				assert.Equal(t, 1, route53resolver.FirewallRuleBackendCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			tt.run(t, b)
		})
	}
}

// --- resolverConfigToOutput (handler helper — tested via handler) ---

func TestResolverConfigToOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vpc      string
		wantCode int
	}{
		{
			name:     "get_creates_and_returns_config",
			vpc:      "vpc-rco-1",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetResolverConfig", map[string]any{"ResourceId": tt.vpc})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			cfg, ok := resp["ResolverConfig"].(map[string]any)
			require.True(t, ok)
			assert.NotEmpty(t, cfg["Arn"])
			assert.Equal(t, tt.vpc, cfg["ResourceId"])
		})
	}
}

// --- UpdateResolverEndpoint ---

func TestUpdateResolverEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupEP  func(t *testing.T, h *route53resolver.Handler) string
		body     func(epID string) map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "update_name",
			setupEP: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
					"Name":      "ep-before-update",
					"Direction": "INBOUND",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := decodeJSON(t, rec)

				return resp["ResolverEndpoint"].(map[string]any)["Id"].(string)
			},
			body: func(epID string) map[string]any {
				return map[string]any{
					"ResolverEndpointId": epID,
					"Name":               "ep-after-update",
				}
			},
			wantCode: http.StatusOK,
			wantName: "ep-after-update",
		},
		{
			name:    "missing_id",
			setupEP: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"Name": "new-name"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:    "not_found",
			setupEP: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"ResolverEndpointId": "rslvr-in-notexist", "Name": "x"}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			epID := tt.setupEP(t, h)
			rec := doRequest(t, h, "UpdateResolverEndpoint", tt.body(epID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				ep, ok := resp["ResolverEndpoint"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, ep["Name"])
			}
		})
	}
}
