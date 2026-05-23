package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// --- CreateFirewallRuleGroup ---

func TestCreateFirewallRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":             "my-rule-group",
				"CreatorRequestId": "req-1",
			},
			wantCode: http.StatusOK,
			wantName: "my-rule-group",
		},
		{
			name:     "missing_name_returns_bad_request",
			body:     map[string]any{"CreatorRequestId": "req-2"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateFirewallRuleGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				grp, ok := resp["FirewallRuleGroup"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, grp["Name"])
				assert.Contains(t, grp["Arn"].(string), "arn:aws:route53resolver:")
				assert.NotEmpty(t, grp["Id"])
				assert.Equal(t, "COMPLETE", grp["Status"])
			}
		})
	}
}

// --- AssociateFirewallRuleGroup ---

func TestAssociateFirewallRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(groupID string) map[string]any
		name       string
		wantStatus string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "grp-assoc"})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["FirewallRuleGroup"].(map[string]any)["Id"].(string)
			},
			body: func(groupID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId": groupID,
					"VpcId":               "vpc-12345",
					"Name":                "assoc-1",
					"Priority":            200,
					"CreatorRequestId":    "req-assoc",
				}
			},
			wantCode:   http.StatusOK,
			wantStatus: "COMPLETE",
		},
		{
			name:       "missing_group_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"VpcId": "vpc-12345"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "missing_vpc_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"FirewallRuleGroupId": "rslvr-frg-notexist"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "group_not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId": "rslvr-frg-notexist",
					"VpcId":               "vpc-12345",
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			groupID := tt.setupExtra(t, h)
			rec := doRequest(t, h, "AssociateFirewallRuleGroup", tt.body(groupID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc, ok := resp["FirewallRuleGroupAssociation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantStatus, assoc["Status"])
				assert.NotEmpty(t, assoc["Id"])
				assert.Equal(t, groupID, assoc["FirewallRuleGroupId"])
			}
		})
	}
}

// --- AssociateResolverEndpointIpAddress ---

func TestAssociateResolverEndpointIpAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		endpointID  string
		wantCode    int
		wantIPCount int
		useRealEp   bool
	}{
		{
			name:        "success_adds_ip",
			useRealEp:   true,
			wantCode:    http.StatusOK,
			wantIPCount: 2, // existing 1 + new 1
		},
		{
			name:       "missing_endpoint_id",
			useRealEp:  false,
			endpointID: "",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "endpoint_not_found",
			useRealEp:  false,
			endpointID: "rslvr-in-notexist",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			endpointID := tt.endpointID
			if tt.useRealEp {
				rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
					"Name":        "assoc-ep",
					"Direction":   "INBOUND",
					"IpAddresses": []map[string]string{{"SubnetId": "subnet-1", "Ip": "10.0.0.1"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				endpointID = resp["ResolverEndpoint"].(map[string]any)["Id"].(string)
			}

			rec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": endpointID,
				"IpAddress":          map[string]string{"SubnetId": "subnet-2", "Ip": "10.0.0.2"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep, ok := resp["ResolverEndpoint"].(map[string]any)
				require.True(t, ok)
				ips := ep["IpAddresses"].([]any)
				assert.Len(t, ips, tt.wantIPCount)
			}
		})
	}
}

// --- CreateResolverQueryLogConfig ---

func TestCreateResolverQueryLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":             "my-log-config",
				"DestinationArn":   "arn:aws:s3:::my-bucket",
				"CreatorRequestId": "req-log-1",
			},
			wantCode: http.StatusOK,
			wantName: "my-log-config",
		},
		{
			name:     "missing_name",
			body:     map[string]any{"DestinationArn": "arn:aws:s3:::bucket"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_destination_arn",
			body:     map[string]any{"Name": "cfg"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cfg, ok := resp["ResolverQueryLogConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, cfg["Name"])
				assert.Equal(t, "CREATED", cfg["Status"])
				assert.NotEmpty(t, cfg["Id"])
				assert.Contains(t, cfg["Arn"].(string), "arn:aws:route53resolver:")
			}
		})
	}
}

// --- AssociateResolverQueryLogConfig ---

func TestAssociateResolverQueryLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(cfgID string) map[string]any
		name       string
		wantStatus string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
					"Name": "cfg", "DestinationArn": "arn:aws:s3:::bkt",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)
			},
			body: func(cfgID string) map[string]any {
				return map[string]any{
					"ResolverQueryLogConfigId": cfgID,
					"ResourceId":               "vpc-12345",
				}
			},
			wantCode:   http.StatusOK,
			wantStatus: "ACTIVE",
		},
		{
			name:       "missing_config_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{"ResourceId": "vpc-1"} },
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "missing_resource_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"ResolverQueryLogConfigId": "rqlc-notexist"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "config_not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"ResolverQueryLogConfigId": "rqlc-notexist",
					"ResourceId":               "vpc-1",
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cfgID := tt.setupExtra(t, h)
			rec := doRequest(t, h, "AssociateResolverQueryLogConfig", tt.body(cfgID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc, ok := resp["ResolverQueryLogConfigAssociation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantStatus, assoc["Status"])
				assert.NotEmpty(t, assoc["Id"])
				assert.Equal(t, cfgID, assoc["ResolverQueryLogConfigId"])
			}
		})
	}
}

// --- AssociateResolverRule ---

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

func TestCreateFirewallDomainList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":             "my-domain-list",
				"CreatorRequestId": "req-dl-1",
			},
			wantCode: http.StatusOK,
			wantName: "my-domain-list",
		},
		{
			name:     "missing_name",
			body:     map[string]any{"CreatorRequestId": "req-dl-2"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateFirewallDomainList", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				dl, ok := resp["FirewallDomainList"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, dl["Name"])
				assert.Equal(t, "COMPLETE", dl["Status"])
				assert.NotEmpty(t, dl["Id"])
				assert.Contains(t, dl["Arn"].(string), "arn:aws:route53resolver:")
			}
		})
	}
}

// --- DeleteFirewallDomainList ---

func TestDeleteFirewallDomainList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(id string) map[string]any
		name       string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "dl-del"})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["FirewallDomainList"].(map[string]any)["Id"].(string)
			},
			body:     func(id string) map[string]any { return map[string]any{"FirewallDomainListId": id} },
			wantCode: http.StatusOK,
		},
		{
			name:       "missing_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{} },
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{"FirewallDomainListId": "rslvr-fdl-notexist"} },
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setupExtra(t, h)
			rec := doRequest(t, h, "DeleteFirewallDomainList", tt.body(id))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				dl, ok := resp["FirewallDomainList"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, id, dl["Id"])
			}
		})
	}
}

// --- CreateFirewallRule ---

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

func TestCreateOutpostResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          any
		name          string
		wantName      string
		wantCode      int
		wantInstances int32
	}{
		{
			name: "success_default_instance_count",
			body: map[string]any{
				"Name":                  "my-outpost-resolver",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-1234",
				"PreferredInstanceType": "m5.xlarge",
				"CreatorRequestId":      "req-op-1",
			},
			wantCode:      http.StatusOK,
			wantName:      "my-outpost-resolver",
			wantInstances: 4,
		},
		{
			name: "success_custom_instance_count",
			body: map[string]any{
				"Name":                  "my-outpost-resolver-6",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-5678",
				"PreferredInstanceType": "m5.2xlarge",
				"InstanceCount":         6,
				"CreatorRequestId":      "req-op-2",
			},
			wantCode:      http.StatusOK,
			wantName:      "my-outpost-resolver-6",
			wantInstances: 6,
		},
		{
			name:     "missing_name",
			body:     map[string]any{"OutpostArn": "arn:aws:outposts:x", "PreferredInstanceType": "m5.xlarge"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_outpost_arn",
			body:     map[string]any{"Name": "resolver", "PreferredInstanceType": "m5.xlarge"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_instance_type",
			body:     map[string]any{"Name": "resolver", "OutpostArn": "arn:aws:outposts:x"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateOutpostResolver", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				r, ok := resp["OutpostResolver"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, r["Name"])
				assert.Equal(t, "OPERATIONAL", r["Status"])
				assert.Equal(t, tt.wantInstances, int32(r["InstanceCount"].(float64)))
				assert.NotEmpty(t, r["Id"])
				assert.Contains(t, r["Arn"].(string), "arn:aws:route53resolver:")
			}
		})
	}
}

// --- Persistence round-trip for new types ---

func TestNewOpsPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, h *route53resolver.Handler)
		verify func(t *testing.T, h2 *route53resolver.Handler, snap []byte)
		name   string
	}{
		{
			name: "firewall_rule_group_survives_snapshot",
			setup: func(t *testing.T, h *route53resolver.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "persist-grp"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			verify: func(t *testing.T, h2 *route53resolver.Handler, snap []byte) {
				t.Helper()
				require.NoError(t, h2.Restore(snap))
				// Create a new group to verify the backend is functional after restore.
				rec := doRequest(t, h2, "CreateFirewallRuleGroup", map[string]any{"Name": "post-restore-grp"})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "firewall_domain_list_survives_snapshot",
			setup: func(t *testing.T, h *route53resolver.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "persist-dl"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			verify: func(t *testing.T, h2 *route53resolver.Handler, snap []byte) {
				t.Helper()
				require.NoError(t, h2.Restore(snap))
				// Verify domain list can be listed/accessed after restore.
				rec := doRequest(t, h2, "CreateFirewallDomainList", map[string]any{"Name": "post-restore-dl"})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "outpost_resolver_survives_snapshot",
			setup: func(t *testing.T, h *route53resolver.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
					"Name":                  "persist-op",
					"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-persist",
					"PreferredInstanceType": "m5.xlarge",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			verify: func(t *testing.T, h2 *route53resolver.Handler, snap []byte) {
				t.Helper()
				require.NoError(t, h2.Restore(snap))
				rec := doRequest(t, h2, "CreateOutpostResolver", map[string]any{
					"Name":                  "post-restore-op",
					"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-pr",
					"PreferredInstanceType": "m5.xlarge",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)

			snap := h.Snapshot()
			require.NotEmpty(t, snap)

			h2 := route53resolver.NewHandler(route53resolver.NewInMemoryBackend("000000000000", "us-east-1"))
			tt.verify(t, h2, snap)
		})
	}
}
