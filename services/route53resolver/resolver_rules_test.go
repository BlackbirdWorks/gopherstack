package route53resolver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestResolverRule_TargetIpWithIPv6AndProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		targetIP map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "ipv4_target",
			targetIP: map[string]any{"Ip": "10.0.0.1", "Port": 53},
			wantCode: http.StatusOK,
		},
		{
			name:     "ipv6_target",
			targetIP: map[string]any{"Ipv6": "2001:db8::1", "Port": 853},
			wantCode: http.StatusOK,
		},
		{
			name:     "target_with_protocol_doh",
			targetIP: map[string]any{"Ip": "10.0.0.1", "Port": 443, "Protocol": "DoH"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverRule", map[string]any{
				"Name":       "rule-target-test",
				"DomainName": "example.com",
				"RuleType":   "FORWARD",
				"TargetIps":  []map[string]any{tt.targetIP},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Issue 10: ResolverRule CreatorRequestId, OwnerID, timestamps ---

func TestResolverRule_CreatorAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":             "rule-with-creator",
		"DomainName":       "example.com",
		"RuleType":         "FORWARD",
		"CreatorRequestId": "req-rule-001",
		"TargetIps":        []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule := resp["ResolverRule"].(map[string]any)

	assert.Equal(t, "req-rule-001", rule["CreatorRequestId"])
	assert.NotEmpty(t, rule["OwnerId"])
	assert.NotEmpty(t, rule["CreationTime"])
	assert.NotEmpty(t, rule["ModificationTime"])
}

// --- Issue 12: SYSTEM/RECURSIVE rule enforcement ---

func TestResolverRule_TypeEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "system_no_endpoint_no_targets_ok",
			body: map[string]any{
				"Name":       "sys-rule",
				"DomainName": "sys.example.com",
				"RuleType":   "SYSTEM",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "recursive_no_endpoint_no_targets_ok",
			body: map[string]any{
				"Name":       "rec-rule",
				"DomainName": "rec.example.com",
				"RuleType":   "RECURSIVE",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "system_with_endpoint_rejected",
			body: map[string]any{
				"Name":               "sys-rule-ep",
				"DomainName":         "sys.example.com",
				"RuleType":           "SYSTEM",
				"ResolverEndpointId": "rslvr-in-12345678",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "system_with_target_ips_rejected",
			body: map[string]any{
				"Name":       "sys-rule-tips",
				"DomainName": "sys.example.com",
				"RuleType":   "SYSTEM",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "recursive_with_target_ips_rejected",
			body: map[string]any{
				"Name":       "rec-rule-tips",
				"DomainName": "rec.example.com",
				"RuleType":   "RECURSIVE",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "forward_with_targets_ok",
			body: map[string]any{
				"Name":       "fwd-rule",
				"DomainName": "fwd.example.com",
				"RuleType":   "FORWARD",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Issue 14: ResolverQueryLogConfig AssociationCount, ShareStatus ---

func TestBackend_RuleTypeEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ruleType  string
		epID      string
		targetIps []route53resolver.TargetIP
		wantErr   bool
	}{
		{
			name:     "system_no_ep_no_tips_ok",
			ruleType: "SYSTEM",
			wantErr:  false,
		},
		{
			name:     "system_with_ep_error",
			ruleType: "SYSTEM",
			epID:     "rslvr-in-fake1234",
			wantErr:  true,
		},
		{
			name:      "system_with_tips_error",
			ruleType:  "SYSTEM",
			targetIps: []route53resolver.TargetIP{{IP: "1.2.3.4", Port: 53}},
			wantErr:   true,
		},
		{
			name:      "forward_with_tips_ok",
			ruleType:  "FORWARD",
			targetIps: []route53resolver.TargetIP{{IP: "1.2.3.4", Port: 53}},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateResolverRule(
				context.Background(),
				"r1",
				"example.com",
				tt.ruleType,
				tt.epID,
				"",
				"",
				tt.targetIps,
			)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Backend direct: isValidQueryLogDestination ---

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

func TestCreateResolverRule_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_name",
			body: map[string]any{
				"DomainName": "example.com",
				"RuleType":   "FORWARD",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_domain_name",
			body: map[string]any{
				"Name":     "my-rule",
				"RuleType": "FORWARD",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_rule_type",
			body: map[string]any{
				"Name":       "my-rule",
				"DomainName": "example.com",
				"RuleType":   "INVALID",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "system_rule_type_ok",
			body: map[string]any{
				"Name":       "sys-rule",
				"DomainName": "example.com",
				"RuleType":   "SYSTEM",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "recursive_rule_type_ok",
			body: map[string]any{
				"Name":       "rec-rule",
				"DomainName": "example.com",
				"RuleType":   "RECURSIVE",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Sort order ---

func TestListResolverRules_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverRule", map[string]any{"Name": "z-rule", "DomainName": "z.com", "RuleType": "SYSTEM"})
	doRequest(t, h, "CreateResolverRule", map[string]any{"Name": "a-rule", "DomainName": "a.com", "RuleType": "SYSTEM"})

	rec := doRequest(t, h, "ListResolverRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rules, ok := resp["ResolverRules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, 2)
	assert.Equal(t, "a-rule", rules[0].(map[string]any)["Name"])
	assert.Equal(t, "z-rule", rules[1].(map[string]any)["Name"])
}

// --- DeleteResolverRule cascade ---

func TestDeleteResolverRule_CascadesAssociations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create rule.
	ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "cascade-rule",
		"DomainName": "cascade.com",
		"RuleType":   "SYSTEM",
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	ruleID := ruleResp["ResolverRule"].(map[string]any)["Id"].(string)

	// Associate with a VPC.
	assocRec := doRequest(t, h, "AssociateResolverRule", map[string]any{
		"ResolverRuleId": ruleID,
		"VPCId":          "vpc-cascade",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// Verify association count before delete.
	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	_ = b // we check via export helpers on h.Backend
	assert.Equal(t, 1, route53resolver.RuleAssociationCount(h.Backend.(*route53resolver.InMemoryBackend)))

	// Delete the rule — should cascade delete associations.
	delRec := doRequest(t, h, "DeleteResolverRule", map[string]any{"ResolverRuleId": ruleID})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Association should be gone.
	assert.Equal(t, 0, route53resolver.RuleAssociationCount(h.Backend.(*route53resolver.InMemoryBackend)))
}

// --- Firewall rule Action validation ---

func TestAddRuleInternal(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	r := b.AddRuleInternal("seeded-rule", "internal.example.com", "SYSTEM")
	require.NotNil(t, r)
	assert.Equal(t, "seeded-rule", r.Name)
	assert.Equal(t, 1, route53resolver.RuleCount(b))
}

func TestListResolverRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantLen    int
		wantToken  bool
	}{
		{"MaxResults=1 limits to 1", 1, 1, true},
		{"MaxResults=100 returns all 3", 100, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, d := range []string{"a.com", "b.com", "c.com"} {
				doRequest(t, h, "CreateResolverRule", map[string]any{
					"Name": "rule-" + d, "DomainName": d, "RuleType": "SYSTEM",
				})
			}

			rec := doRequest(t, h, "ListResolverRules", map[string]any{"MaxResults": tt.maxResults})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rules, _ := resp["ResolverRules"].([]any)
			assert.Len(t, rules, tt.wantLen)
			nextToken, _ := resp["NextToken"].(string)
			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestCreateResolverRule_EndpointValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		endpointID string
		wantCode   int
	}{
		{
			name:       "valid_endpoint",
			wantCode:   http.StatusOK,
			endpointID: "", // set after creating endpoint
		},
		{
			name:       "missing_endpoint_returns_not_found",
			endpointID: "rslvr-out-notexist",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "empty_endpoint_id_is_ok",
			endpointID: "",
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			endpointID := tt.endpointID
			if tt.name == "valid_endpoint" {
				// Create a real endpoint first.
				epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
					"Name":      "ep-for-rule",
					"Direction": "OUTBOUND",
				})
				require.Equal(t, http.StatusOK, epRec.Code)

				var epResp map[string]any
				require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
				ep := epResp["ResolverEndpoint"].(map[string]any)
				endpointID = ep["Id"].(string)
			}

			rec := doRequest(t, h, "CreateResolverRule", map[string]any{
				"Name":               "forward-rule",
				"DomainName":         "example.com.",
				"RuleType":           "FORWARD",
				"ResolverEndpointId": endpointID,
				"TargetIps":          []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestCreateResolverRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "my-rule",
		"DomainName": "example.com",
		"RuleType":   "FORWARD",
		"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule, ok := resp["ResolverRule"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, rule["Arn"], "arn:aws:route53resolver:")
	assert.Equal(t, "my-rule", rule["Name"])
	assert.Equal(t, "example.com", rule["DomainName"])
	assert.Equal(t, "FORWARD", rule["RuleType"])
	assert.Equal(t, "COMPLETE", rule["Status"])
}

// TestCreateResolverRule_DelegationRecord verifies DelegationRecord
// (verified against api_op_CreateResolverRule.go and types.ResolverRule --
// "DNS queries with delegation records that point to this domain name are
// forwarded to resolvers on your network") is accepted, stored, and echoed
// back on both Create and Get. The wire struct previously had no field for
// it at all, so a real client's value was silently dropped.
func TestCreateResolverRule_DelegationRecord(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":             "delegation-rule",
		"DomainName":       "example.com",
		"RuleType":         "FORWARD",
		"DelegationRecord": "ns.example.com",
		"TargetIps":        []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := decodeJSON(t, createRec)
	rule, ok := createResp["ResolverRule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ns.example.com", rule["DelegationRecord"])
	ruleID, _ := rule["Id"].(string)

	getRec := doRequest(t, h, "GetResolverRule", map[string]any{"ResolverRuleId": ruleID})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := decodeJSON(t, getRec)
	assert.Equal(t, "ns.example.com", getResp["ResolverRule"].(map[string]any)["DelegationRecord"])
}

func TestListResolverRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(
		t,
		h,
		"CreateResolverRule",
		map[string]any{
			"Name":       "r1",
			"DomainName": "a.com",
			"RuleType":   "FORWARD",
			"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
		},
	)
	doRequest(
		t,
		h,
		"CreateResolverRule",
		map[string]any{"Name": "r2", "DomainName": "b.com", "RuleType": "SYSTEM"},
	)

	rec := doRequest(t, h, "ListResolverRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rules, ok := resp["ResolverRules"].([]any)
	require.True(t, ok)
	assert.Len(t, rules, 2)
}

func TestListResolverRules_Filters(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *route53resolver.Handler {
		t.Helper()
		h := newTestHandler(t)
		doRequest(t, h, "CreateResolverRule", map[string]any{
			"Name": "fwd-rule", "DomainName": "fwd.example.com", "RuleType": "FORWARD",
			"CreatorRequestId": "req-fwd",
		})
		doRequest(t, h, "CreateResolverRule", map[string]any{
			"Name": "sys-rule", "DomainName": "sys.example.com", "RuleType": "SYSTEM",
			"CreatorRequestId": "req-sys",
		})

		return h
	}

	tests := []struct {
		filter    map[string]any
		name      string
		wantNames []string
	}{
		{
			name:      "type canonical name",
			filter:    map[string]any{"Name": "Type", "Values": []string{"FORWARD"}},
			wantNames: []string{"fwd-rule"},
		},
		{
			name:      "type legacy uppercase name",
			filter:    map[string]any{"Name": "TYPE", "Values": []string{"SYSTEM"}},
			wantNames: []string{"sys-rule"},
		},
		{
			name:      "domain name",
			filter:    map[string]any{"Name": "DomainName", "Values": []string{"sys.example.com"}},
			wantNames: []string{"sys-rule"},
		},
		{
			name:      "creator request id",
			filter:    map[string]any{"Name": "CreatorRequestId", "Values": []string{"req-fwd"}},
			wantNames: []string{"fwd-rule"},
		},
		{
			name:      "values are OR-combined",
			filter:    map[string]any{"Name": "Type", "Values": []string{"FORWARD", "SYSTEM"}},
			wantNames: []string{"fwd-rule", "sys-rule"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := setup(t)

			rec := doRequest(t, h, "ListResolverRules", map[string]any{
				"Filters": []map[string]any{tt.filter},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rules, _ := resp["ResolverRules"].([]any)
			gotNames := make([]string, len(rules))
			for i, r := range rules {
				gotNames[i] = r.(map[string]any)["Name"].(string)
			}
			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}

func TestListResolverRules_UnknownFilterNameRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name": "r1", "DomainName": "a.com", "RuleType": "FORWARD",
	})

	rec := doRequest(t, h, "ListResolverRules", map[string]any{
		"Filters": []map[string]any{
			{"Name": "NotARealFilter", "Values": []string{"x"}},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

func TestDeleteResolverRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "rule-to-delete",
		"DomainName": "test.com",
		"RuleType":   "FORWARD",
		"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	rule := createResp["ResolverRule"].(map[string]any)
	id := rule["Id"].(string)

	rec := doRequest(t, h, "DeleteResolverRule", map[string]any{
		"ResolverRuleId": id,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGetResolverRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "get-rule",
		"DomainName": "get.example.com",
		"RuleType":   "FORWARD",
		"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	rule := createResp["ResolverRule"].(map[string]any)
	id := rule["Id"].(string)

	rec := doRequest(t, h, "GetResolverRule", map[string]any{
		"ResolverRuleId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got, ok := resp["ResolverRule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, id, got["Id"])
	assert.Equal(t, "get-rule", got["Name"])
}
