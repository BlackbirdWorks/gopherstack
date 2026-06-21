package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// --- Handler infrastructure ---

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ep-to-reset",
		"Direction": "INBOUND",
	})

	h.Reset()

	rec := doRequest(t, h, "ListResolverEndpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["ResolverEndpoints"].([]any)
	require.True(t, ok)
	assert.Empty(t, endpoints)
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 68, route53resolver.HandlerOpsLen(h))
}

func TestRefinement1_BackendAccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// --- ErrNilAppContext ---

func TestRefinement1_ProviderNilContext(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, route53resolver.ErrNilAppContext)
}

func TestRefinement1_ProviderValidContext(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// --- Endpoint validation ---

func TestRefinement1_CreateResolverEndpoint_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_name_returns_bad_request",
			body:     map[string]any{"Direction": "INBOUND"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_direction_returns_bad_request",
			body:     map[string]any{"Name": "ep", "Direction": "INVALID"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_direction_returns_bad_request",
			body:     map[string]any{"Name": "ep"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "inbound_direction_ok",
			body:     map[string]any{"Name": "ep", "Direction": "INBOUND"},
			wantCode: http.StatusOK,
		},
		{
			name:     "outbound_direction_ok",
			body:     map[string]any{"Name": "ep", "Direction": "OUTBOUND"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverEndpoint", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Endpoint VpcId + SecurityGroupIds in output ---

func TestRefinement1_CreateResolverEndpoint_VpcIdAndSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":             "ep-with-vpc",
		"Direction":        "INBOUND",
		"VpcId":            "vpc-abc123",
		"SecurityGroupIds": []string{"sg-111", "sg-222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep, ok := resp["ResolverEndpoint"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "vpc-abc123", ep["VpcId"])
	sgs, ok := ep["SecurityGroupIds"].([]any)
	require.True(t, ok)
	assert.Len(t, sgs, 2)
}

// --- Resolver rule validation ---

func TestRefinement1_CreateResolverRule_Validation(t *testing.T) {
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

func TestRefinement1_ListResolverEndpoints_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "zebra-ep", "Direction": "INBOUND"})
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "alpha-ep", "Direction": "OUTBOUND"})
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "mango-ep", "Direction": "INBOUND"})

	rec := doRequest(t, h, "ListResolverEndpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["ResolverEndpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 3)

	names := make([]string, 3)
	for i, e := range endpoints {
		names[i] = e.(map[string]any)["Name"].(string)
	}
	assert.Equal(t, []string{"alpha-ep", "mango-ep", "zebra-ep"}, names)
}

func TestRefinement1_ListResolverRules_SortedByName(t *testing.T) {
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

func TestRefinement1_DeleteResolverRule_CascadesAssociations(t *testing.T) {
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

func TestRefinement1_CreateFirewallRule_ActionValidation(t *testing.T) {
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

func TestRefinement1_CreateFirewallRule_IdAndArnInOutput(t *testing.T) {
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

func TestRefinement1_CountHelpers(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")

	ep := b.AddEndpointInternal("ep1", "INBOUND")
	require.NotNil(t, ep)
	assert.Equal(t, 1, route53resolver.EndpointCount(b))

	rule := b.AddRuleInternal("rule1", "example.com", "FORWARD")
	require.NotNil(t, rule)
	assert.Equal(t, 1, route53resolver.RuleCount(b))

	grp := b.AddFirewallRuleGroupInternal("grp1")
	require.NotNil(t, grp)
	assert.Equal(t, 1, route53resolver.FirewallRuleGroupCount(b))

	dl := b.AddFirewallDomainListInternal("dl1")
	require.NotNil(t, dl)
	assert.Equal(t, 1, route53resolver.FirewallDomainListCount(b))

	op := b.AddOutpostResolverInternal("op1", "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc")
	require.NotNil(t, op)
	assert.Equal(t, 1, route53resolver.OutpostResolverCount(b))

	cfg := b.AddQueryLogConfigInternal("cfg1", "arn:aws:s3:::my-bucket")
	require.NotNil(t, cfg)
	assert.Equal(t, 1, route53resolver.QueryLogConfigCount(b))
}

// --- AddXInternal seed helpers ---

func TestRefinement1_AddEndpointInternal(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	ep := b.AddEndpointInternal("seeded-ep", "OUTBOUND")
	require.NotNil(t, ep)
	assert.Equal(t, "seeded-ep", ep.Name)
	assert.Contains(t, ep.ARN, "arn:aws:route53resolver:")
	assert.Equal(t, 1, route53resolver.EndpointCount(b))
}

func TestRefinement1_AddRuleInternal(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	r := b.AddRuleInternal("seeded-rule", "internal.example.com", "SYSTEM")
	require.NotNil(t, r)
	assert.Equal(t, "seeded-rule", r.Name)
	assert.Equal(t, 1, route53resolver.RuleCount(b))
}

func TestRefinement1_AddFirewallRuleGroupInternal(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	g := b.AddFirewallRuleGroupInternal("seeded-grp")
	require.NotNil(t, g)
	assert.Equal(t, "seeded-grp", g.Name)
	assert.Equal(t, 1, route53resolver.FirewallRuleGroupCount(b))
}

// --- Tags on Create ---

func TestRefinement1_CreateResolverEndpoint_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "tagged-ep",
		"Direction": "INBOUND",
		"Tags":      []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep := resp["ResolverEndpoint"].(map[string]any)
	epARN := ep["Arn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": epARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)
}

// --- AssociateResolverQueryLogConfig not-found ---

func TestRefinement1_AssociateResolverQueryLogConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": "rqlc-nonexistent",
		"ResourceId":               "vpc-12345",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- AssociateResolverRule not-found ---

func TestRefinement1_AssociateResolverRule_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "AssociateResolverRule", map[string]any{
		"ResolverRuleId": "rslvr-rr-nonexistent",
		"VPCId":          "vpc-12345",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- DeleteFirewallDomainList not-found ---

func TestRefinement1_DeleteFirewallDomainList_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteFirewallDomainList", map[string]any{
		"FirewallDomainListId": "rslvr-fdl-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- CreateOutpostResolver defaults ---

func TestRefinement1_CreateOutpostResolver_DefaultInstanceCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
		"Name":                  "my-outpost",
		"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc",
		"PreferredInstanceType": "m5.large",
		"InstanceCount":         0, // should default to 4
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	resolver, ok := resp["OutpostResolver"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 4, resolver["InstanceCount"], 0)
}

// --- Persistence snapshot with all maps ---

func TestRefinement1_SnapshotRestoreAllMaps(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")

	b.AddEndpointInternal("ep1", "INBOUND")
	b.AddRuleInternal("rule1", "a.com", "FORWARD")
	b.AddFirewallRuleGroupInternal("frg1")
	b.AddFirewallDomainListInternal("fdl1")
	b.AddOutpostResolverInternal("op1", "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc")
	b.AddQueryLogConfigInternal("cfg1", "arn:aws:s3:::logs-bucket")

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, route53resolver.EndpointCount(b2))
	assert.Equal(t, 1, route53resolver.RuleCount(b2))
	assert.Equal(t, 1, route53resolver.FirewallRuleGroupCount(b2))
	assert.Equal(t, 1, route53resolver.FirewallDomainListCount(b2))
	assert.Equal(t, 1, route53resolver.OutpostResolverCount(b2))
	assert.Equal(t, 1, route53resolver.QueryLogConfigCount(b2))
}

// --- Reset clears all maps ---

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")

	b.AddEndpointInternal("ep1", "INBOUND")
	b.AddRuleInternal("rule1", "a.com", "FORWARD")
	b.AddFirewallRuleGroupInternal("frg1")

	assert.Equal(t, 1, route53resolver.EndpointCount(b))
	assert.Equal(t, 1, route53resolver.RuleCount(b))
	assert.Equal(t, 1, route53resolver.FirewallRuleGroupCount(b))

	b.Reset()

	assert.Equal(t, 0, route53resolver.EndpointCount(b))
	assert.Equal(t, 0, route53resolver.RuleCount(b))
	assert.Equal(t, 0, route53resolver.FirewallRuleGroupCount(b))
}

// --- ListResolverEndpointIPAddresses not-found ---

func TestRefinement1_ListResolverEndpointIPAddresses_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
		"ResolverEndpointId": "rslvr-in-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- AssociateFirewallRuleGroup not-found ---

func TestRefinement1_AssociateFirewallRuleGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": "rslvr-frg-nonexistent",
		"VpcId":               "vpc-12345",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- CreateFirewallRule increments RuleCount ---

func TestRefinement1_CreateFirewallRule_IncrementsRuleCount(t *testing.T) {
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

func TestRefinement1_AssociateResolverEndpointIPAddress_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ip-assoc-ep",
		"Direction": "INBOUND",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ep := createResp["ResolverEndpoint"].(map[string]any)
	epID := ep["Id"].(string)

	assocRec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
		"ResolverEndpointId": epID,
		"IpAddress": map[string]string{
			"SubnetId": "subnet-new",
			"Ip":       "10.0.1.5",
		},
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	updatedEP := assocResp["ResolverEndpoint"].(map[string]any)
	ips, ok := updatedEP["IpAddresses"].([]any)
	require.True(t, ok)
	assert.Len(t, ips, 1)
}

// --- Pagination: MaxResults + NextToken -----------------------------------------

func TestRefinement1_ListResolverEndpoints_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantLen    int
		wantToken  bool
	}{
		{"MaxResults=1 limits to 1", 1, 1, true},
		{"MaxResults=2 limits to 2", 2, 2, true},
		{"MaxResults=100 returns all 3", 100, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, name := range []string{"ep-a", "ep-b", "ep-c"} {
				doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": name, "Direction": "INBOUND"})
			}

			rec := doRequest(t, h, "ListResolverEndpoints", map[string]any{"MaxResults": tt.maxResults})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			eps, _ := resp["ResolverEndpoints"].([]any)
			assert.Len(t, eps, tt.wantLen)
			nextToken, _ := resp["NextToken"].(string)
			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestRefinement1_ListResolverRules_Pagination(t *testing.T) {
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

func TestRefinement1_ListFirewallRuleGroups_Pagination(t *testing.T) {
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
			for _, name := range []string{"grp-a", "grp-b", "grp-c"} {
				doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": name})
			}

			rec := doRequest(t, h, "ListFirewallRuleGroups", map[string]any{"MaxResults": tt.maxResults})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			groups, _ := resp["FirewallRuleGroups"].([]any)
			assert.Len(t, groups, tt.wantLen)
			nextToken, _ := resp["NextToken"].(string)
			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestRefinement1_ListResolverEndpoints_NextTokenContinuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"ep-x", "ep-y", "ep-z"} {
		doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": name, "Direction": "INBOUND"})
	}

	rec1 := doRequest(t, h, "ListResolverEndpoints", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	eps1, _ := resp1["ResolverEndpoints"].([]any)
	require.Len(t, eps1, 2)
	nextToken, _ := resp1["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec2 := doRequest(t, h, "ListResolverEndpoints", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	eps2, _ := resp2["ResolverEndpoints"].([]any)
	assert.NotEmpty(t, eps2)

	names1 := make(map[string]bool)
	for _, e := range eps1 {
		em, _ := e.(map[string]any)
		names1[em["Name"].(string)] = true
	}
	for _, e := range eps2 {
		em, _ := e.(map[string]any)
		assert.False(t, names1[em["Name"].(string)], "page 2 should not repeat page 1 items")
	}
}
