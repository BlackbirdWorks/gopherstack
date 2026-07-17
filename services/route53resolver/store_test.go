package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// TestAudit_FullCRUDLifecycle verifies complete lifecycle across all major resources.
func TestFullCRUDLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 1. Endpoint lifecycle.
	epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":                 "lifecycle-ep",
		"Direction":            "OUTBOUND",
		"ResolverEndpointType": "IPV4",
		"Protocols":            []string{"Do53"},
		"CreatorRequestId":     "req-ep-lifecycle",
	})
	require.Equal(t, http.StatusOK, epRec.Code)
	var epResp map[string]any
	require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
	ep := epResp["ResolverEndpoint"].(map[string]any)
	epID := ep["Id"].(string)
	assert.NotEmpty(t, ep["CreationTime"])

	// 2. Rule with TargetIps.
	ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":               "lifecycle-rule",
		"DomainName":         "example.internal",
		"RuleType":           "FORWARD",
		"ResolverEndpointId": epID,
		"CreatorRequestId":   "req-rule-lifecycle",
		"TargetIps": []map[string]any{
			{"Ip": "10.0.0.1", "Port": 53},
			{"Ip": "10.0.0.2", "Port": 53},
		},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)
	var ruleResp map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	rule := ruleResp["ResolverRule"].(map[string]any)
	ruleID := rule["Id"].(string)
	assert.Equal(t, "req-rule-lifecycle", rule["CreatorRequestId"])
	assert.NotEmpty(t, rule["OwnerId"])

	// 3. Query log config.
	qlcRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "lifecycle-qlc",
		"DestinationArn": "arn:aws:s3:::lifecycle-logs",
	})
	require.Equal(t, http.StatusOK, qlcRec.Code)
	var qlcResp map[string]any
	require.NoError(t, json.Unmarshal(qlcRec.Body.Bytes(), &qlcResp))
	qlc := qlcResp["ResolverQueryLogConfig"].(map[string]any)
	qlcID := qlc["Id"].(string)
	assert.EqualValues(t, 0, qlc["AssociationCount"])

	// 4. Query log association increments count.
	doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": qlcID,
		"ResourceId":               "vpc-lifecycle",
	})
	getRec := doRequest(
		t,
		h,
		"GetResolverQueryLogConfig",
		map[string]any{"ResolverQueryLogConfigId": qlcID},
	)
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.EqualValues(t, 1, getResp["ResolverQueryLogConfig"].(map[string]any)["AssociationCount"])

	// 5. Firewall rule group with timestamps.
	frgRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "lifecycle-frg",
		"CreatorRequestId": "req-frg-lifecycle",
	})
	require.Equal(t, http.StatusOK, frgRec.Code)
	var frgResp map[string]any
	require.NoError(t, json.Unmarshal(frgRec.Body.Bytes(), &frgResp))
	frg := frgResp["FirewallRuleGroup"].(map[string]any)
	frgID := frg["Id"].(string)
	assert.Equal(t, "NOT_SHARED", frg["ShareStatus"])
	assert.NotEmpty(t, frg["CreationTime"])

	// 6. Delete rule + endpoint.
	doRequest(t, h, "DeleteResolverRule", map[string]any{"ResolverRuleId": ruleID})
	doRequest(t, h, "DeleteResolverEndpoint", map[string]any{"ResolverEndpointId": epID})
	doRequest(t, h, "DeleteFirewallRuleGroup", map[string]any{"FirewallRuleGroupId": frgID})

	// 7. Verify deletion.
	getRuleRec := doRequest(t, h, "GetResolverRule", map[string]any{"ResolverRuleId": ruleID})
	assert.Equal(t, http.StatusNotFound, getRuleRec.Code)
}

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

func TestBackendAccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// --- ErrNilAppContext ---

func TestCountHelpers(t *testing.T) {
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

func TestBackendReset(t *testing.T) {
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
