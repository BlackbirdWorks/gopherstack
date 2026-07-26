package route53resolver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
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

// TestBackend_ConcurrentReadNoRace is a regression test for a class of data
// races where a read-only method -- holding only b.mu.RLock() -- lazily
// initialised a per-region resource map (e.g. b.tags[region] = ...) exactly
// like the Tag*/Put*Policy methods that hold the full b.mu.Lock(). Two
// RLock-holding readers could both observe a nil entry for a region no
// writer has ever touched and concurrently write to the same outer map,
// which is a data race on a plain Go map regardless of the RWMutex, since
// RLock does not serialize readers against each other.
//
// Each case hammers one such read path from many workers against a fresh
// backend for an ARN that has never been tagged/policied, so the very first
// call is the one that would lazily create the per-region entry. Run with
// `go test -race` to catch a regression of this class; it must pass cleanly
// with no race detected.
func TestBackend_ConcurrentReadNoRace(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:route53resolver:us-east-1:000000000000:resolver-rule/rslvr-rr-neverwritten"

	tests := []struct {
		call func(b *route53resolver.InMemoryBackend, ctx context.Context)
		name string
	}{
		{
			name: "list_tags_for_resource",
			call: func(b *route53resolver.InMemoryBackend, ctx context.Context) {
				_ = b.ListTagsForResource(ctx, arn)
			},
		},
		{
			name: "get_resolver_rule_policy",
			call: func(b *route53resolver.InMemoryBackend, ctx context.Context) {
				_ = b.GetResolverRulePolicy(ctx, arn)
			},
		},
		{
			name: "get_firewall_rule_group_policy",
			call: func(b *route53resolver.InMemoryBackend, ctx context.Context) {
				_ = b.GetFirewallRuleGroupPolicy(ctx, arn)
			},
		},
		{
			name: "get_resolver_query_log_config_policy",
			call: func(b *route53resolver.InMemoryBackend, ctx context.Context) {
				_ = b.GetResolverQueryLogConfigPolicy(ctx, arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			ctx := context.Background()

			const workers = 16
			const opsPerWorker = 25

			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					for range opsPerWorker {
						tt.call(backend, ctx)
					}
				})
			}
			wg.Wait()
		})
	}
}

// --- ListResolverEndpointIPAddresses not-found ---
