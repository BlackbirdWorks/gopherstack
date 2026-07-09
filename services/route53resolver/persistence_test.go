package route53resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *route53resolver.InMemoryBackend) string
		verify func(t *testing.T, b *route53resolver.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *route53resolver.InMemoryBackend) string {
				ep, err := b.CreateResolverEndpoint(
					context.Background(),
					"test-ep",
					"INBOUND",
					"vpc-12345",
					[]route53resolver.IPAddress{
						{SubnetID: "subnet-1", IP: "10.0.0.1"},
					},
					[]string{"sg-12345"},
					"IPV4",
					nil,
					"",
					"",
					"",
				)
				if err != nil {
					return ""
				}

				return ep.ID
			},
			verify: func(t *testing.T, b *route53resolver.InMemoryBackend, id string) {
				t.Helper()

				ep, err := b.GetResolverEndpoint(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, "test-ep", ep.Name)
				assert.Equal(t, id, ep.ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *route53resolver.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *route53resolver.InMemoryBackend, _ string) {
				t.Helper()

				endpoints := b.ListResolverEndpoints(context.Background())
				assert.Empty(t, endpoints)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises every store.Table
// (and every raw map left un-converted -- see store_setup.go) the Phase 3.3
// pkgs/store conversion touched, proving a full Snapshot -> Restore round
// trip preserves state for all of them, not just the resolver-endpoint path
// TestInMemoryBackend_SnapshotRestore already covers. Region-isolation itself
// (composite-key correctness) is covered separately by
// TestRoute53ResolverRegionIsolation and TestRoute53ResolverTagRegionIsolation
// in isolation_test.go.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	original := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")

	// 1. ResolverEndpoint + 2. ResolverRule (associated to the endpoint).
	ep, err := original.CreateResolverEndpoint(
		ctx, "full-state-ep", "INBOUND", "vpc-1",
		[]route53resolver.IPAddress{{SubnetID: "subnet-1", IP: "10.0.0.1"}},
		[]string{"sg-1"}, "IPV4", nil, "", "", "req-1",
	)
	require.NoError(t, err)

	rule, err := original.CreateResolverRule(ctx, "full-state-rule", "example.com", "FORWARD", ep.ID, "req-2", nil)
	require.NoError(t, err)

	// 3. Tags (raw map, keyed by ARN).
	require.NoError(t, original.TagResource(ctx, ep.ARN, []svcTags.KV{{Key: "env", Value: "prod"}}))

	// 4. FirewallRuleGroup + 5. FirewallRuleGroupAssociation.
	grp, err := original.CreateFirewallRuleGroup(ctx, "full-state-grp", "req-3")
	require.NoError(t, err)

	frgAssoc, err := original.AssociateFirewallRuleGroup(ctx, grp.ID, "vpc-1", "full-state-frg-assoc", "req-4", "", 0)
	require.NoError(t, err)

	// 6. FirewallDomainList + 7. FirewallRule.
	domainList, err := original.CreateFirewallDomainList(ctx, "full-state-domains", "req-5")
	require.NoError(t, err)

	fwRule, err := original.CreateFirewallRule(ctx, route53resolver.CreateFirewallRuleParams{
		FirewallRuleGroupID:  grp.ID,
		Name:                 "full-state-fw-rule",
		Action:               "ALLOW",
		FirewallDomainListID: domainList.ID,
	})
	require.NoError(t, err)

	// 8. OutpostResolver.
	outpost, err := original.CreateOutpostResolver(
		ctx, "full-state-outpost", "req-6", "arn:aws:outposts:us-east-1:000000000000:outpost/op-1", "", 0,
	)
	require.NoError(t, err)

	// 9. ResolverQueryLogConfig + 10. ResolverQueryLogConfigAssociation.
	qlc, err := original.CreateResolverQueryLogConfig(ctx, "full-state-qlc", "req-7", "arn:aws:s3:::full-state-bucket")
	require.NoError(t, err)

	qlcAssoc, err := original.AssociateResolverQueryLogConfig(ctx, qlc.ID, "vpc-1")
	require.NoError(t, err)

	// 11. ResolverRuleAssociation.
	ruleAssoc, err := original.AssociateResolverRule(ctx, rule.ID, "vpc-2", "full-state-rule-assoc")
	require.NoError(t, err)

	// 12. FirewallConfig, 13. ResolverConfig, 14. ResolverDnssecConfig
	// (get-or-create, keyed by ResourceID rather than a backend-generated ID).
	_, err = original.UpdateFirewallConfig(ctx, "vpc-3", "ENABLED")
	require.NoError(t, err)

	_, err = original.UpdateResolverConfig(ctx, "vpc-3", "ENABLE")
	require.NoError(t, err)

	_, err = original.UpdateResolverDnssecConfig(ctx, "vpc-3", "ENABLE")
	require.NoError(t, err)

	// 15-17. The three bare resource-policy maps.
	require.NoError(t, original.PutFirewallRuleGroupPolicy(ctx, grp.ARN, "policy-doc-frg"))
	require.NoError(t, original.PutResolverQueryLogConfigPolicy(ctx, qlc.ARN, "policy-doc-qlc"))
	require.NoError(t, original.PutResolverRulePolicy(ctx, rule.ARN, "policy-doc-rule"))

	// Round-trip through Snapshot/Restore onto a fresh backend.
	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	// Verify every table/map survived the round trip.
	gotEP, err := fresh.GetResolverEndpoint(ctx, ep.ID)
	require.NoError(t, err)
	assert.Equal(t, ep.Name, gotEP.Name)

	gotRule, err := fresh.GetResolverRule(ctx, rule.ID)
	require.NoError(t, err)
	assert.Equal(t, rule.Name, gotRule.Name)

	gotTags := fresh.ListTagsForResource(ctx, ep.ARN)
	require.Len(t, gotTags, 1)
	assert.Equal(t, "env", gotTags[0].Key)

	gotGrp, err := fresh.GetFirewallRuleGroup(ctx, grp.ID)
	require.NoError(t, err)
	assert.Equal(t, grp.Name, gotGrp.Name)

	gotFrgAssoc, err := fresh.GetFirewallRuleGroupAssociation(ctx, frgAssoc.ID)
	require.NoError(t, err)
	assert.Equal(t, frgAssoc.VpcID, gotFrgAssoc.VpcID)

	gotDomainList, err := fresh.GetFirewallDomainList(ctx, domainList.ID)
	require.NoError(t, err)
	assert.Equal(t, domainList.Name, gotDomainList.Name)

	gotFwRules := fresh.ListFirewallRules(ctx, grp.ID)
	require.Len(t, gotFwRules, 1)
	assert.Equal(t, fwRule.ID, gotFwRules[0].ID)

	gotOutpost, err := fresh.GetOutpostResolver(ctx, outpost.ID)
	require.NoError(t, err)
	assert.Equal(t, outpost.Name, gotOutpost.Name)

	gotQlc, err := fresh.GetResolverQueryLogConfig(ctx, qlc.ID)
	require.NoError(t, err)
	assert.Equal(t, qlc.Name, gotQlc.Name)

	gotQlcAssoc, err := fresh.GetResolverQueryLogConfigAssociation(ctx, qlcAssoc.ID)
	require.NoError(t, err)
	assert.Equal(t, qlcAssoc.ResourceID, gotQlcAssoc.ResourceID)

	gotRuleAssoc, err := fresh.GetResolverRuleAssociation(ctx, ruleAssoc.ID)
	require.NoError(t, err)
	assert.Equal(t, ruleAssoc.VPCID, gotRuleAssoc.VPCID)

	gotFwCfg := fresh.GetFirewallConfig(ctx, "vpc-3")
	assert.Equal(t, "ENABLED", gotFwCfg.FirewallFailOpen)

	gotResolverCfg := fresh.GetResolverConfig(ctx, "vpc-3")
	assert.Equal(t, "ENABLED", gotResolverCfg.AutodefinedReverse)

	gotDnssecCfg := fresh.GetResolverDnssecConfig(ctx, "vpc-3")
	assert.Equal(t, "ENABLING", gotDnssecCfg.ValidationStatus)

	assert.Equal(t, "policy-doc-frg", fresh.GetFirewallRuleGroupPolicy(ctx, grp.ARN))
	assert.Equal(t, "policy-doc-qlc", fresh.GetResolverQueryLogConfigPolicy(ctx, qlc.ARN))
	assert.Equal(t, "policy-doc-rule", fresh.GetResolverRulePolicy(ctx, rule.ARN))
}
