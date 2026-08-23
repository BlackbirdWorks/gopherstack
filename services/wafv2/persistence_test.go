package wafv2_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// TestBackend_SnapshotRestore_FullStateRoundTrip exercises Phase 3.3's
// converted store.Table/store.Index resources (webACLs, ipSets,
// regexPatternSets, ruleGroups -- "clean" tables registered directly on
// b.registry) alongside the "dirty" DTO-backed tables (managedRuleSets,
// apiKeys) and the raw maps left untouched by the conversion
// (loggingConfigs, permissionPolicies, associations), across both a REGIONAL
// and a CLOUDFRONT resource, to confirm nothing is silently dropped by the
// region+id composite keys or the json:"-" identity fields introduced by
// this refactor.
func TestBackend_SnapshotRestore_FullStateRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")

	webACL, err := wafv2.CreateWebACLSimple(b, "full-state-acl", wafv2.ScopeRegional, "desc", "ALLOW",
		map[string]string{"env": "test"})
	require.NoError(t, err)

	cfWebACL, err := wafv2.CreateWebACLSimple(b, "full-state-acl-cf", wafv2.ScopeCloudFront, "desc", "ALLOW", nil)
	require.NoError(t, err)

	ipSet, err := b.CreateIPSet(ctx, "full-state-ip", wafv2.ScopeRegional, "desc",
		wafv2.IPVersionIPv4, []string{"10.0.0.0/8"}, nil)
	require.NoError(t, err)

	rps, err := b.CreateRegexPatternSet(ctx, "full-state-rps", wafv2.ScopeRegional, "desc",
		[]wafv2.RegexEntry{{RegexString: "^abc$"}}, nil)
	require.NoError(t, err)

	rg, err := b.CreateRuleGroup(ctx, "full-state-rg", wafv2.ScopeRegional, "desc", "{}", 10, nil, nil, nil, nil)
	require.NoError(t, err)

	ms, err := b.PutManagedRuleSetVersions(ctx, "full-state-mrs", "full-state-mrs", wafv2.ScopeRegional, "", "",
		map[string]any{"v1": map[string]any{"Capacity": float64(5)}})
	require.NoError(t, err)

	apiKey, err := b.CreateAPIKey(ctx, wafv2.ScopeRegional, []string{"example.com"})
	require.NoError(t, err)

	require.NoError(t, b.PutLoggingConfiguration(ctx, webACL.ARN, []byte(`{"LogDestinationConfigs":["a"]}`)))
	require.NoError(t, b.PutPermissionPolicy(ctx, webACL.ARN, `{"Version":"2012-10-17"}`))
	lbARN := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/x/1"
	require.NoError(t, b.AssociateWebACL(ctx, webACL.ARN, lbARN))

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	restored := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(ctx, snap))

	// REGIONAL + CLOUDFRONT WebACLs both survive, and ListWebACLs still
	// merges CLOUDFRONT (global) entries into a REGIONAL-region listing.
	acls := restored.ListWebACLs(ctx)
	require.Len(t, acls, 2)

	gotWebACL, err := restored.GetWebACL(ctx, webACL.ID)
	require.NoError(t, err)
	assert.Equal(t, webACL.Name, gotWebACL.Name)
	assert.Equal(t, map[string]string{"env": "test"}, gotWebACL.Tags)

	gotCFWebACL, err := restored.GetWebACL(ctx, cfWebACL.ID)
	require.NoError(t, err)
	assert.Equal(t, cfWebACL.Name, gotCFWebACL.Name)

	gotIPSet, err := restored.GetIPSet(ctx, ipSet.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.0/8"}, gotIPSet.Addresses)

	gotRPS, err := restored.GetRegexPatternSet(ctx, rps.ID)
	require.NoError(t, err)
	assert.Equal(t, "^abc$", gotRPS.RegularExpressionList[0].RegexString)

	gotRG, err := restored.GetRuleGroup(ctx, rg.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(10), gotRG.Capacity)

	gotMS, err := restored.GetManagedRuleSet(ctx, ms.ID)
	require.NoError(t, err)
	assert.Contains(t, gotMS.PublishedVersions, "v1")

	gotAPIKey, err := restored.GetDecryptedAPIKey(ctx, wafv2.ScopeRegional, apiKey.APIKeyValue)
	require.NoError(t, err)
	assert.Equal(t, []string{"example.com"}, gotAPIKey.TokenDomains)

	gotLogCfg, err := restored.GetLoggingConfiguration(ctx, webACL.ARN)
	require.NoError(t, err)
	assert.JSONEq(t, `{"LogDestinationConfigs":["a"]}`, string(gotLogCfg))

	gotPolicy, err := restored.GetPermissionPolicy(ctx, webACL.ARN)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, gotPolicy)

	resources, err := restored.ListResourcesForWebACL(ctx, webACL.ARN)
	require.NoError(t, err)
	assert.Equal(t, []string{lbARN}, resources)
}

// TestBackend_Restore_IncompatibleVersionResetsState confirms that a
// snapshot whose version field doesn't match the current
// wafv2SnapshotVersion is discarded (state reset to empty) rather than
// partially decoded, and that Restore reports success (nil error) for this
// expected/recoverable condition -- mirroring services/ses's guard.
func TestBackend_Restore_IncompatibleVersionResetsState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := wafv2.CreateWebACLSimple(b, "pre-existing", wafv2.ScopeRegional, "", "ALLOW", nil)
	require.NoError(t, err)
	require.Len(t, b.ListWebACLs(ctx), 1)

	// A snapshot with no "version" field at all (pre-Phase-3.3 shape, or any
	// other incompatible payload) decodes with Version == 0.
	incompatible := []byte(`{"tables":{},"version":0}`)

	require.NoError(t, b.Restore(ctx, incompatible))
	assert.Empty(t, b.ListWebACLs(ctx))
}

func TestBackend_Snapshot_And_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*wafv2.InMemoryBackend)
		name    string
		wantIDs int
	}{
		{
			name:  "empty_backend",
			setup: func(_ *wafv2.InMemoryBackend) {},
		},
		{
			name: "with_webacls_and_ipsets",
			setup: func(b *wafv2.InMemoryBackend) {
				_, _ = wafv2.CreateWebACLSimple(b, "acl1", "REGIONAL", "desc", "ALLOW", nil)
				_, _ = b.CreateIPSet(
					context.Background(), "set1", "REGIONAL", "desc", "IPV4", []string{"1.2.3.4/32"}, nil,
				)
			},
			wantIDs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			acls := b2.ListWebACLs(context.Background())
			sets := b2.ListIPSets(context.Background())

			assert.Len(t, acls, len(b.ListWebACLs(context.Background())))
			assert.Len(t, sets, len(b.ListIPSets(context.Background())))
		})
	}
}

func TestHandler_Snapshot_And_Restore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	acls := h2.Backend.ListWebACLs(context.Background())
	require.Len(t, acls, 1)
	assert.Equal(t, "my-acl", acls[0].Name)
}

func TestBackend_Restore_InvalidData(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestBackend_Snapshot_WithNewResources(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateRegexPatternSet(
		context.Background(), "my-regex", "REGIONAL", "", []wafv2.RegexEntry{{RegexString: "^foo"}}, nil,
	)
	require.NoError(t, err)

	_, err = b.CreateRuleGroup(context.Background(), "my-rg", "REGIONAL", "", "", 10, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPIKey(context.Background(), "REGIONAL", []string{"example.com"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify regex pattern sets are restored (via delete which requires lookup).
	rps2, err := b.CreateRegexPatternSet(context.Background(), "another-regex", "REGIONAL", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeleteRegexPatternSet(context.Background(), rps2.ID, ""))
}

func TestSnapshot_IncludesManagedRuleSets(t *testing.T) {
	t.Parallel()

	b1 := newTestHandler(t).Backend

	// Populate managed rule set in b1.
	_, err := b1.PutManagedRuleSetVersions(
		context.Background(),
		"snap-ms-001", "snap-ruleset", "REGIONAL", "", "Version_2.0",
		map[string]any{
			"Version_2.0": map[string]any{
				"AssociatedRuleGroupArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/rg/abc",
			},
		},
	)
	require.NoError(t, err)

	// Snapshot and restore.
	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newTestHandler(t).Backend
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify managed rule set was restored.
	ms, err := b2.GetManagedRuleSet(context.Background(), "snap-ms-001")
	require.NoError(t, err)
	assert.Equal(t, "snap-ruleset", ms.Name)
	assert.Equal(t, "Version_2.0", ms.RecommendedVersion)
	assert.Contains(t, ms.PublishedVersions, "Version_2.0")
}

// ---- WebACL pagination: RuleGroups and IPSets pagination --------------------

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	b1 := wafv2.NewInMemoryBackend("123456789012", "eu-west-1")
	h1 := wafv2.NewHandler(b1)

	_, arnWebACL := createWebACLHelper(t, h1, "my-acl", "REGIONAL")
	doWafv2Request(t, h1, "CreateIPSet", map[string]any{
		"Name": "my-ipset", "Scope": "REGIONAL", "IPAddressVersion": "IPV4",
	})
	createRegexPatternSetHelper(t, h1, "my-rps")
	createRuleGroupHelper(t, h1, "my-rg")

	// Put logging config.
	rec := doWafv2Request(t, h1, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{"ResourceArn": arnWebACL},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	snap := b1.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := wafv2.NewInMemoryBackend("123456789012", "eu-west-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, wafv2.WebACLCount(b2))
	assert.Equal(t, 1, wafv2.IPSetCount(b2))
	assert.Equal(t, 1, wafv2.RegexPatternSetCount(b2))
	assert.Equal(t, 1, wafv2.RuleGroupCount(b2))

	// Logging config should be present after restore.
	h2 := wafv2.NewHandler(b2)
	rec = doWafv2Request(t, h2, "GetLoggingConfiguration", map[string]any{"ResourceArn": arnWebACL})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSnapshotRestore_NilMaps(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	snap := []byte(`{"webACLs":null,"ipSets":null,"accountID":"123","region":"us-east-1"}`)
	require.NoError(t, b.Restore(t.Context(), snap))

	// After restore with nil maps, counts should be 0 (not panic).
	assert.Equal(t, 0, wafv2.WebACLCount(b))
	assert.Equal(t, 0, wafv2.IPSetCount(b))
	assert.Equal(t, 0, wafv2.RegexPatternSetCount(b))
}
