package wafv2_test

import (
	"context"
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

	rg, err := b.CreateRuleGroup(ctx, "full-state-rg", wafv2.ScopeRegional, "desc", "{}", 10, nil, nil)
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
