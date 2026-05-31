package waf_test

import (
	"testing"

	wafsdk "github.com/aws/aws-sdk-go-v2/service/waf"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/waf"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// waf client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := waf.NewInMemoryBackend("000000000000", "us-east-1")
	h := waf.NewHandler(backend)

	// Operations not yet implemented — rate-based rules, regex sets, rule groups,
	// logging, permission policy, subscribed rule groups, and migration stack.
	notImplemented := []string{
		"CreateRateBasedRule",
		"CreateRegexMatchSet",
		"CreateRegexPatternSet",
		"CreateRuleGroup",
		"CreateWebACLMigrationStack",
		"DeleteLoggingConfiguration",
		"DeletePermissionPolicy",
		"DeleteRateBasedRule",
		"DeleteRegexMatchSet",
		"DeleteRegexPatternSet",
		"DeleteRuleGroup",
		"GetLoggingConfiguration",
		"GetPermissionPolicy",
		"GetRateBasedRule",
		"GetRateBasedRuleManagedKeys",
		"GetRegexMatchSet",
		"GetRegexPatternSet",
		"GetRuleGroup",
		"ListActivatedRulesInRuleGroup",
		"ListLoggingConfigurations",
		"ListRateBasedRules",
		"ListRegexMatchSets",
		"ListRegexPatternSets",
		"ListRuleGroups",
		"ListSubscribedRuleGroups",
		"PutLoggingConfiguration",
		"PutPermissionPolicy",
		"UpdateRateBasedRule",
		"UpdateRegexMatchSet",
		"UpdateRegexPatternSet",
		"UpdateRuleGroup",
	}

	sdkcheck.CheckCompleteness(t, &wafsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
