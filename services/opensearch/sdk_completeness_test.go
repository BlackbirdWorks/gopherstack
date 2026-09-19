package opensearch_test

import (
	"testing"

	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchserverlesssdk "github.com/aws/aws-sdk-go-v2/service/opensearchserverless"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// opensearch client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := opensearch.NewHandler(backend)

	// serverlessOps are the OpenSearch Serverless (AOSS) operations. AWS
	// models these on a separate SDK client, opensearchserverless.Client,
	// distinct from the "classic" managed-domain client
	// (opensearchsdk.Client) checked below. gopherstack's single Handler
	// implements both surfaces and reports them together from
	// GetSupportedOperations(), so this test splits them before checking
	// each half against the SDK client that actually owns it. See
	// serverlessOperations() in handler_operations.go for the full list.
	serverlessOps := map[string]bool{
		"BatchGetCollection":               true,
		"CreateAccessPolicy":               true,
		"CreateCollection":                 true,
		"CreateSecurityConfig":             true,
		"CreateSecurityPolicy":             true,
		"DeleteAccessPolicy":               true,
		"DeleteCollection":                 true,
		"DeleteSecurityConfig":             true,
		"DeleteSecurityPolicy":             true,
		"GetAccessPolicy":                  true,
		"GetSecurityConfig":                true,
		"GetSecurityPolicy":                true,
		"ListAccessPolicies":               true,
		"ListCollections":                  true,
		"ListSecurityConfigs":              true,
		"ListSecurityPolicies":             true,
		"ListTagsForResource":              true,
		"TagResource":                      true,
		"UntagResource":                    true,
		"UpdateAccessPolicy":               true,
		"UpdateSecurityConfig":             true,
		"UpdateSecurityPolicy":             true,
		"CreateLifecyclePolicy":            true,
		"UpdateLifecyclePolicy":            true,
		"DeleteLifecyclePolicy":            true,
		"ListLifecyclePolicies":            true,
		"BatchGetLifecyclePolicy":          true,
		"BatchGetEffectiveLifecyclePolicy": true,
		"CreateCollectionGroup":            true,
		"UpdateCollectionGroup":            true,
		"DeleteCollectionGroup":            true,
		"ListCollectionGroups":             true,
		"BatchGetCollectionGroup":          true,
		"BatchGetVpcEndpoint":              true,
		"GetAccountSettings":               true,
		"UpdateAccountSettings":            true,
		"GetPoliciesStats":                 true,
		"UpdateCollection":                 true,
	}

	// dualSurfaceOps lists operation names that are real, independently
	// implemented operations on BOTH the classic opensearch.Client and the
	// opensearchserverless.Client (gopherstack parity sweep 2026-09-19:
	// CreateIndex/GetIndex/UpdateIndex/DeleteIndex and CreateVpcEndpoint/
	// ListVpcEndpoints/UpdateVpcEndpoint/DeleteVpcEndpoint each name a real,
	// distinct op on both SDK clients -- verified by listing api_op_*.go in
	// both service directories). GetSupportedOperations() reports each such
	// name once (it's already there from domainOperations()/
	// connectionAndTagOperations()/infraAndAppOperations()); dispatch
	// distinguishes the two real wire protocols via the X-Amz-Target header
	// (ExtractOperation, handler_operations.go), not by name. Both
	// CheckCompleteness calls below need to see the name accounted for, so
	// each dual-surface op is added to both derived lists without needing a
	// second, duplicate entry in GetSupportedOperations() itself (which
	// would fail CheckCompleteness's own no-duplicates assertion).
	dualSurfaceOps := map[string]bool{
		"CreateIndex":       true,
		"GetIndex":          true,
		"UpdateIndex":       true,
		"DeleteIndex":       true,
		"CreateVpcEndpoint": true,
		"ListVpcEndpoints":  true,
		"UpdateVpcEndpoint": true,
		"DeleteVpcEndpoint": true,
	}

	var domainOps, slOps []string
	for _, op := range h.GetSupportedOperations() {
		if serverlessOps[op] || dualSurfaceOps[op] {
			slOps = append(slOps, op)
		}

		if !serverlessOps[op] || dualSurfaceOps[op] {
			domainOps = append(domainOps, op)
		}
	}

	sdkcheck.CheckCompleteness(t, &opensearchsdk.Client{}, domainOps, []string{})
	// This Handler implements the full AOSS surface it advertises: the
	// collection/access-policy/security-config/security-policy/tagging slice,
	// lifecycle policies, collection groups, GetAccountSettings/
	// UpdateAccountSettings, GetPoliciesStats, BatchGetVpcEndpoint (reads the
	// AOSS-native VPC endpoint store first, falling back to the classic-domain
	// one), the Index family, the native VPC-endpoint write/list family, and
	// UpdateCollection (gopherstack parity sweep 2026-09-19) -- no
	// opensearchserverless.Client operation remains unimplemented.
	sdkcheck.CheckCompleteness(t, &opensearchserverlesssdk.Client{}, slOps, []string{})
}
