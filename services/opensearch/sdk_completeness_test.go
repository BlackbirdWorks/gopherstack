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
	}

	var domainOps, slOps []string
	for _, op := range h.GetSupportedOperations() {
		if serverlessOps[op] {
			slOps = append(slOps, op)
		} else {
			domainOps = append(domainOps, op)
		}
	}

	sdkcheck.CheckCompleteness(t, &opensearchsdk.Client{}, domainOps, []string{})
	// This Handler implements the collection/access-policy/security-config/
	// security-policy/tagging slice of AOSS plus (gopherstack parity sweep
	// 2026-09-19) lifecycle policies, collection groups, GetAccountSettings/
	// UpdateAccountSettings, GetPoliciesStats, and BatchGetVpcEndpoint (a
	// read against the classic-domain VPC endpoint store this package
	// already maintains, vpc_endpoints.go). The document-plane Index family
	// (Create/Get/Update/DeleteIndex) and the full VPC-endpoint write/list
	// surface (Create/List/Update/DeleteVpcEndpoint) and UpdateCollection
	// (moving a collection into a collection group) remain unimplemented.
	sdkcheck.CheckCompleteness(t, &opensearchserverlesssdk.Client{}, slOps, []string{
		"CreateIndex",
		"CreateVpcEndpoint",
		"DeleteIndex",
		"DeleteVpcEndpoint",
		"GetIndex",
		"ListVpcEndpoints",
		"UpdateCollection",
		"UpdateIndex",
		"UpdateVpcEndpoint",
	})
}
