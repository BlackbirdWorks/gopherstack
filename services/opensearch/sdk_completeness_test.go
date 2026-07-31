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
		"BatchGetCollection":   true,
		"CreateAccessPolicy":   true,
		"CreateCollection":     true,
		"CreateSecurityConfig": true,
		"CreateSecurityPolicy": true,
		"DeleteAccessPolicy":   true,
		"DeleteCollection":     true,
		"DeleteSecurityConfig": true,
		"DeleteSecurityPolicy": true,
		"GetAccessPolicy":      true,
		"GetSecurityConfig":    true,
		"GetSecurityPolicy":    true,
		"ListAccessPolicies":   true,
		"ListCollections":      true,
		"ListSecurityConfigs":  true,
		"ListSecurityPolicies": true,
		"UpdateAccessPolicy":   true,
		"UpdateSecurityConfig": true,
		"UpdateSecurityPolicy": true,
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
	// This Handler only implements the collection/access-policy/security-
	// config/security-policy slice of AOSS. The rest of opensearchserverless.Client
	// (collection groups, indices, lifecycle policies, VPC endpoints, account
	// settings, tagging) is not implemented.
	sdkcheck.CheckCompleteness(t, &opensearchserverlesssdk.Client{}, slOps, []string{
		"BatchGetCollectionGroup",
		"BatchGetEffectiveLifecyclePolicy",
		"BatchGetLifecyclePolicy",
		"BatchGetVpcEndpoint",
		"CreateCollectionGroup",
		"CreateIndex",
		"CreateLifecyclePolicy",
		"CreateVpcEndpoint",
		"DeleteCollectionGroup",
		"DeleteIndex",
		"DeleteLifecyclePolicy",
		"DeleteVpcEndpoint",
		"GetAccountSettings",
		"GetIndex",
		"GetPoliciesStats",
		"ListCollectionGroups",
		"ListLifecyclePolicies",
		"ListTagsForResource",
		"ListVpcEndpoints",
		"TagResource",
		"UntagResource",
		"UpdateAccountSettings",
		"UpdateCollection",
		"UpdateCollectionGroup",
		"UpdateIndex",
		"UpdateLifecyclePolicy",
		"UpdateVpcEndpoint",
	})
}
