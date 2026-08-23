package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/require"
)

// TestBatchGetPolicy_UnresolvableAlias_ErrorCode proves BatchGetPolicy
// reports POLICY_STORE_ALIAS_NOT_FOUND, not POLICY_STORE_NOT_FOUND, when a
// per-item policyStoreId is an alias name that doesn't resolve. Real AWS's
// BatchGetPolicyErrorCode enum declares POLICY_STORE_ALIAS_NOT_FOUND
// specifically for this case (verifiedpermissions@v1.36.4:
// types/enums.go:30, distinct from POLICY_STORE_NOT_FOUND at line 28), and
// gopherstack's handler can always tell the two cases apart: its
// resolvePolicyStoreID only returns an error when the input string carried
// the policy-store-alias/ prefix and resolution failed -- a bare (non-alias)
// policyStoreId is passed through unchanged and only ever fails not-found
// inside the backend's own item loop, which already reports
// POLICY_STORE_NOT_FOUND correctly (policies.go).
func TestBatchGetPolicy_UnresolvableAlias_ErrorCode(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	out, err := client.BatchGetPolicy(ctx, &avpsdk.BatchGetPolicyInput{
		Requests: []types.BatchGetPolicyInputItem{
			{
				PolicyStoreId: aws.String("policy-store-alias/does-not-exist"),
				PolicyId:      aws.String("SPEXAMPLEabcdefg111111"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Errors, 1)
	require.Equal(t, types.BatchGetPolicyErrorCodePolicyStoreAliasNotFound, out.Errors[0].Code)
}
