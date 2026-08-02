package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_ResourcePolicy drives family W -- a resource-based IAM-style
// JSON policy document, structurally unrelated to CoreNetworkPolicy despite
// the shared name.
func TestRoundTrip_ResourcePolicy(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)
	resourceArn := aws.ToString(cn.CoreNetwork.CoreNetworkArn)

	// No policy stored yet -- GetResourcePolicy has no ResourceNotFoundException
	// in its real error set (PARITY.md), so this must succeed with an empty document.
	empty, err := client.GetResourcePolicy(
		ctx,
		&networkmanagersdk.GetResourcePolicyInput{ResourceArn: aws.String(resourceArn)},
	)
	require.NoError(t, err)
	require.Empty(t, aws.ToString(empty.PolicyDocument))

	policyDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"networkmanager:GetCoreNetwork","Resource":"*"}]}`

	_, err = client.PutResourcePolicy(ctx, &networkmanagersdk.PutResourcePolicyInput{
		ResourceArn: aws.String(resourceArn), PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)

	got, err := client.GetResourcePolicy(
		ctx,
		&networkmanagersdk.GetResourcePolicyInput{ResourceArn: aws.String(resourceArn)},
	)
	require.NoError(t, err)
	require.JSONEq(t, policyDoc, aws.ToString(got.PolicyDocument))

	_, err = client.DeleteResourcePolicy(
		ctx,
		&networkmanagersdk.DeleteResourcePolicyInput{ResourceArn: aws.String(resourceArn)},
	)
	require.NoError(t, err)

	afterDelete, err := client.GetResourcePolicy(ctx, &networkmanagersdk.GetResourcePolicyInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(afterDelete.PolicyDocument))
}
