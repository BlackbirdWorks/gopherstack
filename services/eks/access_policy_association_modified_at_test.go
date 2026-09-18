package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestAssociatedAccessPolicy_ModifiedAt_RealClient drives
// AssociateAccessPolicy/ListAssociatedAccessPolicies through the real
// aws-sdk-go-v2 client. types.AssociatedAccessPolicy.ModifiedAt (eks@v1.98.0
// deserializers.go, awsRestjson1_deserializeDocumentAssociatedAccessPolicy,
// case "modifiedAt") was never emitted by either op -- AccessPolicyAssociation
// had no backing field for it at all.
func TestAssociatedAccessPolicy_ModifiedAt_RealClient(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:    aws.String("policy-modifiedat-cluster"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
			SubnetIds: []string{"subnet-abc123"},
		},
	})
	require.NoError(t, err)

	const principal = "arn:aws:iam::123456789012:role/dev"

	_, err = client.CreateAccessEntry(ctx, &ekssdk.CreateAccessEntryInput{
		ClusterName:  aws.String("policy-modifiedat-cluster"),
		PrincipalArn: aws.String(principal),
	})
	require.NoError(t, err)

	assoc, err := client.AssociateAccessPolicy(ctx, &ekssdk.AssociateAccessPolicyInput{
		ClusterName:  aws.String("policy-modifiedat-cluster"),
		PrincipalArn: aws.String(principal),
		PolicyArn:    aws.String("arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy"),
		AccessScope:  &ekstypes.AccessScope{Type: ekstypes.AccessScopeTypeCluster},
	})
	require.NoError(t, err)
	require.NotNil(t, assoc.AssociatedAccessPolicy.ModifiedAt)
	assert.False(t, assoc.AssociatedAccessPolicy.ModifiedAt.IsZero())
	assert.Equal(t, *assoc.AssociatedAccessPolicy.AssociatedAt, *assoc.AssociatedAccessPolicy.ModifiedAt)

	listed, err := client.ListAssociatedAccessPolicies(ctx, &ekssdk.ListAssociatedAccessPoliciesInput{
		ClusterName:  aws.String("policy-modifiedat-cluster"),
		PrincipalArn: aws.String(principal),
	})
	require.NoError(t, err)
	require.Len(t, listed.AssociatedAccessPolicies, 1)
	require.NotNil(t, listed.AssociatedAccessPolicies[0].ModifiedAt)
	assert.False(t, listed.AssociatedAccessPolicies[0].ModifiedAt.IsZero())
}
