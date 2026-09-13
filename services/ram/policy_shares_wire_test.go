package ram_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// TestAssociateResourceShare_CreatedFromPolicyRejected drives a real ram client's
// AssociateResourceShare against a CREATED_FROM_POLICY share. AssociateResourceShare's
// own error model (ram@v1.39.4 deserializers.go
// awsRestjson1_deserializeOpErrorAssociateResourceShare) declares
// InvalidStateTransitionException for exactly this case.
func TestAssociateResourceShare_CreatedFromPolicyRejected(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.PutPolicyBasedShare(
		policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
	))

	shares := backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)

	client := newRoundTripClient(t, ram.NewHandler(backend))

	_, err := client.AssociateResourceShare(t.Context(), &ramsdk.AssociateResourceShareInput{
		ResourceShareArn: aws.String(shares[0].ARN),
		Principals:       []string{"222233334444"},
	})
	require.Error(t, err)

	var apiErr *ramtypes.InvalidStateTransitionException
	require.ErrorAs(t, err, &apiErr, "expected a real InvalidStateTransitionException from the SDK deserializer")
}

// TestUpdateResourceShare_CreatedFromPolicyRejected drives a real ram client's
// UpdateResourceShare against a CREATED_FROM_POLICY share. UpdateResourceShare's own
// error model has no InvalidStateTransitionException at all (ram@v1.39.4
// deserializers.go awsRestjson1_deserializeOpErrorUpdateResourceShare), but does define
// OperationNotPermittedException, the modeled fit for "can't be modified by using RAM".
func TestUpdateResourceShare_CreatedFromPolicyRejected(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.PutPolicyBasedShare(
		policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
	))

	shares := backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)

	client := newRoundTripClient(t, ram.NewHandler(backend))

	_, err := client.UpdateResourceShare(t.Context(), &ramsdk.UpdateResourceShareInput{
		ResourceShareArn: aws.String(shares[0].ARN),
		Name:             aws.String("renamed"),
	})
	require.Error(t, err)

	var apiErr *ramtypes.OperationNotPermittedException
	require.ErrorAs(t, err, &apiErr, "expected a real OperationNotPermittedException from the SDK deserializer")
}

// TestPromoteResourceShareCreatedFromPolicy_AlreadyStandardRejected drives a real ram
// client's PromoteResourceShareCreatedFromPolicy against a share that is already
// STANDARD. The operation's own error model declares InvalidStateTransitionException
// (ram@v1.39.4 deserializers.go
// awsRestjson1_deserializeOpErrorPromoteResourceShareCreatedFromPolicy).
func TestPromoteResourceShareCreatedFromPolicy_AlreadyStandardRejected(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	share, err := backend.CreateResourceShare("already-standard", false, nil, nil, nil)
	require.NoError(t, err)

	client := newRoundTripClient(t, ram.NewHandler(backend))

	_, err = client.PromoteResourceShareCreatedFromPolicy(
		t.Context(),
		&ramsdk.PromoteResourceShareCreatedFromPolicyInput{ResourceShareArn: aws.String(share.ARN)},
	)
	require.Error(t, err)

	var apiErr *ramtypes.InvalidStateTransitionException
	require.ErrorAs(t, err, &apiErr, "expected a real InvalidStateTransitionException from the SDK deserializer")
}

// TestPromoteResourceShareCreatedFromPolicy_Succeeds drives a real ram client's full
// create-from-policy -> promote -> STANDARD lifecycle, proving the featureSet state
// machine gopherstack-kvyy filed as unreachable is now real end to end.
func TestPromoteResourceShareCreatedFromPolicy_Succeeds(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.PutPolicyBasedShare(
		policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
	))

	shares := backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)

	client := newRoundTripClient(t, ram.NewHandler(backend))

	got, err := client.GetResourceShares(t.Context(), &ramsdk.GetResourceSharesInput{
		ResourceOwner:     ramtypes.ResourceOwnerSelf,
		ResourceShareArns: []string{shares[0].ARN},
	})
	require.NoError(t, err)
	require.Len(t, got.ResourceShares, 1)
	require.Equal(t, ramtypes.ResourceShareFeatureSetCreatedFromPolicy, got.ResourceShares[0].FeatureSet)

	_, err = client.PromoteResourceShareCreatedFromPolicy(
		t.Context(),
		&ramsdk.PromoteResourceShareCreatedFromPolicyInput{ResourceShareArn: aws.String(shares[0].ARN)},
	)
	require.NoError(t, err)

	got, err = client.GetResourceShares(t.Context(), &ramsdk.GetResourceSharesInput{
		ResourceOwner:     ramtypes.ResourceOwnerSelf,
		ResourceShareArns: []string{shares[0].ARN},
	})
	require.NoError(t, err)
	require.Len(t, got.ResourceShares, 1)
	require.Equal(t, ramtypes.ResourceShareFeatureSetStandard, got.ResourceShares[0].FeatureSet)
}
