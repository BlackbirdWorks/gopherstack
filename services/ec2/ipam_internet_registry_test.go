package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// createTestIpamAndAssociation creates a fresh IPAM and an internet registry association
// against it, returning the IPAM ID and the full created association.
func createTestIpamAndAssociation(
	t *testing.T, client *ec2sdk.Client,
) (string, *types.IpamInternetRegistryAssociation) {
	t.Helper()

	ipamOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
	require.NoError(t, err)

	createOut, err := client.CreateIpamInternetRegistryAssociation(
		t.Context(), &ec2sdk.CreateIpamInternetRegistryAssociationInput{
			IpamId:             ipamOut.Ipam.IpamId,
			OrganizationHandle: aws.String("ORG-HANDLE"),
			Rir:                types.RirArin,
			Description:        aws.String("test association"),
		},
	)
	require.NoError(t, err)

	return aws.ToString(ipamOut.Ipam.IpamId), createOut.IpamInternetRegistryAssociation
}

// createTestIpamInternetRegistryAssociation creates an IPAM and an internet registry
// association against it, returning the association ID.
func createTestIpamInternetRegistryAssociation(t *testing.T, client *ec2sdk.Client) string {
	t.Helper()

	_, assoc := createTestIpamAndAssociation(t, client)

	return aws.ToString(assoc.IpamInternetRegistryAssociationId)
}

func TestIpamInternetRegistryAssociation_CRUD(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	ipamID, assoc := createTestIpamAndAssociation(t, client)
	assocID := aws.ToString(assoc.IpamInternetRegistryAssociationId)
	assert.NotEmpty(t, assocID)
	assert.Equal(t, ipamID, aws.ToString(assoc.IpamId))
	assert.Equal(t, types.RirArin, assoc.Rir)
	assert.Equal(t, "ORG-HANDLE", aws.ToString(assoc.OrganizationHandle))
	assert.Equal(t, types.IpamInternetRegistryAssociationStatePendingEnable, assoc.State)

	t.Run("describe by id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeIpamInternetRegistryAssociations(
			t.Context(), &ec2sdk.DescribeIpamInternetRegistryAssociationsInput{
				IpamInternetRegistryAssociationIds: []string{assocID},
			},
		)
		require.NoError(t, err)
		require.Len(t, out.IpamInternetRegistryAssociations, 1)
		gotID := aws.ToString(out.IpamInternetRegistryAssociations[0].IpamInternetRegistryAssociationId)
		assert.Equal(t, assocID, gotID)
	})

	t.Run("filter by rir excludes non-matching", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeIpamInternetRegistryAssociations(
			t.Context(), &ec2sdk.DescribeIpamInternetRegistryAssociationsInput{
				Filters: []types.Filter{{Name: aws.String("rir"), Values: []string{"ripe"}}},
			},
		)
		require.NoError(t, err)

		for _, a := range out.IpamInternetRegistryAssociations {
			assert.NotEqual(t, assocID, aws.ToString(a.IpamInternetRegistryAssociationId))
		}
	})

	t.Run("filter by matching rir includes it", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeIpamInternetRegistryAssociations(
			t.Context(), &ec2sdk.DescribeIpamInternetRegistryAssociationsInput{
				Filters: []types.Filter{{Name: aws.String("rir"), Values: []string{"arin"}}},
			},
		)
		require.NoError(t, err)

		var found bool

		for _, a := range out.IpamInternetRegistryAssociations {
			if aws.ToString(a.IpamInternetRegistryAssociationId) == assocID {
				found = true
			}
		}

		assert.True(t, found)
	})

	t.Run("asns and cidrs are empty before any routing policy registration", func(t *testing.T) {
		t.Parallel()

		asnsOut, err := client.GetIpamInternetRegistryAssociationAsns(
			t.Context(), &ec2sdk.GetIpamInternetRegistryAssociationAsnsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, asnsOut.IpamInternetRegistryAssociationAsns)

		cidrsOut, err := client.GetIpamInternetRegistryAssociationCidrs(
			t.Context(), &ec2sdk.GetIpamInternetRegistryAssociationCidrsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, cidrsOut.IpamInternetRegistryAssociationCidrs)
	})

	t.Run("create requires a valid rir", func(t *testing.T) {
		t.Parallel()

		_, err := client.CreateIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.CreateIpamInternetRegistryAssociationInput{
				IpamId:             aws.String(ipamID),
				OrganizationHandle: aws.String("ORG-HANDLE"),
				Rir:                "not-a-real-rir",
			},
		)
		require.Error(t, err)
	})

	t.Run("enable completes bpki setup", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)

		out, err := client.EnableIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.EnableIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				ChildHandle:                       aws.String("child"),
				ParentBpkiTa:                      aws.String("ta"),
				ParentHandle:                      aws.String("parent"),
				RpkiVersion:                       aws.String("4"),
				ServiceUri:                        aws.String("https://example.com/rpki"),
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			types.IpamInternetRegistryAssociationStateEnableComplete,
			out.IpamInternetRegistryAssociation.State,
		)

		// Enabling again fails: no longer pending-enable.
		_, err = client.EnableIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.EnableIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				ChildHandle:                       aws.String("child"),
				ParentBpkiTa:                      aws.String("ta"),
				ParentHandle:                      aws.String("parent"),
				RpkiVersion:                       aws.String("4"),
				ServiceUri:                        aws.String("https://example.com/rpki"),
			},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "IncorrectState", apiErr.ErrorCode())
	})

	t.Run("enable requires all bpki fields", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)

		_, err := client.EnableIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.EnableIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				ChildHandle:                       aws.String("child"),
			},
		)
		require.Error(t, err)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.DeleteIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String("ipam-ira-doesnotexist"),
			},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidIpamInternetRegistryAssociationId.NotFound", apiErr.ErrorCode())
	})

	t.Run("delete then not found", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)

		delOut, err := client.DeleteIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.DeleteIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			types.IpamInternetRegistryAssociationStateDeleteComplete,
			delOut.IpamInternetRegistryAssociation.State,
		)

		_, err = client.DeleteIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.DeleteIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
			},
		)
		require.Error(t, err)
	})

	t.Run("pagination via max results and next token", func(t *testing.T) {
		t.Parallel()

		bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		hh := ec2.NewHandler(bk)
		c := newTestEC2Client(t, hh)

		const seedCount = 3
		for range seedCount {
			createTestIpamInternetRegistryAssociation(t, c)
		}

		firstPage, err := c.DescribeIpamInternetRegistryAssociations(
			t.Context(), &ec2sdk.DescribeIpamInternetRegistryAssociationsInput{MaxResults: aws.Int32(1)},
		)
		require.NoError(t, err)
		assert.Len(t, firstPage.IpamInternetRegistryAssociations, 1)
		require.NotNil(t, firstPage.NextToken)

		secondPage, err := c.DescribeIpamInternetRegistryAssociations(
			t.Context(), &ec2sdk.DescribeIpamInternetRegistryAssociationsInput{
				MaxResults: aws.Int32(1), NextToken: firstPage.NextToken,
			},
		)
		require.NoError(t, err)
		assert.Len(t, secondPage.IpamInternetRegistryAssociations, 1)
		assert.NotEqual(
			t,
			aws.ToString(firstPage.IpamInternetRegistryAssociations[0].IpamInternetRegistryAssociationId),
			aws.ToString(secondPage.IpamInternetRegistryAssociations[0].IpamInternetRegistryAssociationId),
		)
	})
}
