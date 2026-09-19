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

// createTestRoutingPolicyRegistration creates a routing policy registration for the given
// association and CIDR, returning the resulting delta.
func createTestRoutingPolicyRegistration(
	t *testing.T, client *ec2sdk.Client, assocID, cidr string, asns []string,
) *types.IpamRoutingPolicyRegistrationDelta {
	t.Helper()

	out, err := client.CreateIpamRoutingPolicyRegistration(
		t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
			IpamInternetRegistryAssociationId: aws.String(assocID),
			Cidr:                              aws.String(cidr),
			Asns:                              asns,
		},
	)
	require.NoError(t, err)

	return out.IpamRoutingPolicyRegistrationDelta
}

func TestIpamRoutingPolicyRegistration_CRUD(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)
	assocID := createTestIpamInternetRegistryAssociation(t, client)

	delta := createTestRoutingPolicyRegistration(t, client, assocID, "203.0.113.0/24", []string{"64512"})
	assert.NotEmpty(t, aws.ToString(delta.DeltaId))
	assert.Equal(t, "published", string(delta.State))

	t.Run("get registrations lists it", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.IpamRoutingPolicyRegistrations, 1)
		assert.Equal(t, "203.0.113.0/24", aws.ToString(out.IpamRoutingPolicyRegistrations[0].Cidr))
		assert.Equal(t, []string{"64512"}, out.IpamRoutingPolicyRegistrations[0].Asns)
	})

	t.Run("get registrations filters by cidr", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
				Cidr:                              aws.String("198.51.100.0/24"),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, out.IpamRoutingPolicyRegistrations)
	})

	t.Run("asns and cidrs now reflect the registration", func(t *testing.T) {
		t.Parallel()

		asnsOut, err := client.GetIpamInternetRegistryAssociationAsns(
			t.Context(), &ec2sdk.GetIpamInternetRegistryAssociationAsnsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		require.Len(t, asnsOut.IpamInternetRegistryAssociationAsns, 1)
		assert.Equal(t, "64512", aws.ToString(asnsOut.IpamInternetRegistryAssociationAsns[0].Asn))

		cidrsOut, err := client.GetIpamInternetRegistryAssociationCidrs(
			t.Context(), &ec2sdk.GetIpamInternetRegistryAssociationCidrsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		require.Len(t, cidrsOut.IpamInternetRegistryAssociationCidrs, 1)
		assert.Equal(t, "203.0.113.0/24", aws.ToString(cidrsOut.IpamInternetRegistryAssociationCidrs[0].Cidr))
	})

	t.Run("route origin authorizations reflect the registration", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetIpamRouteOriginAuthorizations(
			t.Context(), &ec2sdk.GetIpamRouteOriginAuthorizationsInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.IpamRouteOriginAuthorizations, 1)
		assert.Equal(t, "64512", aws.ToString(out.IpamRouteOriginAuthorizations[0].Asn))
		assert.Equal(t, "203.0.113.0/24", aws.ToString(out.IpamRouteOriginAuthorizations[0].Cidr))
	})

	t.Run("create duplicate cidr fails", func(t *testing.T) {
		t.Parallel()

		_, err := client.CreateIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
				Cidr:                              aws.String("203.0.113.0/24"),
				Asns:                              []string{"64513"},
			},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "IncorrectState", apiErr.ErrorCode())
	})

	t.Run("modify updates asns and description", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)
		_, err := client.CreateIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
				Asns:                              []string{"64512"},
			},
		)
		require.NoError(t, err)

		modOut, err := client.ModifyIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.ModifyIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
				Asns:                              []string{"64512", "64513"},
				Description:                       aws.String("modified"),
			},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(modOut.IpamRoutingPolicyRegistrationDelta.DeltaId))

		out, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.IpamRoutingPolicyRegistrations, 1)
		assert.ElementsMatch(t, []string{"64512", "64513"}, out.IpamRoutingPolicyRegistrations[0].Asns)
		assert.Equal(t, "modified", aws.ToString(out.IpamRoutingPolicyRegistrations[0].Description))
	})

	t.Run("modify not found", func(t *testing.T) {
		t.Parallel()

		_, err := client.ModifyIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.ModifyIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
				Cidr:                              aws.String("192.0.2.0/24"),
				Asns:                              []string{"64512"},
			},
		)
		require.Error(t, err)
	})

	t.Run("delta history records the create", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetIpamRoutingPolicyRegistrationDeltas(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationDeltasInput{
				IpamInternetRegistryAssociationId: aws.String(assocID),
			},
		)
		require.NoError(t, err)
		require.NotEmpty(t, out.IpamRoutingPolicyRegistrationDeltas)
	})

	t.Run("batch modify creates and deletes in one call", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)
		_, err := client.CreateIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
				Asns:                              []string{"64512"},
			},
		)
		require.NoError(t, err)

		batchDoc := `[
			{"action":"delete","cidr":"203.0.113.0/24"},
			{"action":"create","cidr":"198.51.100.0/24","asns":["64514"]}
		]`

		batchOut, err := client.BatchModifyIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.BatchModifyIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				DeltaJson:                         aws.String(batchDoc),
			},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(batchOut.IpamRoutingPolicyRegistrationDelta.DeltaId))

		out, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.IpamRoutingPolicyRegistrations, 1)
		assert.Equal(t, "198.51.100.0/24", aws.ToString(out.IpamRoutingPolicyRegistrations[0].Cidr))
	})

	t.Run("delete then get is empty", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)
		_, err := client.CreateIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
				Asns:                              []string{"64512"},
			},
		)
		require.NoError(t, err)

		delOut, err := client.DeleteIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.DeleteIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
			},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(delOut.IpamRoutingPolicyRegistrationDelta.DeltaId))

		out, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, out.IpamRoutingPolicyRegistrations)

		// Deleting again: not found.
		_, err = client.DeleteIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.DeleteIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
			},
		)
		require.Error(t, err)
	})

	t.Run("delete blocks internet registry association deletion", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)
		_, err := client.CreateIpamRoutingPolicyRegistration(
			t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				Cidr:                              aws.String("203.0.113.0/24"),
				Asns:                              []string{"64512"},
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteIpamInternetRegistryAssociation(
			t.Context(), &ec2sdk.DeleteIpamInternetRegistryAssociationInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
			},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "DependencyViolation", apiErr.ErrorCode())
	})

	t.Run("pagination via max results and next token", func(t *testing.T) {
		t.Parallel()

		myAssocID := createTestIpamInternetRegistryAssociation(t, client)
		cidrs := []string{"203.0.113.0/24", "198.51.100.0/24", "192.0.2.0/24"}

		for i, cidr := range cidrs {
			_, err := client.CreateIpamRoutingPolicyRegistration(
				t.Context(), &ec2sdk.CreateIpamRoutingPolicyRegistrationInput{
					IpamInternetRegistryAssociationId: aws.String(myAssocID),
					Cidr:                              aws.String(cidr),
					Asns:                              []string{"64512"},
				},
			)
			require.NoErrorf(t, err, "seed %d", i)
		}

		firstPage, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID), MaxResults: aws.Int32(1),
			},
		)
		require.NoError(t, err)
		assert.Len(t, firstPage.IpamRoutingPolicyRegistrations, 1)
		require.NotNil(t, firstPage.NextToken)

		secondPage, err := client.GetIpamRoutingPolicyRegistrations(
			t.Context(), &ec2sdk.GetIpamRoutingPolicyRegistrationsInput{
				IpamInternetRegistryAssociationId: aws.String(myAssocID),
				MaxResults:                        aws.Int32(1),
				NextToken:                         firstPage.NextToken,
			},
		)
		require.NoError(t, err)
		assert.Len(t, secondPage.IpamRoutingPolicyRegistrations, 1)
		assert.NotEqual(
			t,
			aws.ToString(firstPage.IpamRoutingPolicyRegistrations[0].Cidr),
			aws.ToString(secondPage.IpamRoutingPolicyRegistrations[0].Cidr),
		)
	})
}

// TestGetIpamDiscoveredRoutes_StructuralGap covers the "no BGP discovery pipeline modeled"
// gap: a valid resource discovery ID always returns an empty, correctly-shaped result, and an
// unknown ID surfaces the real not-found error rather than a fabricated route.
func TestGetIpamDiscoveredRoutes_StructuralGap(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	ipamOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
	require.NoError(t, err)

	out, err := client.GetIpamDiscoveredRoutes(t.Context(), &ec2sdk.GetIpamDiscoveredRoutesInput{
		IpamResourceDiscoveryId: ipamOut.Ipam.DefaultResourceDiscoveryId,
		ResourceRegion:          aws.String("us-east-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.IpamDiscoveredRoutes)

	_, err = client.GetIpamDiscoveredRoutes(t.Context(), &ec2sdk.GetIpamDiscoveredRoutesInput{
		IpamResourceDiscoveryId: aws.String("ipam-res-disco-doesnotexist"),
		ResourceRegion:          aws.String("us-east-1"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidIpamResourceDiscoveryId.NotFound", apiErr.ErrorCode())
}

// TestGetIpamRouteProtectionFindings_StructuralGap covers the "no RPKI validation pipeline
// modeled" gap: a valid IPAM ID always returns an empty, correctly-shaped result, and an
// unknown ID surfaces the real not-found error rather than a fabricated finding.
func TestGetIpamRouteProtectionFindings_StructuralGap(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	ipamOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
	require.NoError(t, err)

	out, err := client.GetIpamRouteProtectionFindings(t.Context(), &ec2sdk.GetIpamRouteProtectionFindingsInput{
		IpamId: ipamOut.Ipam.IpamId,
	})
	require.NoError(t, err)
	assert.Empty(t, out.RouteProtectionFindings)
	assert.Equal(t, aws.ToString(ipamOut.Ipam.IpamId), aws.ToString(out.IpamId))

	_, err = client.GetIpamRouteProtectionFindings(t.Context(), &ec2sdk.GetIpamRouteProtectionFindingsInput{
		IpamId: aws.String("ipam-doesnotexist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidIpamId.NotFound", apiErr.ErrorCode())
}
