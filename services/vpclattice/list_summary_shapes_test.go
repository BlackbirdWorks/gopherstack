package vpclattice_test

import (
	"net/http"
	"testing"

	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaries_MemberRoundTrips drives each op's Create then List
// through the real aws-sdk-go-v2 client and asserts a member that was
// missing from the List summary (over-wide response class, gopherstack-dv4s)
// now round-trips.
func TestListSummaries_MemberRoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("domain verification tags", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestVPCLatticeClient(t, h)

		rec := doRequest(t, h, http.MethodPost, "/domainverifications", map[string]any{
			"domainName": "example-tags.com",
			"tags":       map[string]string{"team": "networking"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		listed, err := client.ListDomainVerifications(t.Context(), &vpclatticesdk.ListDomainVerificationsInput{})
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.Equal(t, map[string]string{"team": "networking"}, listed.Items[0].Tags)
	})

	t.Run("service network resource association createdBy and privateDnsEnabled", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestVPCLatticeClient(t, h)

		snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snra"})
		require.Equal(t, http.StatusCreated, snRec.Code)
		snID, _ := parseBody(t, snRec)["id"].(string)

		rcRec := doRequest(t, h, http.MethodPost, "/resourceconfigurations", map[string]any{
			"name": "rc-snra",
			"type": "ARN",
			"resourceConfigurationDefinition": map[string]any{
				"arnResource": map[string]any{"arn": "arn:aws:rds:us-east-1:000000000000:db:snra"},
			},
		})
		require.Equal(t, http.StatusCreated, rcRec.Code)
		rcID, _ := parseBody(t, rcRec)["id"].(string)

		assocRec := doRequest(t, h, http.MethodPost, "/servicenetworkresourceassociations", map[string]any{
			"serviceNetworkIdentifier":        snID,
			"resourceConfigurationIdentifier": rcID,
			"privateDnsEnabled":               true,
		})
		require.Equal(t, http.StatusCreated, assocRec.Code)

		listed, err := client.ListServiceNetworkResourceAssociations(
			t.Context(), &vpclatticesdk.ListServiceNetworkResourceAssociationsInput{},
		)
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.NotEmpty(t, listed.Items[0].CreatedBy)
		require.NotNil(t, listed.Items[0].PrivateDnsEnabled)
		assert.True(t, *listed.Items[0].PrivateDnsEnabled)
	})

	t.Run("service network service association createdBy", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestVPCLatticeClient(t, h)

		snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snsa"})
		require.Equal(t, http.StatusCreated, snRec.Code)
		snID, _ := parseBody(t, snRec)["id"].(string)

		svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-snsa"})
		require.Equal(t, http.StatusCreated, svcRec.Code)
		svcID, _ := parseBody(t, svcRec)["id"].(string)

		assocRec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
			"serviceNetworkIdentifier": snID,
			"serviceIdentifier":        svcID,
		})
		require.Equal(t, http.StatusCreated, assocRec.Code)

		listed, err := client.ListServiceNetworkServiceAssociations(
			t.Context(), &vpclatticesdk.ListServiceNetworkServiceAssociationsInput{},
		)
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.NotEmpty(t, listed.Items[0].CreatedBy)
	})

	t.Run("service network vpc association createdBy and lastUpdatedAt", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestVPCLatticeClient(t, h)

		snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snva"})
		require.Equal(t, http.StatusCreated, snRec.Code)
		snID, _ := parseBody(t, snRec)["id"].(string)

		assocRec := doRequest(t, h, http.MethodPost, "/servicenetworkvpcassociations", map[string]any{
			"serviceNetworkIdentifier": snID,
			"vpcIdentifier":            "vpc-snva",
		})
		require.Equal(t, http.StatusCreated, assocRec.Code)

		listed, err := client.ListServiceNetworkVpcAssociations(
			t.Context(), &vpclatticesdk.ListServiceNetworkVpcAssociationsInput{},
		)
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.NotEmpty(t, listed.Items[0].CreatedBy)
		require.NotNil(t, listed.Items[0].LastUpdatedAt)
		assert.False(t, listed.Items[0].LastUpdatedAt.IsZero())
	})

	t.Run("service network lastUpdatedAt", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestVPCLatticeClient(t, h)

		snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-lastupdated"})
		require.Equal(t, http.StatusCreated, snRec.Code)

		listed, err := client.ListServiceNetworks(t.Context(), &vpclatticesdk.ListServiceNetworksInput{})
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		require.NotNil(t, listed.Items[0].LastUpdatedAt)
		assert.False(t, listed.Items[0].LastUpdatedAt.IsZero())
	})
}
