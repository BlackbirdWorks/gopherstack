package ociidentity_test

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenancy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	adRe := regexp.MustCompile(`^[A-Za-z]{4}:US-ASHBURN-AD-[1-3]$`)

	t.Run("get", func(t *testing.T) {
		t.Parallel()

		r, err := e.client.GetTenancy(t.Context(), identity.GetTenancyRequest{TenancyId: new(testTenancy)})
		require.NoError(t, err)
		assert.Equal(t, testTenancy, *r.Id)
		assert.Equal(t, "IAD", *r.HomeRegionKey)
	})

	t.Run("get unknown", func(t *testing.T) {
		t.Parallel()

		_, err := e.client.GetTenancy(
			t.Context(),
			identity.GetTenancyRequest{TenancyId: new("ocid1.tenancy.oc1..other")},
		)
		se := svcErr(t, err)
		assert.Equal(t, http.StatusNotFound, se.GetHTTPStatusCode())
		assert.Equal(t, "NotAuthorizedOrNotFound", se.GetCode())
	})

	t.Run("regions", func(t *testing.T) {
		t.Parallel()

		r, err := e.client.ListRegions(t.Context())
		require.NoError(t, err)

		got := map[string]string{}
		for _, reg := range r.Items {
			got[*reg.Key] = *reg.Name
		}

		assert.Equal(t, "us-ashburn-1", got["IAD"])
		assert.Equal(t, "eu-frankfurt-1", got["FRA"])
	})

	t.Run("region subscriptions", func(t *testing.T) {
		t.Parallel()

		r, err := e.client.ListRegionSubscriptions(t.Context(),
			identity.ListRegionSubscriptionsRequest{TenancyId: new(testTenancy)})
		require.NoError(t, err)
		require.Len(t, r.Items, 1)
		assert.True(t, *r.Items[0].IsHomeRegion)
		assert.Equal(t, identity.RegionSubscriptionStatusReady, r.Items[0].Status)
	})

	t.Run("availability and fault domains", func(t *testing.T) {
		t.Parallel()

		ads, err := e.client.ListAvailabilityDomains(t.Context(),
			identity.ListAvailabilityDomainsRequest{CompartmentId: new(testTenancy)})
		require.NoError(t, err)
		require.Len(t, ads.Items, 3)

		for _, ad := range ads.Items {
			assert.Regexp(t, adRe, *ad.Name)
			assert.NotEmpty(t, *ad.Id)
		}

		fds, err := e.client.ListFaultDomains(t.Context(), identity.ListFaultDomainsRequest{
			CompartmentId: new(testTenancy), AvailabilityDomain: ads.Items[0].Name,
		})
		require.NoError(t, err)
		require.Len(t, fds.Items, 3)
		assert.Equal(t, "FAULT-DOMAIN-1", *fds.Items[0].Name)

		_, err = e.client.ListFaultDomains(t.Context(), identity.ListFaultDomainsRequest{
			CompartmentId: new(testTenancy), AvailabilityDomain: new("nope:AD-1"),
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
	})
}
