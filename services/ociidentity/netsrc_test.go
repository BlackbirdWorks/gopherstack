package ociidentity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func (e *env) mkNetworkSource(t *testing.T, name string) identity.NetworkSources {
	t.Helper()

	r, err := e.client.CreateNetworkSource(t.Context(), identity.CreateNetworkSourceRequest{
		CompartmentId: new(testTenancy), Name: new(name), Description: new("d"),
		PublicSourceList: []string{"203.0.113.0/24", "198.51.100.7"},
		VirtualSourceList: []identity.NetworkSourcesVirtualSourceList{
			{VcnId: new("ocid1.vcn.oc1..abc"), IpRanges: []string{"10.0.0.0/16"}},
		},
	})
	require.NoError(t, err)

	return r.NetworkSources
}

func TestNetworkSourceCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	ns := e.mkNetworkSource(t, "Office")
	assert.Equal(t, identity.NetworkSourcesLifecycleStateActive, ns.LifecycleState)
	assert.Equal(t, []string{"203.0.113.0/24", "198.51.100.7"}, ns.PublicSourceList)

	_, err := e.client.CreateNetworkSource(ctx, identity.CreateNetworkSourceRequest{
		CompartmentId: new(testTenancy), Name: new("office"), Description: new("d"),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "names are unique case-insensitively")

	got, err := e.client.GetNetworkSource(ctx, identity.GetNetworkSourceRequest{NetworkSourceId: ns.Id})
	require.NoError(t, err)
	assert.Equal(t, *ns.Id, *got.Id)
	require.NotNil(t, got.Etag)

	e.mkNetworkSource(t, "Home")

	lst, err := e.client.ListNetworkSources(ctx, identity.ListNetworkSourcesRequest{
		CompartmentId: new(testTenancy), SortBy: identity.ListNetworkSourcesSortByName,
	})
	require.NoError(t, err)
	require.Len(t, lst.Items, 2)
	assert.Equal(t, "Home", *lst.Items[0].Name)

	byName, err := e.client.ListNetworkSources(ctx, identity.ListNetworkSourcesRequest{
		CompartmentId: new(testTenancy), Name: new("Office"),
	})
	require.NoError(t, err)
	assert.Len(t, byName.Items, 1)

	_, err = e.client.UpdateNetworkSource(ctx, identity.UpdateNetworkSourceRequest{
		NetworkSourceId: ns.Id, IfMatch: new("stale"),
		Description: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	up, err := e.client.UpdateNetworkSource(ctx, identity.UpdateNetworkSourceRequest{
		NetworkSourceId: ns.Id, IfMatch: got.Etag,
		Description: new("new"), PublicSourceList: []string{"192.0.2.1"},
	})
	require.NoError(t, err)
	assert.Equal(t, "new", *up.Description)
	assert.Equal(t, []string{"192.0.2.1"}, up.PublicSourceList)
	assert.Len(t, up.VirtualSourceList, 1, "an omitted list is left alone")

	_, err = e.client.DeleteNetworkSource(ctx, identity.DeleteNetworkSourceRequest{NetworkSourceId: ns.Id})
	require.NoError(t, err)

	_, err = e.client.GetNetworkSource(ctx, identity.GetNetworkSourceRequest{NetworkSourceId: ns.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestNetworkSourceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		details identity.CreateNetworkSourceDetails
		code    string
	}{
		{"bad cidr", identity.CreateNetworkSourceDetails{
			CompartmentId: new(
				testTenancy,
			), Name: new("a"), Description: new("d"), PublicSourceList: []string{"300.1.1.1/8"},
		}, "InvalidParameter"},
		{"bad vcn", identity.CreateNetworkSourceDetails{
			CompartmentId: new(testTenancy), Name: new("a"), Description: new("d"),
			VirtualSourceList: []identity.NetworkSourcesVirtualSourceList{{VcnId: new("vcn-1")}},
		}, "InvalidParameter"},
		{"not tenancy", identity.CreateNetworkSourceDetails{
			CompartmentId: new("ocid1.compartment.oc1..x"), Name: new("a"), Description: new("d"),
		}, "InvalidParameter"},
		{"blank name", identity.CreateNetworkSourceDetails{
			CompartmentId: new(testTenancy), Name: new(" "), Description: new("d"),
		}, "MissingParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := e.client.CreateNetworkSource(
				t.Context(),
				identity.CreateNetworkSourceRequest{CreateNetworkSourceDetails: tt.details},
			)
			assert.Equal(t, tt.code, svcErr(t, err).GetCode())
		})
	}
}

func TestAuthenticationPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	def, err := e.client.GetAuthenticationPolicy(
		ctx,
		identity.GetAuthenticationPolicyRequest{CompartmentId: new(testTenancy)},
	)
	require.NoError(t, err)
	assert.Equal(t, testTenancy, *def.CompartmentId)
	assert.Empty(t, def.NetworkPolicy.NetworkSourceIds)
	require.NotNil(t, def.PasswordPolicy.MinimumPasswordLength)

	_, err = e.client.GetAuthenticationPolicy(
		ctx,
		identity.GetAuthenticationPolicyRequest{CompartmentId: new("ocid1.compartment.oc1..x")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	ns := e.mkNetworkSource(t, "corp")

	_, err = e.client.UpdateAuthenticationPolicy(ctx, identity.UpdateAuthenticationPolicyRequest{
		CompartmentId: new(testTenancy),
		NetworkPolicy: &identity.NetworkPolicy{NetworkSourceIds: []string{"ocid1.networksource.oc1..nope"}},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "the referenced network source must exist")

	_, err = e.client.UpdateAuthenticationPolicy(ctx, identity.UpdateAuthenticationPolicyRequest{
		CompartmentId:  new(testTenancy),
		PasswordPolicy: &identity.PasswordPolicy{MinimumPasswordLength: new(0)},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	up, err := e.client.UpdateAuthenticationPolicy(ctx, identity.UpdateAuthenticationPolicyRequest{
		CompartmentId: new(testTenancy),
		PasswordPolicy: &identity.PasswordPolicy{
			MinimumPasswordLength:       new(20),
			IsSpecialCharactersRequired: new(true),
		},
		NetworkPolicy: &identity.NetworkPolicy{NetworkSourceIds: []string{*ns.Id}},
	})
	require.NoError(t, err)
	assert.Equal(t, 20, *up.PasswordPolicy.MinimumPasswordLength)
	assert.True(t, *up.PasswordPolicy.IsSpecialCharactersRequired)
	assert.Equal(t, *def.PasswordPolicy.IsUppercaseCharactersRequired, *up.PasswordPolicy.IsUppercaseCharactersRequired,
		"unset fields keep their value")
	assert.Equal(t, []string{*ns.Id}, up.NetworkPolicy.NetworkSourceIds)

	_, err = e.client.DeleteNetworkSource(ctx, identity.DeleteNetworkSourceRequest{NetworkSourceId: ns.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "still referenced")

	_, err = e.client.UpdateAuthenticationPolicy(ctx, identity.UpdateAuthenticationPolicyRequest{
		CompartmentId: new(testTenancy), IfMatch: new("stale"),
		UpdateAuthenticationPolicyDetails: identity.UpdateAuthenticationPolicyDetails{},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdateAuthenticationPolicy(ctx, identity.UpdateAuthenticationPolicyRequest{
		CompartmentId: new(testTenancy),
		NetworkPolicy: &identity.NetworkPolicy{NetworkSourceIds: []string{}},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteNetworkSource(ctx, identity.DeleteNetworkSourceRequest{NetworkSourceId: ns.Id})
	require.NoError(t, err)
}

func TestRegionSubscriptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	create := func(key, token string) (identity.CreateRegionSubscriptionResponse, error) {
		return e.client.CreateRegionSubscription(ctx, identity.CreateRegionSubscriptionRequest{
			TenancyId: new(testTenancy), OpcRetryToken: new(token),
			RegionKey: new(key),
		})
	}

	_, err := create("ZZZ", "")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = create("IAD", "")
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "home region")

	r, err := create("phx", "tok")
	require.NoError(t, err)
	assert.Equal(t, "PHX", *r.RegionKey)
	assert.Equal(t, "us-phoenix-1", *r.RegionName)
	assert.Equal(t, identity.RegionSubscriptionStatusInProgress, r.Status)
	assert.False(t, *r.IsHomeRegion)

	again, err := create("PHX", "tok")
	require.NoError(t, err, "same token replays")
	assert.Equal(t, "PHX", *again.RegionKey)

	_, err = create("PHX", "other")
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	e.clock.Advance(3 * time.Second)

	lst, err := e.client.ListRegionSubscriptions(
		ctx,
		identity.ListRegionSubscriptionsRequest{TenancyId: new(testTenancy)},
	)
	require.NoError(t, err)
	require.Len(t, lst.Items, 2)
	assert.Equal(t, "IAD", *lst.Items[0].RegionKey)
	assert.Equal(t, "PHX", *lst.Items[1].RegionKey)
	assert.Equal(t, identity.RegionSubscriptionStatusReady, lst.Items[1].Status)
}

func TestTenancyLevelStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	ns := e.mkNetworkSource(t, "persisted")

	_, err := e.client.UpdateAuthenticationPolicy(ctx, identity.UpdateAuthenticationPolicyRequest{
		CompartmentId:  new(testTenancy),
		PasswordPolicy: &identity.PasswordPolicy{MinimumPasswordLength: new(33)},
		NetworkPolicy:  &identity.NetworkPolicy{NetworkSourceIds: []string{*ns.Id}},
	})
	require.NoError(t, err)

	_, err = e.client.CreateRegionSubscription(ctx, identity.CreateRegionSubscriptionRequest{
		TenancyId: new(testTenancy),
		RegionKey: new("FRA"),
	})
	require.NoError(t, err)

	fresh := oci.NewInMemoryBackend()
	fresh.SetClock(e.clock.Now)
	require.NoError(t, fresh.Restore(ctx, e.backend.Snapshot(ctx)))

	got, _, err := fresh.GetNetworkSource(*ns.Id)
	require.NoError(t, err)
	assert.Equal(t, "persisted", got.Name)

	pol, _, err := fresh.GetAuthenticationPolicy(testTenancy)
	require.NoError(t, err)
	assert.Equal(t, 33, pol.PasswordPolicy.MinimumPasswordLength)
	assert.Equal(t, []string{*ns.Id}, pol.NetworkPolicy.NetworkSourceIDs)

	subs, err := fresh.ListRegionSubscriptions(testTenancy)
	require.NoError(t, err)
	assert.Len(t, subs, 2)
}
