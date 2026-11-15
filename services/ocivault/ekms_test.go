package ocivault_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const ekmsSubnet = "ocid1.subnet.oc1.iad.ekms"

func (e *env) ekmClient(t *testing.T) keymanagement.EkmClient {
	t.Helper()

	c, err := keymanagement.NewEkmClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	c.Host = e.base

	return c
}

func ekmsDetails(t *testing.T, name string) keymanagement.CreateEkmsPrivateEndpointDetails {
	t.Helper()

	ca, err := k3sengine.NewCA("ekms-test")
	require.NoError(t, err)

	return keymanagement.CreateEkmsPrivateEndpointDetails{
		CompartmentId: new(testCompartment), SubnetId: new(ekmsSubnet), DisplayName: new(name),
		ExternalKeyManagerIp: new("10.0.9.9"), CaBundle: new(string(ca.CertPEM)),
	}
}

func TestEkmsPrivateEndpointLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetSubnetResolver(func(id string) (string, bool) { return "10.0.1.0/24", id == ekmsSubnet })

	c := e.ekmClient(t)
	ctx := t.Context()

	created, err := c.CreateEkmsPrivateEndpoint(ctx, keymanagement.CreateEkmsPrivateEndpointRequest{
		CreateEkmsPrivateEndpointDetails: ekmsDetails(t, "ekms-a"),
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.EkmsPrivateEndpointLifecycleStateCreating, created.LifecycleState)
	assert.Equal(t, 443, *created.Port)
	assert.Equal(t, "10.0.1.254", *created.PrivateEndpointIp)
	require.NotNil(t, created.Etag)

	e.settle()

	got, err := c.GetEkmsPrivateEndpoint(ctx, keymanagement.GetEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.EkmsPrivateEndpointLifecycleStateActive, got.LifecycleState)

	second, err := c.CreateEkmsPrivateEndpoint(ctx, keymanagement.CreateEkmsPrivateEndpointRequest{
		CreateEkmsPrivateEndpointDetails: ekmsDetails(t, "ekms-b"),
	})
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.253", *second.PrivateEndpointIp, "each endpoint gets its own address")

	list, err := c.ListEkmsPrivateEndpoints(ctx, keymanagement.ListEkmsPrivateEndpointsRequest{
		CompartmentId: new(testCompartment), SortBy: keymanagement.ListEkmsPrivateEndpointsSortByDisplayname,
		SortOrder: keymanagement.ListEkmsPrivateEndpointsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.Equal(t, "ekms-a", *list.Items[0].DisplayName)

	_, err = c.UpdateEkmsPrivateEndpoint(ctx, keymanagement.UpdateEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id, IfMatch: new("stale"), DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := c.UpdateEkmsPrivateEndpoint(ctx, keymanagement.UpdateEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id, DisplayName: new("renamed"), FreeformTags: map[string]string{"a": "b"},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)
	assert.NotNil(t, upd.TimeUpdated)

	snap := e.backend.Snapshot(ctx)
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(ctx, snap))

	restored, err := fresh.ekmClient(t).GetEkmsPrivateEndpoint(ctx, keymanagement.GetEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *restored.DisplayName)

	_, err = c.DeleteEkmsPrivateEndpoint(ctx, keymanagement.DeleteEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id,
	})
	require.NoError(t, err)

	mid, err := c.GetEkmsPrivateEndpoint(ctx, keymanagement.GetEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.EkmsPrivateEndpointLifecycleStateDeleting, mid.LifecycleState)

	e.settle()

	gone, err := c.GetEkmsPrivateEndpoint(ctx, keymanagement.GetEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.EkmsPrivateEndpointLifecycleStateDeleted, gone.LifecycleState)
}

func TestEkmsPrivateEndpointRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetSubnetResolver(func(id string) (string, bool) { return "10.0.1.0/24", id == ekmsSubnet })

	c := e.ekmClient(t)

	tests := []struct {
		mut    func(d *keymanagement.CreateEkmsPrivateEndpointDetails)
		name   string
		status int
	}{
		{
			name:   "unknown subnet",
			status: http.StatusBadRequest,
			mut: func(d *keymanagement.CreateEkmsPrivateEndpointDetails) {
				d.SubnetId = new("ocid1.subnet.oc1.iad.nope")
			},
		},
		{name: "bad ip", status: http.StatusBadRequest, mut: func(d *keymanagement.CreateEkmsPrivateEndpointDetails) {
			d.ExternalKeyManagerIp = new("not-an-ip")
		}},
		{
			name:   "bad ca bundle",
			status: http.StatusBadRequest,
			mut: func(d *keymanagement.CreateEkmsPrivateEndpointDetails) {
				d.CaBundle = new("not pem")
			},
		},
		{name: "bad port", status: http.StatusBadRequest, mut: func(d *keymanagement.CreateEkmsPrivateEndpointDetails) {
			d.Port = new(70000)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := ekmsDetails(t, "rules")
			tt.mut(&d)

			_, err := c.CreateEkmsPrivateEndpoint(t.Context(), keymanagement.CreateEkmsPrivateEndpointRequest{
				CreateEkmsPrivateEndpointDetails: d,
			})
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	for i := range 3 {
		d := ekmsDetails(t, "limit")
		d.DisplayName = new("limit-" + string(rune('a'+i)))

		_, err := c.CreateEkmsPrivateEndpoint(t.Context(), keymanagement.CreateEkmsPrivateEndpointRequest{
			CreateEkmsPrivateEndpointDetails: d,
		})
		require.NoError(t, err)
	}

	_, err := c.CreateEkmsPrivateEndpoint(t.Context(), keymanagement.CreateEkmsPrivateEndpointRequest{
		CreateEkmsPrivateEndpointDetails: ekmsDetails(t, "over"),
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	_, err = c.GetEkmsPrivateEndpoint(t.Context(), keymanagement.GetEkmsPrivateEndpointRequest{
		EkmsPrivateEndpointId: new("ocid1.ekmsprivateendpoint.oc1.iad.nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
