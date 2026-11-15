package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestPrivateIPRouteTable(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "iprt")
	other := e.mkVcn(t, "10.1.0.0/16", "iprtother")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	nic, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, SubnetID: *s.Id})
	require.NoError(t, err)

	rt, err := e.client.CreateRouteTable(
		ctx,
		core.CreateRouteTableRequest{CompartmentId: new(testComp), VcnId: v.Id, RouteRules: []core.RouteRule{}},
	)
	require.NoError(t, err)

	tests := []struct {
		rtID     *string
		name     string
		wantCode int
	}{
		{rt.Id, "own vcn", 0},
		{other.DefaultRouteTableId, "other vcn", http.StatusBadRequest},
		{new("ocid1.routetable.oc1.iad.nope"), "unknown", http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, cerr := e.client.CreatePrivateIp(t.Context(), core.CreatePrivateIpRequest{
				VnicId: &nic.VnicID, RouteTableId: tc.rtID,
			})
			if tc.wantCode != 0 {
				assert.Equal(t, tc.wantCode, svcErr(t, cerr).GetHTTPStatusCode())

				return
			}

			require.NoError(t, cerr)
			assert.Equal(t, *tc.rtID, *r.RouteTableId)
		})
	}

	ip, err := e.client.CreatePrivateIp(ctx, core.CreatePrivateIpRequest{
		VnicId: &nic.VnicID,
	})
	require.NoError(t, err)
	assert.Nil(t, ip.RouteTableId)

	up, err := e.client.UpdatePrivateIp(ctx, core.UpdatePrivateIpRequest{
		PrivateIpId:  ip.Id,
		RouteTableId: rt.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, *rt.Id, *up.RouteTableId)

	_, err = e.client.DeleteRouteTable(ctx, core.DeleteRouteTableRequest{RtId: rt.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "a private IP uses the route table")

	_, err = e.client.BulkUpdatePrivateIps(ctx, core.BulkUpdatePrivateIpsRequest{
		BulkUpdatePrivateIpItem: []core.BulkUpdatePrivateIpItem{
			{PrivateIpId: ip.Id, RouteTableId: other.DefaultRouteTableId},
		},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.BulkCreatePrivateIps(ctx, core.BulkCreatePrivateIpsRequest{
		VnicId:                  &nic.VnicID,
		BulkCreatePrivateIpItem: []core.BulkCreatePrivateIpItem{{RouteTableId: rt.Id}},
	})
	require.NoError(t, err)
}

func TestSecondaryPrivateIPLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "iplimit")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	nic, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, SubnetID: *s.Id})
	require.NoError(t, err)

	for range 64 {
		_, err = e.client.CreatePrivateIp(ctx, core.CreatePrivateIpRequest{
			VnicId: &nic.VnicID,
		})
		require.NoError(t, err)
	}

	_, err = e.client.CreatePrivateIp(ctx, core.CreatePrivateIpRequest{
		VnicId: &nic.VnicID,
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode(), "64 secondary private IPv4 addresses per VNIC")
}
