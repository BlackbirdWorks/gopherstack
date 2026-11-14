package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/oracle/oci-go-sdk/v65/workrequests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func (e *env) listVnicIPs(t *testing.T, vnicID *string) []core.PrivateIp {
	t.Helper()

	r, err := e.client.ListPrivateIps(t.Context(), core.ListPrivateIpsRequest{VnicId: vnicID})
	require.NoError(t, err)

	return r.Items
}

func TestBulkPrivateIPs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "bulk")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	nic, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, SubnetID: *s.Id})
	require.NoError(t, err)

	vnic := &nic.VnicID
	require.Len(t, e.listVnicIPs(t, vnic), 1)

	created, err := e.client.BulkCreatePrivateIps(ctx, core.BulkCreatePrivateIpsRequest{
		OpcRetryToken: new("tok"),
		VnicId:        vnic,
		BulkCreatePrivateIpItem: []core.BulkCreatePrivateIpItem{
			{DisplayName: new("a"), IpAddress: new("10.0.1.20")},
			{DisplayName: new("b"), FreeformTags: map[string]string{"k": "v"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.OpcWorkRequestId)

	again, err := e.client.BulkCreatePrivateIps(ctx, core.BulkCreatePrivateIpsRequest{
		OpcRetryToken:           new("tok"),
		VnicId:                  vnic,
		BulkCreatePrivateIpItem: []core.BulkCreatePrivateIpItem{{DisplayName: new("a")}},
	})
	require.NoError(t, err)
	assert.Equal(t, *created.OpcWorkRequestId, *again.OpcWorkRequestId, "a repeated token replays the first request")

	wr, err := e.wr.GetWorkRequest(ctx, workrequests.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Len(t, wr.Resources, 2)

	ips := e.listVnicIPs(t, vnic)
	require.Len(t, ips, 3)

	var a, b core.PrivateIp

	for _, ip := range ips {
		switch {
		case ip.DisplayName != nil && *ip.DisplayName == "a":
			a = ip
		case ip.DisplayName != nil && *ip.DisplayName == "b":
			b = ip
		}
	}

	assert.Equal(t, "10.0.1.20", *a.IpAddress)
	assert.Equal(t, map[string]string{"k": "v"}, b.FreeformTags)

	_, err = e.client.BulkCreatePrivateIps(ctx, core.BulkCreatePrivateIpsRequest{
		VnicId: vnic,
		BulkCreatePrivateIpItem: []core.BulkCreatePrivateIpItem{
			{DisplayName: new("c")}, {IpAddress: new("10.9.9.9")},
		},
	})
	require.Error(t, err)
	assert.Len(t, e.listVnicIPs(t, vnic), 3, "a failing item creates nothing")

	_, err = e.client.BulkUpdatePrivateIps(ctx, core.BulkUpdatePrivateIpsRequest{
		BulkUpdatePrivateIpItem: []core.BulkUpdatePrivateIpItem{
			{
				PrivateIpId: a.Id,
				DisplayName: new("renamed"),
				Lifetime:    core.BulkUpdatePrivateIpItemLifetimeReserved,
			},
			{PrivateIpId: b.Id, Lifetime: core.BulkUpdatePrivateIpItemLifetimeReserved},
		},
	})
	require.NoError(t, err)

	got, err := e.client.GetPrivateIp(ctx, core.GetPrivateIpRequest{PrivateIpId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *got.DisplayName)
	assert.Equal(t, core.PrivateIpLifetimeReserved, got.Lifetime)

	_, err = e.client.BulkDetachPrivateIps(ctx, core.BulkDetachPrivateIpsRequest{
		VnicId:                  vnic,
		BulkDetachPrivateIpItem: []core.BulkDetachPrivateIpItem{{PrivateIpId: a.Id}, {PrivateIpId: b.Id}},
	})
	require.NoError(t, err)

	got, err = e.client.GetPrivateIp(ctx, core.GetPrivateIpRequest{PrivateIpId: a.Id})
	require.NoError(t, err)
	assert.Nil(t, got.VnicId)
	assert.Len(t, e.listVnicIPs(t, vnic), 1)

	_, err = e.client.BulkUpdatePrivateIps(ctx, core.BulkUpdatePrivateIpsRequest{
		VnicId:                  vnic,
		BulkUpdatePrivateIpItem: []core.BulkUpdatePrivateIpItem{{PrivateIpId: a.Id}, {PrivateIpId: b.Id}},
	})
	require.NoError(t, err)
	assert.Len(t, e.listVnicIPs(t, vnic), 3, "update with a vnicId reassigns")

	_, err = e.client.BulkDeletePrivateIps(ctx, core.BulkDeletePrivateIpsRequest{
		VnicId:                  vnic,
		BulkDeletePrivateIpItem: []core.BulkDeletePrivateIpItem{{PrivateIpId: a.Id}, {PrivateIpId: b.Id}},
	})
	require.NoError(t, err)
	assert.Len(t, e.listVnicIPs(t, vnic), 1)

	_, err = e.client.GetPrivateIp(ctx, core.GetPrivateIpRequest{PrivateIpId: a.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	primary := e.listVnicIPs(t, vnic)[0]
	_, err = e.client.BulkDeletePrivateIps(ctx, core.BulkDeletePrivateIpsRequest{
		BulkDeletePrivateIpItem: []core.BulkDeletePrivateIpItem{{PrivateIpId: primary.Id}},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "the primary IP stays")
}
