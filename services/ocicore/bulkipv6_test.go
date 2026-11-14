package ocicore_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func (e *env) listVnicIpv6s(t *testing.T, vnicID *string) []core.Ipv6 {
	t.Helper()

	r, err := e.client.ListIpv6s(t.Context(), core.ListIpv6sRequest{VnicId: vnicID})
	require.NoError(t, err)

	return r.Items
}

func TestBulkIpv6s(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.v6Fixture(t)

	nic, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, SubnetID: *f.subnet.Id})
	require.NoError(t, err)

	vnic := &nic.VnicID

	created, err := e.client.BulkCreateIpv6s(ctx, core.BulkCreateIpv6sRequest{
		OpcRetryToken: new("tok"),
		VnicId:        vnic,
		BulkCreateIpv6sItem: []core.BulkCreateIpv6sItem{
			{DisplayName: new("a")}, {DisplayName: new("b"), FreeformTags: map[string]string{"k": "v"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.OpcWorkRequestId)
	assert.Len(t, e.workRequest(t, created.OpcWorkRequestId).Resources, 2)

	again, err := e.client.BulkCreateIpv6s(ctx, core.BulkCreateIpv6sRequest{
		OpcRetryToken: new("tok"),
		VnicId:        vnic, BulkCreateIpv6sItem: []core.BulkCreateIpv6sItem{{}},
	})
	require.NoError(t, err)
	assert.Equal(t, *created.OpcWorkRequestId, *again.OpcWorkRequestId)

	ips := e.listVnicIpv6s(t, vnic)
	require.Len(t, ips, 2)

	_, err = e.client.BulkCreateIpv6s(ctx, core.BulkCreateIpv6sRequest{
		VnicId: vnic, BulkCreateIpv6sItem: []core.BulkCreateIpv6sItem{{}, {IpAddress: new("2001:db8::1")}},
	})
	require.Error(t, err)
	assert.Len(t, e.listVnicIpv6s(t, vnic), 2, "a failing item creates nothing")

	items := []core.BulkUpdateIpv6sItem{
		{Ipv6Id: ips[0].Id, DisplayName: new("renamed"), Lifetime: core.BulkUpdateIpv6sItemLifetimeReserved},
		{Ipv6Id: ips[1].Id, Lifetime: core.BulkUpdateIpv6sItemLifetimeReserved},
	}
	_, err = e.client.BulkUpdateIpv6s(ctx, core.BulkUpdateIpv6sRequest{
		BulkUpdateIpv6sItem: items,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *e.getIpv6(t, ips[0].Id).DisplayName)

	_, err = e.client.BulkDetachIpv6s(ctx, core.BulkDetachIpv6sRequest{
		VnicId: vnic, BulkDetachIpv6sItem: []core.BulkDetachIpv6sItem{{Ipv6Id: ips[0].Id}, {Ipv6Id: ips[1].Id}},
	})
	require.NoError(t, err)
	assert.Nil(t, e.getIpv6(t, ips[0].Id).VnicId)

	_, err = e.client.BulkUpdateIpv6s(ctx, core.BulkUpdateIpv6sRequest{
		VnicId: vnic, BulkUpdateIpv6sItem: []core.BulkUpdateIpv6sItem{{Ipv6Id: ips[0].Id}, {Ipv6Id: ips[1].Id}},
	})
	require.NoError(t, err)
	assert.Len(t, e.listVnicIpv6s(t, vnic), 2, "update with a vnicId reassigns")

	_, err = e.client.BulkDeleteIpv6s(ctx, core.BulkDeleteIpv6sRequest{
		SubnetId: f.plain.Id, BulkDeleteIpv6sItem: []core.BulkDeleteIpv6sItem{{Ipv6Id: ips[0].Id}},
	})
	require.Error(t, err, "an IPv6 of another subnet is refused")

	_, err = e.client.BulkDeleteIpv6s(ctx, core.BulkDeleteIpv6sRequest{
		VnicId: vnic, BulkDeleteIpv6sItem: []core.BulkDeleteIpv6sItem{{Ipv6Id: ips[0].Id}, {Ipv6Id: ips[1].Id}},
	})
	require.NoError(t, err)

	e.settle()
	assert.Empty(t, e.listVnicIpv6s(t, vnic))
}
