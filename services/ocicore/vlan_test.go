package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVlanLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)

	nsg, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
		CompartmentId: new(testComp),
		VcnId:         f.vcn.Id,
	})
	require.NoError(t, err)

	mk := func(cidr string, mod func(*core.CreateVlanDetails)) (core.Vlan, error) {
		d := core.CreateVlanDetails{CompartmentId: new(testComp), VcnId: f.vcn.Id, CidrBlock: new(cidr)}
		if mod != nil {
			mod(&d)
		}

		r, cerr := e.client.CreateVlan(ctx, core.CreateVlanRequest{CreateVlanDetails: d})

		return r.Vlan, cerr
	}

	tests := []struct {
		mod      func(*core.CreateVlanDetails)
		name     string
		cidr     string
		wantCode int
	}{
		{name: "outside vcn", cidr: "192.168.0.0/24", wantCode: http.StatusBadRequest},
		{name: "overlaps subnet", cidr: "10.0.1.0/25", wantCode: http.StatusConflict},
		{
			name: "tag zero", cidr: "10.0.50.0/24", mod: func(d *core.CreateVlanDetails) { d.VlanTag = new(0) },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown nsg", cidr: "10.0.51.0/24",
			mod:      func(d *core.CreateVlanDetails) { d.NsgIds = []string{"ocid1.nsg.oc1.iad.nope"} },
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		_, cerr := mk(tc.cidr, tc.mod)
		assert.Equal(t, tc.wantCode, svcErr(t, cerr).GetHTTPStatusCode(), tc.name)
	}

	v, err := mk("10.0.60.0/24", func(d *core.CreateVlanDetails) { d.NsgIds = []string{*nsg.Id} })
	require.NoError(t, err)
	assert.Equal(t, 1, *v.VlanTag, "Oracle assigns the lowest free tag")
	assert.Equal(t, []string{*nsg.Id}, v.NsgIds)

	_, err = mk("10.0.61.0/24", func(d *core.CreateVlanDetails) { d.VlanTag = new(1) })
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "tags are unique per VCN")

	_, err = mk("10.0.60.128/25", nil)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "VLANs cannot overlap")

	ip, err := e.client.CreatePrivateIp(ctx, core.CreatePrivateIpRequest{
		VlanId: v.Id, DisplayName: new("vip"),
	})
	require.NoError(t, err)
	assert.Equal(t, "10.0.60.2", *ip.IpAddress)
	assert.Equal(t, *v.Id, *ip.VlanId)

	_, err = e.client.CreatePrivateIp(ctx, core.CreatePrivateIpRequest{
		VlanId: v.Id, IpAddress: new("10.0.60.0"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "the first addresses are reserved")

	l, err := e.client.ListPrivateIps(ctx, core.ListPrivateIpsRequest{VlanId: v.Id})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)

	_, err = e.client.DeleteVlan(ctx, core.DeleteVlanRequest{VlanId: v.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a private IP lives in the VLAN")

	vm := e.mustLaunch(t, f.pub)
	e.settle()

	_, err = e.compute.AttachVnic(
		ctx,
		core.AttachVnicRequest{InstanceId: vm.Id, CreateVnicDetails: &core.CreateVnicDetails{VlanId: v.Id}},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "VLAN VNICs need a bare metal instance")

	bm, err := e.launch(
		t,
		f.pub,
		func(d *core.LaunchInstanceDetails) { d.Shape, d.ShapeConfig = new("BM.Standard.E4.128"), nil },
	)
	require.NoError(t, err)
	e.settle()

	att, err := e.compute.AttachVnic(
		ctx,
		core.AttachVnicRequest{InstanceId: bm.Id, CreateVnicDetails: &core.CreateVnicDetails{VlanId: v.Id}},
	)
	require.NoError(t, err)
	assert.Equal(t, *v.Id, *att.VlanId)
	assert.Equal(t, 1, *att.VlanTag)
	assert.Nil(t, att.SubnetId)

	_, err = e.compute.AttachVnic(
		ctx,
		core.AttachVnicRequest{InstanceId: bm.Id, CreateVnicDetails: &core.CreateVnicDetails{VlanId: v.Id}},
	)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "one VNIC per VLAN and instance")

	_, err = e.compute.AttachVnic(
		ctx,
		core.AttachVnicRequest{
			InstanceId:        bm.Id,
			CreateVnicDetails: &core.CreateVnicDetails{VlanId: v.Id, SubnetId: f.pub.Id},
		},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "vlanId and subnetId are exclusive")

	vn, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: att.VnicId})
	require.NoError(t, err)
	assert.Equal(t, *v.Id, *vn.VlanId)
	assert.Equal(t, []string{*nsg.Id}, vn.NsgIds, "the VNIC joins the NSGs of the VLAN")

	_, err = e.client.UpdateVnic(ctx, core.UpdateVnicRequest{
		VnicId:      att.VnicId,
		DisplayName: new("renamed"), SkipSourceDestCheck: new(true),
	})
	require.NoError(t, err)

	_, err = e.client.DeleteVlan(ctx, core.DeleteVlanRequest{VlanId: v.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())
}
