package ocicore_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/oracle/oci-go-sdk/v65/workrequests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func (e *env) nic(t *testing.T, s core.Subnet, ip string) (oci.ServiceNic, error) {
	t.Helper()
	e.backend.Sweep()

	return e.backend.CreateServiceNic(oci.ServiceNicIn{
		CompartmentID: testComp, AD: testAD, SubnetID: *s.Id, PrivateIP: ip})
}

func (e *env) getSubnet(t *testing.T, s core.Subnet) core.Subnet {
	t.Helper()

	r, err := e.client.GetSubnet(t.Context(), core.GetSubnetRequest{SubnetId: s.Id})
	require.NoError(t, err)

	return r.Subnet
}

func TestUpdateSubnetCidr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cidr     string
		wantCode string
		nic      string
		neighbor string
	}{
		{name: "expand", cidr: "10.0.2.0/23"},
		{name: "shrink", cidr: "10.0.2.0/25"},
		{name: "different network address", cidr: "10.0.3.0/24", wantCode: "InvalidParameter"},
		{name: "outside the vcn", cidr: "10.0.2.0/8", wantCode: "InvalidParameter"},
		{name: "overlaps neighbor", cidr: "10.0.2.0/23", neighbor: "10.0.3.0/24", wantCode: "InvalidParameter"},
		{name: "address outside", cidr: "10.0.2.0/29", nic: "10.0.2.20", wantCode: "InvalidParameter"},
		{name: "last address used", cidr: "10.0.2.0/29", nic: "10.0.2.7", wantCode: "InvalidParameter"},
		{name: "address kept", cidr: "10.0.2.0/25", nic: "10.0.2.20"},
		{name: "not a network address", cidr: "10.0.2.1/24", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			ctx := t.Context()
			v := e.mkVcn(t, "10.0.0.0/16", "rs")
			s := e.mkSubnet(t, v, "10.0.2.0/24")
			e.settle()

			if tt.neighbor != "" {
				e.mkSubnet(t, v, tt.neighbor)
			}

			if tt.nic != "" {
				_, err := e.nic(t, s, tt.nic)
				require.NoError(t, err)
			}

			r, err := e.client.UpdateSubnet(ctx, core.UpdateSubnetRequest{SubnetId: s.Id, CidrBlock: new(tt.cidr)})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())
				assert.Equal(t, "10.0.2.0/24", *e.getSubnet(t, s).CidrBlock, "the CIDR block is unchanged")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.cidr, *r.CidrBlock)
			assert.Equal(t, []string{tt.cidr}, r.Ipv4CidrBlocks)
			assert.Equal(t, core.SubnetLifecycleStateUpdating, r.LifecycleState)

			_, nerr := e.nic(t, s, "")
			require.Error(t, nerr, "no VNICs while the subnet is updating")

			e.settle()
			assert.Equal(t, core.SubnetLifecycleStateAvailable, e.getSubnet(t, s).LifecycleState)

			n, nerr := e.nic(t, s, "")
			require.NoError(t, nerr)
			assert.NotEmpty(t, n.IPAddress)
		})
	}
}

func TestSubnetIpv4CidrOps(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "ops")
	s := e.mkSubnet(t, v, "10.0.1.0/30")
	other := e.mkSubnet(t, v, "10.0.9.0/24")
	e.settle()

	first, err := e.nic(t, s, "")
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.2", first.IPAddress)

	_, err = e.nic(t, s, "")
	require.Error(t, err, "the /30 is full")

	add := func(cidr string) (core.AddIpv4SubnetCidrResponse, error) {
		return e.client.AddIpv4SubnetCidr(ctx, core.AddIpv4SubnetCidrRequest{SubnetId: s.Id,
			Ipv4CidrBlock: new(cidr)})
	}

	_, err = add("10.0.9.0/25")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "overlaps another subnet")

	_, err = add("10.0.1.0/30")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "duplicates the subnet's own block")

	_, err = add("192.168.0.0/24")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "outside the VCN")

	r, err := add("10.0.2.0/29")
	require.NoError(t, err)
	assert.Equal(t, "ADD_SUBNET_IPV4_CIDR", *e.workRequest(t, r.OpcWorkRequestId).OperationType)

	got := e.getSubnet(t, s)
	assert.Equal(t, core.SubnetLifecycleStateUpdating, got.LifecycleState)
	assert.Equal(t, []string{"10.0.1.0/30", "10.0.2.0/29"}, got.Ipv4CidrBlocks)
	assert.Equal(t, "10.0.1.0/30", *got.CidrBlock)

	_, err = add("10.0.3.0/29")
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "one change at a time")

	e.settle()

	second, err := e.nic(t, s, "")
	require.NoError(t, err)
	assert.Equal(t, "10.0.2.2", second.IPAddress, "addresses come from the next block once the first is full")

	_, err = e.nic(t, s, "10.0.5.5")
	require.Error(t, err)

	modify := func(from, to string) error {
		_, merr := e.client.ModifyIpv4SubnetCidr(ctx, core.ModifyIpv4SubnetCidrRequest{SubnetId: s.Id,
			Ipv4CidrBlock: new(from), UpdatedIpv4CidrBlock: new(to)})

		return merr
	}

	assert.Equal(t, "InvalidParameter", svcErr(t, modify("10.0.7.0/29", "10.0.7.0/28")).GetCode(), "unknown block")
	assert.Equal(t, "InvalidParameter", svcErr(t, modify("10.0.2.0/29", "10.0.2.0/29")).GetCode(), "unchanged block")
	assert.Equal(t, "InvalidParameter", svcErr(t, modify("10.0.2.0/29", "10.0.9.0/29")).GetCode(), "overlaps a subnet")
	assert.Equal(t, "InvalidParameter", svcErr(t, modify("10.0.2.0/29", "10.0.8.0/30")).GetCode(),
		"address 10.0.2.2 would be outside")

	require.NoError(t, modify("10.0.2.0/29", "10.0.2.0/28"))
	e.settle()
	assert.Equal(t, []string{"10.0.1.0/30", "10.0.2.0/28"}, e.getSubnet(t, s).Ipv4CidrBlocks)

	remove := func(cidr string) (core.RemoveIpv4SubnetCidrResponse, error) {
		return e.client.RemoveIpv4SubnetCidr(ctx, core.RemoveIpv4SubnetCidrRequest{SubnetId: s.Id,
			Ipv4CidrBlock: new(cidr)})
	}

	_, err = remove("10.0.2.0/28")
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "10.0.2.2 is in use")

	_, err = remove("10.0.4.0/28")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	e.backend.DeleteServiceNic(second.VnicID)
	e.settle()

	rr, err := remove("10.0.2.0/28")
	require.NoError(t, err)
	assert.Equal(t, "REMOVE_SUBNET_IPV4_CIDR", *e.workRequest(t, rr.OpcWorkRequestId).OperationType)
	e.settle()

	_, err = remove("10.0.1.0/30")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "the last block stays")

	assert.Equal(t, []string{"10.0.1.0/30"}, e.getSubnet(t, s).Ipv4CidrBlocks)
	assert.Equal(t, "10.0.9.0/24", *e.getSubnet(t, other).CidrBlock)

	_, err = e.client.AddIpv4SubnetCidr(ctx, core.AddIpv4SubnetCidrRequest{SubnetId: new("ocid1.subnet.oc1.iad.nope"),
		Ipv4CidrBlock: new("10.0.3.0/29")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSubnetMultiCidrCreate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "multi")

	tests := []struct {
		name     string
		wantCode string
		cidrs    []string
	}{
		{"two blocks", "", []string{"10.0.1.0/24", "10.0.2.0/24"}},
		{"overlapping blocks", "InvalidParameter", []string{"10.0.3.0/24", "10.0.3.0/25"}},
		{"one outside", "InvalidParameter", []string{"10.0.4.0/24", "172.16.0.0/24"}},
		{"too many", "LimitExceeded", []string{"10.0.10.0/29", "10.0.11.0/29", "10.0.12.0/29", "10.0.13.0/29", "10.0.14.0/29",
			"10.0.15.0/29", "10.0.16.0/29"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
				Ipv4CidrBlocks: tt.cidrs})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.cidrs, r.Ipv4CidrBlocks)
			assert.Equal(t, tt.cidrs[0], *r.CidrBlock)
		})
	}
}

func TestSubnetIpv6CidrOps(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v, err := e.client.CreateVcn(ctx, core.CreateVcnRequest{CompartmentId: new(testComp),
		CidrBlocks: []string{"10.0.0.0/16"}, IsIpv6Enabled: new(true), DnsLabel: new("v6ops")})
	require.NoError(t, err)

	e.settle()

	base := hexSubnet(v.Ipv6CidrBlocks[0])
	second := "2603:c020:4000:101::/64"

	created, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
		CidrBlock: new("10.0.1.0/24"), Ipv6CidrBlock: new(base)})
	require.NoError(t, err)
	e.settle()

	s := created.Subnet
	assert.Equal(t, []string{base}, s.Ipv6CidrBlocks)
	assert.NotEmpty(t, *s.Ipv6VirtualRouterIp)

	_, err = e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
		CidrBlock: new("10.0.2.0/24"), Ipv6CidrBlock: new(base)})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "prefix already used")

	add := func(c string) (core.AddIpv6SubnetCidrResponse, error) {
		return e.client.AddIpv6SubnetCidr(ctx, core.AddIpv6SubnetCidrRequest{SubnetId: s.Id,
			Ipv6CidrBlock: new(c)})
	}

	_, err = add("2001:db8::/64")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "outside the VCN prefixes")

	_, err = add(base)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "already on the subnet")

	r, err := add(second)
	require.NoError(t, err)
	assert.NotEmpty(t, *r.Etag)
	assert.Equal(t, "ADD_SUBNET_IPV6_CIDR", *e.workRequest(t, r.OpcWorkRequestId).OperationType)
	e.settle()

	got := e.getSubnet(t, s)
	assert.Equal(t, []string{base, second}, got.Ipv6CidrBlocks)

	nic, err := e.nic(t, s, "")
	require.NoError(t, err)

	ip, err := e.client.CreateIpv6(ctx, core.CreateIpv6Request{VnicId: new(nic.VnicID), Ipv6SubnetCidr: new(second)})
	require.NoError(t, err)

	remove := func(c string) error {
		_, rerr := e.client.RemoveIpv6SubnetCidr(ctx, core.RemoveIpv6SubnetCidrRequest{SubnetId: s.Id,
			Ipv6CidrBlock: new(c)})

		return rerr
	}

	assert.Equal(t, "Conflict", svcErr(t, remove(second)).GetCode(), "an IPv6 lives in the prefix")
	assert.Equal(t, "InvalidParameter", svcErr(t, remove("2603:c020:4000:999::/64")).GetCode())

	_, err = e.client.DeleteIpv6(ctx, core.DeleteIpv6Request{Ipv6Id: ip.Id})
	require.NoError(t, err)
	e.settle()

	require.NoError(t, remove(second))
	e.settle()

	assert.Equal(t, "InvalidParameter", svcErr(t, remove(base)).GetCode(), "one prefix must remain")

	upd, err := e.client.UpdateSubnet(
		ctx,
		core.UpdateSubnetRequest{SubnetId: s.Id, Ipv6CidrBlocks: []string{base, second}},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{base, second}, upd.Ipv6CidrBlocks)

	upd, err = e.client.UpdateSubnet(ctx, core.UpdateSubnetRequest{SubnetId: s.Id, Ipv6CidrBlocks: []string{base}})
	require.NoError(t, err)
	assert.Equal(t, []string{base}, upd.Ipv6CidrBlocks)

	_, err = e.client.UpdateSubnet(ctx, core.UpdateSubnetRequest{SubnetId: s.Id, Ipv6CidrBlock: new("2001:db8::/64")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestWorkRequestApis(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "wr")
	e.settle()

	r, err := e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
		CidrBlock: new("10.1.0.0/16")})
	require.NoError(t, err)

	logs, err := e.wr.ListWorkRequestLogs(
		ctx,
		workrequests.ListWorkRequestLogsRequest{WorkRequestId: r.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 1)

	e.settle()

	logs, err = e.wr.ListWorkRequestLogs(ctx, workrequests.ListWorkRequestLogsRequest{WorkRequestId: r.OpcWorkRequestId,
		SortOrder: workrequests.ListWorkRequestLogsSortOrderDesc})
	require.NoError(t, err)
	require.Len(t, logs.Items, 2)
	assert.Contains(t, *logs.Items[0].Message, "succeeded")

	errs, err := e.wr.ListWorkRequestErrors(
		ctx,
		workrequests.ListWorkRequestErrorsRequest{WorkRequestId: r.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	_, err = e.client.RemoveVcnCidr(ctx, core.RemoveVcnCidrRequest{VcnId: v.Id,
		CidrBlock: new("10.1.0.0/16")})
	require.NoError(t, err)

	all, err := e.wr.ListWorkRequests(ctx, workrequests.ListWorkRequestsRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)
	require.Len(t, all.Items, 2)
	assert.Equal(t, "REMOVE_VCN_CIDR", *all.Items[0].OperationType, "newest first")

	byRes, err := e.wr.ListWorkRequests(ctx, workrequests.ListWorkRequestsRequest{CompartmentId: new(testComp),
		ResourceId: new("ocid1.vcn.oc1.iad.other"), Limit: new(1)})
	require.NoError(t, err)
	assert.Empty(t, byRes.Items)

	page1, err := e.wr.ListWorkRequests(ctx, workrequests.ListWorkRequestsRequest{CompartmentId: new(testComp),
		ResourceId: v.Id, Limit: new(1)})
	require.NoError(t, err)
	require.Len(t, page1.Items, 1)
	assert.NotEmpty(t, *page1.OpcNextPage)

	_, err = e.wr.GetWorkRequest(
		ctx,
		workrequests.GetWorkRequestRequest{WorkRequestId: new("ocid1.coreservicesworkrequest.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	e.clock.Advance(2 * time.Hour)
	e.backend.Sweep()

	_, err = e.wr.GetWorkRequest(ctx, workrequests.GetWorkRequestRequest{WorkRequestId: r.OpcWorkRequestId})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "finished work requests are purged")
}
