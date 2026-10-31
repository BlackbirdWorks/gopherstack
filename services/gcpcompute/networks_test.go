package gcpcompute_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func TestAutoNetworkLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	nets := e.networks(t)

	op, err := nets.Insert(t.Context(), &computepb.InsertNetworkRequest{
		Project:         project,
		NetworkResource: &computepb.Network{Name: s("auto-net"), AutoCreateSubnetworks: new(true)},
	})
	require.NoError(t, err)

	pb := op.Proto()
	assert.Equal(t, "insert", pb.GetOperationType())
	assert.Equal(t, computepb.Operation_PENDING, pb.GetStatus())
	assert.Equal(
		t,
		"https://www.googleapis.com/compute/v1/projects/"+project+"/global/networks/auto-net",
		pb.GetTargetLink(),
	)
	assert.Contains(t, pb.GetSelfLink(), "/global/operations/operation-")
	assert.NotZero(t, pb.GetTargetId())

	e.settle()
	require.NoError(t, op.Wait(t.Context()))

	n, err := nets.Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "auto-net"})
	require.NoError(t, err)
	assert.True(t, n.GetAutoCreateSubnetworks())
	assert.Equal(t, int32(1460), n.GetMtu())
	assert.Equal(t, "REGIONAL", n.GetRoutingConfig().GetRoutingMode())
	assert.Equal(t, "compute#network", n.GetKind())
	assert.NotEmpty(t, n.GetCreationTimestamp())
	assert.Contains(t, n.GetSelfLinkWithId(), "/global/networks/")
	assert.GreaterOrEqual(t, len(n.GetSubnetworks()), 10)

	sub, err := e.subnets(t).
		Get(t.Context(), &computepb.GetSubnetworkRequest{Project: project, Region: region, Subnetwork: "auto-net"})
	require.NoError(t, err)
	assert.Equal(t, "10.128.0.0/20", sub.GetIpCidrRange())
	assert.Equal(t, "10.128.0.1", sub.GetGatewayAddress())

	routes := e.routes(t).List(t.Context(), &computepb.ListRoutesRequest{Project: project})
	dests := map[string]string{}

	for r, rerr := routes.Next(); !errors.Is(rerr, iterator.Done); r, rerr = routes.Next() {
		require.NoError(t, rerr)
		assert.Contains(t, r.GetName(), "default-route-")
		dests[r.GetDestRange()] = r.GetRouteType()
	}

	assert.Contains(t, dests, "0.0.0.0/0")
	assert.Equal(t, "SUBNET", dests["10.128.0.0/20"])

	op, err = nets.Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "auto-net"})
	e.done(t, op, err)

	_, err = nets.Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "auto-net"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	_, err = e.subnets(t).
		Get(t.Context(), &computepb.GetSubnetworkRequest{Project: project, Region: region, Subnetwork: "auto-net"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	_, err = e.routes(t).List(t.Context(), &computepb.ListRoutesRequest{Project: project}).Next()
	assert.ErrorIs(t, err, iterator.Done)
}

func TestNetworkInsertErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		net    *computepb.Network
		name   string
		reason string
		status int
	}{
		{&computepb.Network{}, "no name", "required", 400},
		{&computepb.Network{Name: s("Bad_Name")}, "bad name", "invalid", 400},
		{&computepb.Network{Name: s("dup")}, "duplicate", "alreadyExists", 409},
		{&computepb.Network{Name: s("legacy"), IPv4Range: s("10.0.0.0/8")}, "legacy", "invalid", 400},
		{&computepb.Network{Name: s("mtu"), Mtu: proto.Int32(100)}, "mtu", "invalid", 400},
		{
			&computepb.Network{Name: s("rm"), RoutingConfig: &computepb.NetworkRoutingConfig{RoutingMode: s("WORLD")}},
			"routing mode",
			"invalid",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "dup", false)

			_, err := e.networks(t).
				Insert(t.Context(), &computepb.InsertNetworkRequest{Project: project, NetworkResource: tt.net})
			assertAPIError(t, err, tt.status, tt.reason)
		})
	}
}

func TestNetworkCustomModeAndPatch(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "custom", false)

	n, err := e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "custom"})
	require.NoError(t, err)
	assert.False(t, n.GetAutoCreateSubnetworks())
	assert.Empty(t, n.GetSubnetworks())

	op, err := e.networks(t).Patch(t.Context(), &computepb.PatchNetworkRequest{
		Project: project, Network: "custom",
		NetworkResource: &computepb.Network{RoutingConfig: &computepb.NetworkRoutingConfig{RoutingMode: s("GLOBAL")}},
	})
	e.done(t, op, err)

	n, err = e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "custom"})
	require.NoError(t, err)
	assert.Equal(t, "GLOBAL", n.GetRoutingConfig().GetRoutingMode())

	_, err = e.networks(t).
		SwitchToCustomMode(
			t.Context(),
			&computepb.SwitchToCustomModeNetworkRequest{Project: project, Network: "custom"},
		)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	e.network(t, "auto", true)

	op, err = e.networks(t).
		SwitchToCustomMode(
			t.Context(),
			&computepb.SwitchToCustomModeNetworkRequest{Project: project, Network: "auto"},
		)
	e.done(t, op, err)

	n, err = e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "auto"})
	require.NoError(t, err)
	assert.False(t, n.GetAutoCreateSubnetworks())
	assert.NotEmpty(t, n.GetSubnetworks())

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "auto"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}

func TestSubnetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sub    *computepb.Subnetwork
		name   string
		reason string
		status int
	}{
		{&computepb.Subnetwork{Name: s("ok"), IpCidrRange: s("10.1.0.0/24")}, "valid", "", 200},
		{
			&computepb.Subnetwork{
				Name:        s("secondary"),
				IpCidrRange: s("10.2.0.0/24"),
				SecondaryIpRanges: []*computepb.SubnetworkSecondaryRange{
					{RangeName: s("pods"), IpCidrRange: s("192.168.0.0/20")},
				},
			},
			"secondary ranges",
			"",
			200,
		},
		{&computepb.Subnetwork{Name: s("overlap"), IpCidrRange: s("10.0.128.0/24")}, "overlap", "invalid", 400},
		{
			&computepb.Subnetwork{
				Name:        s("secoverlap"),
				IpCidrRange: s("10.3.0.0/24"),
				SecondaryIpRanges: []*computepb.SubnetworkSecondaryRange{
					{RangeName: s("pods"), IpCidrRange: s("10.6.0.0/24")},
				},
			},
			"secondary overlap",
			"invalid",
			400,
		},
		{&computepb.Subnetwork{Name: s("public"), IpCidrRange: s("8.8.8.0/24")}, "public range", "invalid", 400},
		{&computepb.Subnetwork{Name: s("host"), IpCidrRange: s("10.4.0.1/24")}, "host bits", "invalid", 400},
		{&computepb.Subnetwork{Name: s("tiny"), IpCidrRange: s("10.5.0.0/30")}, "too small", "invalid", 400},
		{&computepb.Subnetwork{Name: s("garbage"), IpCidrRange: s("nope")}, "garbage", "invalid", 400},
		{&computepb.Subnetwork{Name: s("base"), IpCidrRange: s("10.6.0.0/24")}, "duplicate name", "alreadyExists", 409},
		{&computepb.Subnetwork{IpCidrRange: s("10.7.0.0/24")}, "no name", "required", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)
			e.subnet(t, "base", "10.0.128.0/20")
			e.subnet(t, "base2", "10.6.0.0/24")

			tt.sub.Network = s("global/networks/vpc")

			_, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
				Project: project, Region: region, SubnetworkResource: tt.sub,
			})
			if tt.status == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assertAPIError(t, err, tt.status, tt.reason)
		})
	}
}

func TestSubnetExpandPatchDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)
	e.subnet(t, "s1", "10.0.0.0/24")
	e.subnet(t, "s2", "10.0.2.0/24")

	subs := e.subnets(t)

	tests := []struct {
		name   string
		cidr   string
		reason string
		status int
	}{
		{"shrink", "10.0.0.0/25", "invalid", 400},
		{"unrelated", "10.9.0.0/23", "invalid", 400},
		{"overlaps neighbour", "10.0.0.0/22", "invalid", 400},
		{"expand", "10.0.0.0/23", "", 200},
	}

	for _, tt := range tests {
		op, err := subs.ExpandIpCidrRange(t.Context(), &computepb.ExpandIpCidrRangeSubnetworkRequest{
			Project:    project,
			Region:     region,
			Subnetwork: "s1",
			SubnetworksExpandIpCidrRangeRequestResource: &computepb.SubnetworksExpandIpCidrRangeRequest{
				IpCidrRange: s(tt.cidr),
			},
		})
		if tt.status == http.StatusOK {
			e.done(t, op, err)

			continue
		}

		assertAPIError(t, err, tt.status, tt.reason)
	}

	got, err := subs.Get(
		t.Context(),
		&computepb.GetSubnetworkRequest{Project: project, Region: region, Subnetwork: "s1"},
	)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.0/23", got.GetIpCidrRange())

	fp := got.GetFingerprint()
	op, err := subs.SetPrivateIpGoogleAccess(t.Context(), &computepb.SetPrivateIpGoogleAccessSubnetworkRequest{
		Project:    project,
		Region:     region,
		Subnetwork: "s1",
		SubnetworksSetPrivateIpGoogleAccessRequestResource: &computepb.SubnetworksSetPrivateIpGoogleAccessRequest{
			PrivateIpGoogleAccess: new(true),
		},
	})
	e.done(t, op, err)

	got, err = subs.Get(
		t.Context(),
		&computepb.GetSubnetworkRequest{Project: project, Region: region, Subnetwork: "s1"},
	)
	require.NoError(t, err)
	assert.True(t, got.GetPrivateIpGoogleAccess())
	assert.NotEqual(t, fp, got.GetFingerprint())

	op, err = subs.Patch(t.Context(), &computepb.PatchSubnetworkRequest{
		Project: project, Region: region, Subnetwork: "s1",
		SubnetworkResource: &computepb.Subnetwork{
			Fingerprint: got.Fingerprint,
			SecondaryIpRanges: []*computepb.SubnetworkSecondaryRange{
				{RangeName: s("svc"), IpCidrRange: s("172.16.0.0/20")},
			},
		},
	})
	e.done(t, op, err)

	_, err = subs.Patch(t.Context(), &computepb.PatchSubnetworkRequest{
		Project: project, Region: region, Subnetwork: "s1",
		SubnetworkResource: &computepb.Subnetwork{Fingerprint: s("stale"), Description: s("x")},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = subs.Delete(
		t.Context(),
		&computepb.DeleteSubnetworkRequest{Project: project, Region: region, Subnetwork: "s1"},
	)
	e.done(t, op, err)

	_, err = subs.Delete(
		t.Context(),
		&computepb.DeleteSubnetworkRequest{Project: project, Region: region, Subnetwork: "s1"},
	)
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	e.clock.Advance(time.Second)
}

func TestAutoSubnetsRejectManualSubnets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "auto", true)

	_, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: s("mine"), Network: s("global/networks/auto"), IpCidrRange: s("10.200.0.0/24"),
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
}
