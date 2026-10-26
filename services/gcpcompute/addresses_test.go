package gcpcompute_test

import (
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRegionalAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr   *computepb.Address
		check  func(t *testing.T, a *computepb.Address)
		name   string
		reason string
		status int
	}{
		{
			addr: &computepb.Address{Name: s("ext")}, name: "external default", status: 200,
			check: func(t *testing.T, a *computepb.Address) {
				t.Helper()

				assert.Equal(t, "EXTERNAL", a.GetAddressType())
				assert.True(t, strings.HasPrefix(a.GetAddress(), "34."))
				assert.Equal(t, "PREMIUM", a.GetNetworkTier())
				assert.Equal(t, "RESERVED", a.GetStatus())
				assert.Equal(t, "IPV4", a.GetIpVersion())
			},
		},
		{
			addr: &computepb.Address{
				Name:        s("int"),
				AddressType: s("INTERNAL"),
				Subnetwork:  s("regions/" + region + "/subnetworks/sub"),
			},
			name:   "internal from subnet",
			status: 200,
			check: func(t *testing.T, a *computepb.Address) {
				t.Helper()

				assert.Equal(t, "10.10.0.2", a.GetAddress())
				assert.Equal(t, "GCE_ENDPOINT", a.GetPurpose())
				assert.Contains(t, a.GetSubnetwork(), "/subnetworks/sub")
			},
		},
		{
			addr: &computepb.Address{
				Name:        s("pinned"),
				AddressType: s("INTERNAL"),
				Address:     s("10.10.0.77"),
				Subnetwork:  s("regions/" + region + "/subnetworks/sub"),
			},
			name:   "internal explicit",
			status: 200,
			check: func(t *testing.T, a *computepb.Address) {
				t.Helper()
				assert.Equal(t, "10.10.0.77", a.GetAddress())
			},
		},
		{
			addr: &computepb.Address{
				Name:        s("outside"),
				AddressType: s("INTERNAL"),
				Address:     s("10.99.0.5"),
				Subnetwork:  s("regions/" + region + "/subnetworks/sub"),
			},
			name:   "internal outside range",
			status: 400,
			reason: "invalid",
		},
		{
			addr: &computepb.Address{
				Name:        s("gateway"),
				AddressType: s("INTERNAL"),
				Address:     s("10.10.0.1"),
				Subnetwork:  s("regions/" + region + "/subnetworks/sub"),
			},
			name:   "internal reserved gateway",
			status: 400,
			reason: "invalid",
		},
		{
			addr: &computepb.Address{
				Name:        s("last"),
				AddressType: s("INTERNAL"),
				Address:     s("10.10.0.255"),
				Subnetwork:  s("regions/" + region + "/subnetworks/sub"),
			},
			name:   "internal broadcast",
			status: 400,
			reason: "invalid",
		},
		{
			addr:   &computepb.Address{Name: s("nosub"), AddressType: s("INTERNAL")},
			name:   "internal without subnet",
			status: 400,
			reason: "required",
		},
		{addr: &computepb.Address{Name: s("v6"), IpVersion: s("IPV6")}, name: "ipv6", status: 400, reason: "invalid"},
		{
			addr:   &computepb.Address{Name: s("tier"), NetworkTier: s("SILVER")},
			name:   "bad tier",
			status: 400,
			reason: "invalid",
		},
		{
			addr:   &computepb.Address{Name: s("weird"), AddressType: s("SIDE")},
			name:   "bad type",
			status: 400,
			reason: "invalid",
		},
		{addr: &computepb.Address{Name: s("dup")}, name: "duplicate", status: 409, reason: "alreadyExists"},
		{addr: &computepb.Address{Name: s("Bad")}, name: "bad name", status: 400, reason: "invalid"},
		{
			addr:   &computepb.Address{Name: s("lbl"), Labels: map[string]string{"UPPER": "x"}},
			name:   "bad label",
			status: 400,
			reason: "invalid",
		},
		{
			addr:   &computepb.Address{Name: s("badip"), Address: s("not-an-ip")},
			name:   "bad explicit ip",
			status: 400,
			reason: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)
			e.subnet(t, "sub", "10.10.0.0/24")

			addrs := e.addresses(t)
			op, err := addrs.Insert(
				t.Context(),
				&computepb.InsertAddressRequest{
					Project:         project,
					Region:          region,
					AddressResource: &computepb.Address{Name: s("dup")},
				},
			)
			e.done(t, op, err)

			op, err = addrs.Insert(
				t.Context(),
				&computepb.InsertAddressRequest{Project: project, Region: region, AddressResource: tt.addr},
			)
			if tt.status != http.StatusOK {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			e.done(t, op, err)

			got, err := addrs.Get(
				t.Context(),
				&computepb.GetAddressRequest{Project: project, Region: region, Address: tt.addr.GetName()},
			)
			require.NoError(t, err)
			assert.Equal(t, "compute#address", got.GetKind())
			assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/addresses/"+tt.addr.GetName())
			tt.check(t, got)
		})
	}
}

func TestAddressUniquenessAndLabels(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	addrs := e.addresses(t)

	seen := map[string]bool{}

	for _, n := range []string{"a", "b", "c"} {
		op, err := addrs.Insert(
			t.Context(),
			&computepb.InsertAddressRequest{
				Project:         project,
				Region:          region,
				AddressResource: &computepb.Address{Name: s(n)},
			},
		)
		e.done(t, op, err)

		got, err := addrs.Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: n})
		require.NoError(t, err)
		assert.False(t, seen[got.GetAddress()], "address %s reused", got.GetAddress())
		seen[got.GetAddress()] = true
	}

	a, err := addrs.Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a"})
	require.NoError(t, err)

	_, err = addrs.Insert(
		t.Context(),
		&computepb.InsertAddressRequest{
			Project:         project,
			Region:          region,
			AddressResource: &computepb.Address{Name: s("steal"), Address: s(a.GetAddress())},
		},
	)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = addrs.SetLabels(t.Context(), &computepb.SetLabelsAddressRequest{
		Project:  project,
		Region:   region,
		Resource: "a",
		RegionSetLabelsRequestResource: &computepb.RegionSetLabelsRequest{
			Labels:           map[string]string{"env": "x"},
			LabelFingerprint: s("stale"),
		},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	op, err := addrs.SetLabels(t.Context(), &computepb.SetLabelsAddressRequest{
		Project:  project,
		Region:   region,
		Resource: "a",
		RegionSetLabelsRequestResource: &computepb.RegionSetLabelsRequest{
			Labels:           map[string]string{"env": "x"},
			LabelFingerprint: a.LabelFingerprint,
		},
	})
	e.done(t, op, err)

	b, err := addrs.Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "x"}, b.GetLabels())
	assert.NotEqual(t, a.GetLabelFingerprint(), b.GetLabelFingerprint())

	op, err = addrs.Delete(t.Context(), &computepb.DeleteAddressRequest{Project: project, Region: region, Address: "a"})
	e.done(t, op, err)

	_, err = addrs.Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestGlobalAddresses(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ga := e.globalAddresses(t)

	op, err := ga.Insert(
		t.Context(),
		&computepb.InsertGlobalAddressRequest{Project: project, AddressResource: &computepb.Address{Name: s("lb")}},
	)
	e.done(t, op, err)

	got, err := ga.Get(t.Context(), &computepb.GetGlobalAddressRequest{Project: project, Address: "lb"})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got.GetAddress(), "34."))
	assert.Contains(t, got.GetSelfLink(), "/global/addresses/lb")
	assert.Empty(t, got.GetRegion())

	_, err = ga.Insert(
		t.Context(),
		&computepb.InsertGlobalAddressRequest{
			Project:         project,
			AddressResource: &computepb.Address{Name: s("int"), AddressType: s("INTERNAL")},
		},
	)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = e.addresses(t).
		Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "lb"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = ga.Delete(t.Context(), &computepb.DeleteGlobalAddressRequest{Project: project, Address: "lb"})
	e.done(t, op, err)
}

func TestRouterNat(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)
	e.subnet(t, "sub", "10.10.0.0/24")

	addrs := e.addresses(t)
	op, err := addrs.Insert(
		t.Context(),
		&computepb.InsertAddressRequest{
			Project:         project,
			Region:          region,
			AddressResource: &computepb.Address{Name: s("nat-ip")},
		},
	)
	e.done(t, op, err)

	nat, err := addrs.Get(
		t.Context(),
		&computepb.GetAddressRequest{Project: project, Region: region, Address: "nat-ip"},
	)
	require.NoError(t, err)

	routers := e.routers(t)
	op, err = routers.Insert(t.Context(), &computepb.InsertRouterRequest{
		Project: project, Region: region, RouterResource: &computepb.Router{
			Name: s("rtr"), Network: s("global/networks/vpc"), Bgp: &computepb.RouterBgp{Asn: proto.Uint32(64514)},
			Nats: []*computepb.RouterNat{
				{
					Name:                          s("auto"),
					NatIpAllocateOption:           s("AUTO_ONLY"),
					SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_IP_RANGES"),
				},
				{
					Name: s(
						"manual",
					),
					NatIpAllocateOption:           s("MANUAL_ONLY"),
					SourceSubnetworkIpRangesToNat: s("LIST_OF_SUBNETWORKS"),
					NatIps:                        []string{nat.GetSelfLink()},
					Subnetworks: []*computepb.RouterNatSubnetworkToNat{
						{
							Name:                s("regions/" + region + "/subnetworks/sub"),
							SourceIpRangesToNat: []string{"ALL_IP_RANGES"},
						},
					},
				},
			},
		},
	})
	e.done(t, op, err)

	r, err := routers.Get(t.Context(), &computepb.GetRouterRequest{Project: project, Region: region, Router: "rtr"})
	require.NoError(t, err)
	assert.Equal(t, uint32(64514), r.GetBgp().GetAsn())
	assert.Equal(t, "DEFAULT", r.GetBgp().GetAdvertiseMode())
	require.Len(t, r.GetNats(), 2)
	assert.Equal(t, int32(30), r.GetNats()[0].GetUdpIdleTimeoutSec())
	assert.Equal(t, int32(1200), r.GetNats()[0].GetTcpEstablishedIdleTimeoutSec())
	assert.Equal(t, int32(64), r.GetNats()[0].GetMinPortsPerVm())
	assert.Contains(t, r.GetNats()[1].GetSubnetworks()[0].GetName(), "/subnetworks/sub")

	status, err := routers.GetRouterStatus(
		t.Context(),
		&computepb.GetRouterStatusRouterRequest{Project: project, Region: region, Router: "rtr"},
	)
	require.NoError(t, err)
	require.Len(t, status.GetResult().GetNatStatus(), 2)
	assert.Len(t, status.GetResult().GetNatStatus()[0].GetAutoAllocatedNatIps(), 1)
	assert.Equal(t, []string{nat.GetAddress()}, status.GetResult().GetNatStatus()[1].GetUserAllocatedNatIps())

	cur, err := addrs.Get(
		t.Context(),
		&computepb.GetAddressRequest{Project: project, Region: region, Address: "nat-ip"},
	)
	require.NoError(t, err)
	assert.Equal(t, "IN_USE", cur.GetStatus())
	assert.Contains(t, cur.GetUsers()[0], "/routers/rtr")

	_, err = addrs.Delete(
		t.Context(),
		&computepb.DeleteAddressRequest{Project: project, Region: region, Address: "nat-ip"},
	)
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	autoIP := status.GetResult().GetNatStatus()[0].GetAutoAllocatedNatIps()[0]

	op, err = routers.Patch(t.Context(), &computepb.PatchRouterRequest{
		Project: project, Region: region, Router: "rtr", RouterResource: &computepb.Router{
			Nats: []*computepb.RouterNat{
				{
					Name:                          s("auto"),
					NatIpAllocateOption:           s("AUTO_ONLY"),
					SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_PRIMARY_IP_RANGES"),
					MinPortsPerVm:                 proto.Int32(128),
				},
			},
		},
	})
	e.done(t, op, err)

	status, err = routers.GetRouterStatus(
		t.Context(),
		&computepb.GetRouterStatusRouterRequest{Project: project, Region: region, Router: "rtr"},
	)
	require.NoError(t, err)
	require.Len(t, status.GetResult().GetNatStatus(), 1)
	assert.Equal(t, []string{autoIP}, status.GetResult().GetNatStatus()[0].GetAutoAllocatedNatIps())

	op, err = addrs.Delete(
		t.Context(),
		&computepb.DeleteAddressRequest{Project: project, Region: region, Address: "nat-ip"},
	)
	e.done(t, op, err)

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = routers.Delete(
		t.Context(),
		&computepb.DeleteRouterRequest{Project: project, Region: region, Router: "rtr"},
	)
	e.done(t, op, err)
}

func TestRouterValidation(t *testing.T) {
	t.Parallel()

	nat := func(n *computepb.RouterNat) []*computepb.RouterNat { return []*computepb.RouterNat{n} }

	tests := []struct {
		router *computepb.Router
		name   string
		reason string
		status int
	}{
		{&computepb.Router{Name: s("r")}, "no network", "required", 400},
		{&computepb.Router{Name: s("r"), Network: s("global/networks/missing")}, "missing network", "notFound", 404},
		{
			&computepb.Router{
				Name:    s("r"),
				Network: s("global/networks/vpc"),
				Bgp:     &computepb.RouterBgp{Asn: proto.Uint32(0)},
			},
			"asn",
			"invalid",
			400,
		},
		{
			&computepb.Router{
				Name:    s("r"),
				Network: s("global/networks/vpc"),
				Nats:    nat(&computepb.RouterNat{Name: s("n"), NatIpAllocateOption: s("AUTO_ONLY")}),
			},
			"no source mode",
			"invalid",
			400,
		},
		{
			&computepb.Router{
				Name:    s("r"),
				Network: s("global/networks/vpc"),
				Nats: nat(
					&computepb.RouterNat{
						Name:                          s("n"),
						NatIpAllocateOption:           s("MANUAL_ONLY"),
						SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_IP_RANGES"),
					},
				),
			},
			"manual without ips",
			"required",
			400,
		},
		{
			&computepb.Router{
				Name:    s("r"),
				Network: s("global/networks/vpc"),
				Nats: nat(
					&computepb.RouterNat{
						Name:                          s("n"),
						NatIpAllocateOption:           s("AUTO_ONLY"),
						SourceSubnetworkIpRangesToNat: s("LIST_OF_SUBNETWORKS"),
					},
				),
			},
			"list without subnets",
			"required",
			400,
		},
		{
			&computepb.Router{
				Name:    s("r"),
				Network: s("global/networks/vpc"),
				Nats: nat(
					&computepb.RouterNat{
						Name:                          s("n"),
						NatIpAllocateOption:           s("AUTO_ONLY"),
						SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_IP_RANGES"),
						NatIps:                        []string{"x"},
					},
				),
			},
			"auto with ips",
			"invalid",
			400,
		},
		{&computepb.Router{Name: s("r"), Network: s("global/networks/vpc"), Nats: []*computepb.RouterNat{
			{
				Name:                          s("n"),
				NatIpAllocateOption:           s("AUTO_ONLY"),
				SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_IP_RANGES"),
			},
			{
				Name:                          s("n"),
				NatIpAllocateOption:           s("AUTO_ONLY"),
				SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_IP_RANGES"),
			},
		}}, "duplicate nat", "invalid", 400},
		{
			&computepb.Router{Name: s("dup"), Network: s("global/networks/vpc")},
			"duplicate router",
			"alreadyExists",
			409,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)

			op, err := e.routers(t).Insert(t.Context(), &computepb.InsertRouterRequest{
				Project:        project,
				Region:         region,
				RouterResource: &computepb.Router{Name: s("dup"), Network: s("global/networks/vpc")},
			})
			e.done(t, op, err)

			_, err = e.routers(t).
				Insert(
					t.Context(),
					&computepb.InsertRouterRequest{Project: project, Region: region, RouterResource: tt.router},
				)
			assertAPIError(t, err, tt.status, tt.reason)
		})
	}
}
