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

func TestGlobalInternalPeeringRanges(t *testing.T) {
	t.Parallel()

	peering := func(name string, prefix int32) *computepb.Address {
		return &computepb.Address{
			Name: s(name), AddressType: s("INTERNAL"), Purpose: s("VPC_PEERING"), Network: s("global/networks/vpc"),
			PrefixLength: proto.Int32(prefix),
		}
	}

	tests := []struct {
		name   string
		addr   *computepb.Address
		want   string
		status int
	}{
		{name: "first free block", addr: peering("r", 16), want: "10.0.0.0"},
		{name: "explicit start", addr: func() *computepb.Address {
			a := peering("r", 24)
			a.Address = s("172.20.5.0")

			return a
		}(), want: "172.20.5.0"},
		{name: "second block follows the first", addr: peering("r", 24), want: "10.0.1.0"},
		{name: "unaligned", addr: func() *computepb.Address {
			a := peering("r", 24)
			a.Address = s("172.20.5.7")

			return a
		}(), status: http.StatusBadRequest},
		{name: "overlaps subnet", addr: func() *computepb.Address {
			a := peering("r", 24)
			a.Address = s("10.10.0.0")

			return a
		}(), status: http.StatusBadRequest},
		{name: "public range", addr: func() *computepb.Address {
			a := peering("r", 24)
			a.Address = s("8.8.8.0")

			return a
		}(), status: http.StatusBadRequest},
		{name: "needs prefix", addr: peering("r", 0), status: http.StatusBadRequest},
		{name: "prefix too small", addr: peering("r", 4), status: http.StatusBadRequest},
		{name: "needs network", addr: &computepb.Address{
			Name: s("r"), AddressType: s("INTERNAL"), Purpose: s("VPC_PEERING"), PrefixLength: proto.Int32(16),
		}, status: http.StatusBadRequest},
		{name: "wrong purpose", addr: func() *computepb.Address {
			a := peering("r", 16)
			a.Purpose = s("GCE_ENDPOINT")

			return a
		}(), status: http.StatusBadRequest},
		{name: "unknown network", addr: func() *computepb.Address {
			a := peering("r", 16)
			a.Network = s("global/networks/none")

			return a
		}(), status: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			ga := e.globalAddresses(t)

			if tt.name == "second block follows the first" {
				op, err := ga.Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: peering("first", 24)})
				e.done(t, op, err)
			}

			op, err := ga.Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: tt.addr})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, err := ga.Get(t.Context(), &computepb.GetGlobalAddressRequest{Project: project, Address: "r"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.GetAddress())
			assert.Equal(t, "INTERNAL", got.GetAddressType())
			assert.Equal(t, "VPC_PEERING", got.GetPurpose())
			assert.Equal(t, tt.addr.GetPrefixLength(), got.GetPrefixLength())
			assert.Contains(t, got.GetNetwork(), "/global/networks/vpc")
			assert.Equal(t, "RESERVED", got.GetStatus())
		})
	}
}

func TestPeeringRangeBlocksSubnetAndNetworkDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)

	ga := e.globalAddresses(t)

	op, err := ga.Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: &computepb.Address{
		Name: s("psa"), AddressType: s("INTERNAL"), Purpose: s("VPC_PEERING"), Network: s("global/networks/vpc"),
		PrefixLength: proto.Int32(16), Address: s("10.50.0.0"),
	}})
	e.done(t, op, err)

	_, err = e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: s("clash"), Network: s("global/networks/vpc"), IpCidrRange: s("10.50.1.0/24"),
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = ga.Delete(t.Context(), &computepb.DeleteGlobalAddressRequest{Project: project, Address: "psa"})
	e.done(t, op, err)

	op, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	e.done(t, op, err)
}

func TestIPv6Addresses(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	ga := e.globalAddresses(t)

	op, err := ga.Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: &computepb.Address{
		Name: s("v6a"), IpVersion: s("IPV6"),
	}})
	e.done(t, op, err)

	op, err = ga.Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: &computepb.Address{
		Name: s("v6b"), IpVersion: s("IPV6"),
	}})
	e.done(t, op, err)

	a, err := ga.Get(t.Context(), &computepb.GetGlobalAddressRequest{Project: project, Address: "v6a"})
	require.NoError(t, err)
	b, err := ga.Get(t.Context(), &computepb.GetGlobalAddressRequest{Project: project, Address: "v6b"})
	require.NoError(t, err)

	assert.True(t, strings.Contains(a.GetAddress(), ":"))
	assert.NotEqual(t, a.GetAddress(), b.GetAddress())
	assert.Equal(t, "IPV6", a.GetIpVersion())
	assert.Equal(t, int32(96), a.GetPrefixLength())
	assert.Equal(t, "EXTERNAL", a.GetAddressType())

	tests := []struct {
		name string
		res  *computepb.Address
	}{
		{name: "bad prefix", res: &computepb.Address{Name: s("x"), IpVersion: s("IPV6"), PrefixLength: proto.Int32(64)}},
		{name: "ipv4 literal", res: &computepb.Address{Name: s("x"), IpVersion: s("IPV6"), Address: s("1.2.3.4")}},
		{name: "taken", res: &computepb.Address{Name: s("x"), IpVersion: s("IPV6"), Address: s(a.GetAddress())}},
		{name: "internal", res: &computepb.Address{Name: s("x"), IpVersion: s("IPV6"), AddressType: s("INTERNAL")}},
		{name: "bad version", res: &computepb.Address{Name: s("x"), IpVersion: s("IPV9")}},
	}

	for _, tt := range tests {
		_, err = ga.Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: tt.res})
		assertAPIError(t, err, http.StatusBadRequest, "")
	}

	_, err = e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{Name: s("r6"), IpVersion: s("IPV6")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{Name: s("p"), PrefixLength: proto.Int32(24)},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
}
