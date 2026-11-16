package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) provisionedRange(t *testing.T, v4, v6 string) core.ByoipRange {
	t.Helper()

	d := core.CreateByoipRangeDetails{CompartmentId: new(testComp)}
	if v4 != "" {
		d.CidrBlock = new(v4)
	}

	if v6 != "" {
		d.Ipv6CidrBlock = new(v6)
	}

	r, err := e.client.CreateByoipRange(t.Context(), core.CreateByoipRangeRequest{CreateByoipRangeDetails: d})
	require.NoError(t, err)

	_, err = e.client.ValidateByoipRange(t.Context(), core.ValidateByoipRangeRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	e.settle()

	return r.ByoipRange
}

func TestByoipRangeCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		v4   string
		v6   string
		ok   bool
	}{
		{"public v4", "203.0.113.0/24", "", true},
		{"public v4 big", "11.0.0.0/8", "", true},
		{"too small", "203.0.113.0/25", "", false},
		{"too large", "10.0.0.0/7", "", false},
		{"private", "10.1.0.0/16", "", false},
		{"not masked", "203.0.113.1/24", "", false},
		{"v6", "", "2001:db8:1::/48", true},
		{"v6 too small", "", "2001:db8:1::/56", false},
		{"both", "203.0.113.0/24", "2001:db8:1::/48", false},
		{"neither", "", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			d := core.CreateByoipRangeDetails{CompartmentId: new(testComp)}

			if tc.v4 != "" {
				d.CidrBlock = new(tc.v4)
			}

			if tc.v6 != "" {
				d.Ipv6CidrBlock = new(tc.v6)
			}

			r, err := e.client.CreateByoipRange(t.Context(), core.CreateByoipRangeRequest{CreateByoipRangeDetails: d})
			if !tc.ok {
				assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, *r.ValidationToken)
			assert.Equal(t, core.ByoipRangeLifecycleDetailsCreating, r.LifecycleDetails)
			assert.EqualValues(t, 31898, *r.OriginAsn.Asn)
		})
	}
}

func TestByoipRangeLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	r := e.provisionedRange(t, "203.0.113.0/24", "")
	get := func() core.ByoipRange {
		g, err := e.client.GetByoipRange(ctx, core.GetByoipRangeRequest{ByoipRangeId: r.Id})
		require.NoError(t, err)

		return g.ByoipRange
	}

	assert.Equal(t, core.ByoipRangeLifecycleDetailsProvisioned, get().LifecycleDetails)
	assert.NotNil(t, get().TimeValidated)

	_, err := e.client.WithdrawByoipRange(ctx, core.WithdrawByoipRangeRequest{ByoipRangeId: r.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "only an advertised range can be withdrawn")

	_, err = e.client.AdvertiseByoipRange(ctx, core.AdvertiseByoipRangeRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, core.ByoipRangeLifecycleDetailsAdvertising, get().LifecycleDetails)

	_, err = e.client.DeleteByoipRange(ctx, core.DeleteByoipRangeRequest{ByoipRangeId: r.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "advertising")

	e.settle()
	assert.Equal(t, core.ByoipRangeLifecycleDetailsActive, get().LifecycleDetails)
	assert.NotNil(t, get().TimeAdvertised)

	_, err = e.client.WithdrawByoipRange(ctx, core.WithdrawByoipRangeRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, core.ByoipRangeLifecycleDetailsProvisioned, get().LifecycleDetails)
	assert.NotNil(t, get().TimeWithdrawn)

	_, err = e.client.CreateByoipRange(
		ctx,
		core.CreateByoipRangeRequest{CompartmentId: new(testComp), CidrBlock: new("203.0.113.0/24")},
	)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "overlap")

	_, err = e.client.DeleteByoipRange(ctx, core.DeleteByoipRangeRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, core.ByoipRangeLifecycleStateDeleted, get().LifecycleState)

	l, err := e.client.ListByoipRanges(ctx, core.ListByoipRangesRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)
}

func TestPublicIPPoolFromByoipRange(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	r := e.provisionedRange(t, "203.0.113.0/24", "")

	pool, err := e.client.CreatePublicIpPool(ctx, core.CreatePublicIpPoolRequest{
		CompartmentId: new(testComp),
		DisplayName:   new("pool"),
	})
	require.NoError(t, err)
	assert.Empty(t, pool.CidrBlocks)

	add := func(cidr string) error {
		_, aerr := e.client.AddPublicIpPoolCapacity(ctx, core.AddPublicIpPoolCapacityRequest{
			PublicIpPoolId: pool.Id, ByoipRangeId: r.Id, CidrBlock: new(cidr),
		})

		return aerr
	}

	assert.Equal(t, http.StatusBadRequest, svcErr(t, add("198.51.100.0/28")).GetHTTPStatusCode(), "outside the range")
	require.NoError(t, add("203.0.113.0/30"))
	assert.Equal(t, "Conflict", svcErr(t, add("203.0.113.0/29")).GetCode(), "overlaps capacity")

	allocated, err := e.client.ListByoipAllocatedRanges(ctx, core.ListByoipAllocatedRangesRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	require.Len(t, allocated.Items, 1)
	assert.Equal(t, *pool.Id, *allocated.Items[0].PublicIpPoolId)

	byRange, err := e.client.ListPublicIpPools(ctx, core.ListPublicIpPoolsRequest{
		CompartmentId: new(testComp), ByoipRangeId: r.Id,
	})
	require.NoError(t, err)
	require.Len(t, byRange.Items, 1)

	_, err = e.client.DeleteByoipRange(ctx, core.DeleteByoipRangeRequest{ByoipRangeId: r.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a pool holds a sub-range")

	addrs := make([]string, 0, 2)

	for range 2 {
		ip, cerr := e.client.CreatePublicIp(
			ctx,
			core.CreatePublicIpRequest{
				CompartmentId: new(
					testComp,
				),
				Lifetime:       core.CreatePublicIpDetailsLifetimeReserved,
				PublicIpPoolId: pool.Id,
			},
		)
		require.NoError(t, cerr)
		assert.Equal(t, *pool.Id, *ip.PublicIpPoolId)

		addrs = append(addrs, *ip.IpAddress)
	}

	assert.Equal(t, []string{"203.0.113.1", "203.0.113.2"}, addrs, "a /30 pool offers its two host addresses")

	_, err = e.client.CreatePublicIp(
		ctx,
		core.CreatePublicIpRequest{
			CompartmentId:  new(testComp),
			Lifetime:       core.CreatePublicIpDetailsLifetimeReserved,
			PublicIpPoolId: pool.Id,
		},
	)
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode(), "pool exhausted")

	_, err = e.client.RemovePublicIpPoolCapacity(ctx, core.RemovePublicIpPoolCapacityRequest{
		PublicIpPoolId: pool.Id, CidrBlock: new("203.0.113.0/30"),
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "reserved IPs use the sub-range")

	_, err = e.client.DeletePublicIpPool(ctx, core.DeletePublicIpPoolRequest{PublicIpPoolId: pool.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "pool still has capacity")
}

func TestByoipv6AllocationToVcn(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	r := e.provisionedRange(t, "", "2001:db8:1::/48")

	v, err := e.client.CreateVcn(
		ctx,
		core.CreateVcnRequest{
			CompartmentId: new(testComp),
			CidrBlocks:    []string{"10.0.0.0/16"},
			Byoipv6CidrDetails: []core.Byoipv6CidrDetails{
				{Byoipv6RangeId: r.Id, Ipv6CidrBlock: new("2001:db8:1::/64")},
			},
		},
	)
	require.NoError(t, err)
	assert.Contains(t, v.Ipv6CidrBlocks, "2001:db8:1::/64")

	g, err := e.client.GetByoipRange(ctx, core.GetByoipRangeRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	require.Len(t, g.ByoipRangeVcnIpv6Allocations, 1)
	assert.Equal(t, *v.Id, *g.ByoipRangeVcnIpv6Allocations[0].VcnId)

	_, err = e.client.DeleteByoipRange(ctx, core.DeleteByoipRangeRequest{ByoipRangeId: r.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a VCN holds a prefix")

	e.settle()

	_, err = e.client.AddIpv6VcnCidr(
		ctx,
		core.AddIpv6VcnCidrRequest{
			VcnId:             v.Id,
			Byoipv6CidrDetail: &core.Byoipv6CidrDetails{Byoipv6RangeId: r.Id, Ipv6CidrBlock: new("2001:db8:1::/64")},
		},
	)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "overlap with the first allocation")

	_, err = e.client.AddIpv6VcnCidr(
		ctx,
		core.AddIpv6VcnCidrRequest{
			VcnId:             v.Id,
			Byoipv6CidrDetail: &core.Byoipv6CidrDetails{Byoipv6RangeId: r.Id, Ipv6CidrBlock: new("2001:db8:2::/64")},
		},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "outside the range")

	e.settle()

	_, err = e.client.AddIpv6VcnCidr(
		ctx,
		core.AddIpv6VcnCidrRequest{VcnId: v.Id, IsOracleGuaAllocationEnabled: new(true)},
	)
	require.NoError(t, err, "the BYOIP prefix does not count as the Oracle-allocated one")
}

func TestByoasnOrigin(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	r := e.provisionedRange(t, "203.0.113.0/24", "")

	for _, asn := range []int64{31898, 64512, 0, 4294967295 + 1} {
		_, err := e.client.CreateByoasn(
			ctx,
			core.CreateByoasnRequest{CompartmentId: new(testComp), DisplayName: new("a"), Asn: new(asn)},
		)
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "asn %d", asn)
	}

	a, err := e.client.CreateByoasn(
		ctx,
		core.CreateByoasnRequest{CompartmentId: new(testComp), DisplayName: new("mine"), Asn: new(int64(64000))},
	)
	require.NoError(t, err)
	assert.Equal(t, core.ByoasnLifecycleStateCreating, a.LifecycleState)

	_, err = e.client.SetOriginAsn(ctx, core.SetOriginAsnRequest{
		ByoipRangeId: r.Id,
		ByoasnId:     a.Id,
	})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "the ASN is not validated yet")

	_, err = e.client.ValidateByoasn(ctx, core.ValidateByoasnRequest{ByoasnId: a.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.client.SetOriginAsn(ctx, core.SetOriginAsnRequest{
		ByoipRangeId: r.Id,
		ByoasnId:     a.Id, AsPathPrependLength: new(2),
	})
	require.NoError(t, err)

	g, err := e.client.GetByoipRange(ctx, core.GetByoipRangeRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 64000, *g.OriginAsn.Asn)
	assert.Equal(t, 2, *g.OriginAsn.AsPathPrependLength)

	_, err = e.client.DeleteByoasn(ctx, core.DeleteByoasnRequest{ByoasnId: a.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a range originates from it")

	_, err = e.client.SetOriginAsnToOracle(ctx, core.SetOriginAsnToOracleRequest{ByoipRangeId: r.Id})
	require.NoError(t, err)

	_, err = e.client.DeleteByoasn(ctx, core.DeleteByoasnRequest{ByoasnId: a.Id})
	require.NoError(t, err)
}
