package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *netStack) natGW(t *testing.T, name string, idle *int32, pips ...string) (armnetwork.NatGateway, error) {
	t.Helper()

	props := &armnetwork.NatGatewayPropertiesFormat{IdleTimeoutInMinutes: idle}
	for _, id := range pips {
		props.PublicIPAddresses = append(props.PublicIPAddresses, &armnetwork.SubResource{ID: new(id)})
	}

	p, err := s.net.NewNatGatewaysClient().BeginCreateOrUpdate(t.Context(), netRG, name, armnetwork.NatGateway{
		Location: new("westus"), SKU: &armnetwork.NatGatewaySKU{Name: new(armnetwork.NatGatewaySKUNameStandard)},
		Properties: props,
	}, nil)
	if err != nil {
		return armnetwork.NatGateway{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.NatGateway, err
}

func TestNetworkSDK_NatGateway(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sid := *v.Properties.Subnets[0].ID

	std, err := s.pip(t, "std", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	basic, err := s.pip(t, "basic", armnetwork.PublicIPAddressSKUNameBasic, "")
	require.NoError(t, err)

	tests := []struct {
		idle     *int32
		name     string
		wantCode string
		pips     []string
	}{
		{name: "default idle", pips: []string{*std.ID}},
		{name: "idle too low", idle: new(int32(3)), wantCode: "InvalidRequestFormat"},
		{name: "idle too high", idle: new(int32(121)), wantCode: "InvalidRequestFormat"},
		{name: "basic pip", pips: []string{*basic.ID}, wantCode: "NatGatewayPublicIpSkuMismatch"},
		{name: "missing pip", pips: []string{*std.ID + "x"}, wantCode: "InvalidResourceReference"},
	}

	for _, tt := range tests {
		g, gerr := s.natGW(t, "gw-"+string(rune('a'+len(tt.name)%26)), tt.idle, tt.pips...)
		if tt.wantCode != "" {
			require.Error(t, gerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(gerr), tt.name)

			continue
		}

		require.NoError(t, gerr, tt.name)
		assert.EqualValues(t, 4, *g.Properties.IdleTimeoutInMinutes)
		assert.Equal(t, "Standard", string(*g.SKU.Name))

		if len(tt.pips) > 0 {
			require.NoError(t, s.deleteNat(t, *g.Name))
		}
	}

	g, err := s.natGW(t, "gw1", new(int32(30)), *std.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 30, *g.Properties.IdleTimeoutInMinutes)

	// a public IP belongs to one NAT gateway
	_, err = s.natGW(t, "gw2", nil, *std.ID)
	require.Error(t, err)
	assert.Equal(t, "PublicIPAddressInUse", sdkErrCode(err))

	pip, err := s.net.NewPublicIPAddressesClient().Get(ctx, netRG, "std", nil)
	require.NoError(t, err)
	assert.Equal(t, *g.ID, *pip.Properties.NatGateway.ID)

	_, err = s.net.NewPublicIPAddressesClient().BeginDelete(ctx, netRG, "std", nil)
	require.Error(t, err)
	assert.Equal(t, "PublicIPAddressCannotBeDeleted", sdkErrCode(err))

	// associate with the subnet from the subnet side
	sp, err := s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix: new("10.0.1.0/24"), NatGateway: &armnetwork.SubResource{ID: g.ID},
		},
	}, nil)
	sub := netPoll(t, sp, err)
	assert.Equal(t, *g.ID, *sub.Properties.NatGateway.ID)

	got, err := s.net.NewNatGatewaysClient().Get(ctx, netRG, "gw1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.Subnets, 1)
	assert.Equal(t, sid, *got.Properties.Subnets[0].ID)

	dp, err := s.net.NewNatGatewaysClient().BeginDelete(ctx, netRG, "gw1", nil)
	require.Error(t, err)
	assert.Nil(t, dp)
	assert.Equal(t, "InUseNatGatewayCannotBeDeleted", sdkErrCode(err))

	// unknown NAT gateway on a subnet
	_, err = s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix: new("10.0.1.0/24"), NatGateway: &armnetwork.SubResource{ID: new(*g.ID + "x")},
		},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	// detach, then delete
	sp, err = s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: new("10.0.1.0/24")},
	}, nil)
	netPoll(t, sp, err)
	require.NoError(t, s.deleteNat(t, "gw1"))
}

func (s *netStack) deleteNat(t *testing.T, name string) error {
	t.Helper()

	p, err := s.net.NewNatGatewaysClient().BeginDelete(t.Context(), netRG, name, nil)
	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}

func TestNetworkSDK_NatGatewayIPLimit(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)

	ids := make([]string, 0, 17)

	for i := range 17 {
		pip, err := s.pip(t, "p"+string(rune('a'+i)), armnetwork.PublicIPAddressSKUNameStandard, "")
		require.NoError(t, err)

		ids = append(ids, *pip.ID)
	}

	_, err := s.natGW(t, "gw", nil, ids...)
	require.Error(t, err)
	assert.Equal(t, "NatGatewayHasTooManyPublicIps", sdkErrCode(err))

	_, err = s.natGW(t, "gw", nil, ids[:16]...)
	require.NoError(t, err)
}

func (s *netStack) prefix(t *testing.T, name string, length int32) (armnetwork.PublicIPPrefix, error) {
	t.Helper()

	p, err := s.net.NewPublicIPPrefixesClient().BeginCreateOrUpdate(t.Context(), netRG, name, armnetwork.PublicIPPrefix{
		Location: new(
			"westus",
		),
		SKU:        &armnetwork.PublicIPPrefixSKU{Name: new(armnetwork.PublicIPPrefixSKUNameStandard)},
		Properties: &armnetwork.PublicIPPrefixPropertiesFormat{PrefixLength: new(length)},
	}, nil)
	if err != nil {
		return armnetwork.PublicIPPrefix{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.PublicIPPrefix, err
}

func TestNetworkSDK_PublicIPPrefix(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	for _, tt := range []struct {
		name   string
		code   string
		length int32
	}{
		{name: "toolarge", length: 27, code: "InvalidPublicIpPrefixLength"},
		{name: "toosmall", length: 32, code: "InvalidPublicIpPrefixLength"},
	} {
		_, err := s.prefix(t, tt.name, tt.length)
		require.Error(t, err, tt.name)
		assert.Equal(t, tt.code, sdkErrCode(err), tt.name)
	}

	pf, err := s.prefix(t, "pf", 31)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.0/31", *pf.Properties.IPPrefix)

	pf2, err := s.prefix(t, "pf2", 30)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.4/30", *pf2.Properties.IPPrefix)

	// a plain static public IP stays out of reserved prefix ranges
	plain, err := s.pip(t, "plain", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.2", *plain.Properties.IPAddress)

	a, err := s.pip(t, "a", armnetwork.PublicIPAddressSKUNameStandard, *pf.ID)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.0", *a.Properties.IPAddress)
	assert.Equal(t, *pf.ID, *a.Properties.PublicIPPrefix.ID)

	b, err := s.pip(t, "b", armnetwork.PublicIPAddressSKUNameStandard, *pf.ID)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.1", *b.Properties.IPAddress)

	_, err = s.pip(t, "c", armnetwork.PublicIPAddressSKUNameStandard, *pf.ID)
	require.Error(t, err)
	assert.Equal(t, "PublicIPPrefixExhausted", sdkErrCode(err))

	_, err = s.pip(t, "basic", armnetwork.PublicIPAddressSKUNameBasic, *pf2.ID)
	require.Error(t, err)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	got, err := s.net.NewPublicIPPrefixesClient().Get(ctx, netRG, "pf", nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.PublicIPAddresses, 2)

	// size is immutable
	_, err = s.prefix(t, "pf", 30)
	require.Error(t, err)
	assert.Equal(t, "PublicIPPrefixSizeCannotBeChanged", sdkErrCode(err))

	dp, err := s.net.NewPublicIPPrefixesClient().BeginDelete(ctx, netRG, "pf", nil)
	require.Error(t, err)
	assert.Nil(t, dp)
	assert.Equal(t, "PublicIPPrefixInUse", sdkErrCode(err))

	for _, n := range []string{"a", "b"} {
		d, derr := s.net.NewPublicIPAddressesClient().BeginDelete(ctx, netRG, n, nil)
		netPoll(t, d, derr)
	}

	// the freed address is handed out again
	again, err := s.pip(t, "c", armnetwork.PublicIPAddressSKUNameStandard, *pf.ID)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.0", *again.Properties.IPAddress)
}

func TestNetworkSDK_PrefixWithNatGatewayAndLB(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	pf, err := s.prefix(t, "pf", 28)
	require.NoError(t, err)

	big, err := s.prefix(t, "pf29", 29)
	require.NoError(t, err)

	_, err = s.net.NewNatGatewaysClient().BeginCreateOrUpdate(ctx, netRG, "gw", armnetwork.NatGateway{
		Location: new("westus"),
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			PublicIPPrefixes: []*armnetwork.SubResource{{ID: pf.ID}, {ID: big.ID}},
		},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "NatGatewayHasTooManyPublicIps", sdkErrCode(err))

	p, err := s.net.NewNatGatewaysClient().BeginCreateOrUpdate(ctx, netRG, "gw", armnetwork.NatGateway{
		Location:   new("westus"),
		Properties: &armnetwork.NatGatewayPropertiesFormat{PublicIPPrefixes: []*armnetwork.SubResource{{ID: pf.ID}}},
	}, nil)
	g := netPoll(t, p, err)

	got, err := s.net.NewPublicIPPrefixesClient().Get(ctx, netRG, "pf", nil)
	require.NoError(t, err)
	assert.Equal(t, *g.ID, *got.Properties.NatGateway.ID)

	_, err = s.net.NewPublicIPPrefixesClient().BeginDelete(ctx, netRG, "pf", nil)
	require.Error(t, err)
	assert.Equal(t, "PublicIPPrefixInUse", sdkErrCode(err))
}
