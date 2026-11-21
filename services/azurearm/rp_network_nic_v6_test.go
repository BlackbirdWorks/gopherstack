package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nicV6Config(name, subnetID string, version armnetwork.IPVersion, method armnetwork.IPAllocationMethod,
	addr string,
) *armnetwork.InterfaceIPConfiguration {
	props := &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		Subnet:                    &armnetwork.Subnet{ID: new(subnetID)},
		PrivateIPAddressVersion:   new(version),
		PrivateIPAllocationMethod: new(method),
	}
	if addr != "" {
		props.PrivateIPAddress = new(addr)
	}

	return &armnetwork.InterfaceIPConfiguration{Name: new(name), Properties: props}
}

func TestNetworkSDK_NICIPv6(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	v := s.vnet(t, "vnet6", []string{"10.0.0.0/16", "fd00:db8::/48"},
		map[string]string{"a": "10.0.1.0/24", "b": "fd00:db8:0:1::/64"})
	ids := map[string]string{}

	for _, sn := range v.Properties.Subnets {
		ids[*sn.Name] = *sn.ID
	}

	dyn, stat := armnetwork.IPAllocationMethodDynamic, armnetwork.IPAllocationMethodStatic
	v4, v6 := armnetwork.IPVersionIPv4, armnetwork.IPVersionIPv6

	tests := []struct {
		name     string
		wantCode string
		wantIPs  []string
		cfgs     []*armnetwork.InterfaceIPConfiguration
	}{
		{
			name: "dual dynamic",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c4", ids["a"], v4, dyn, ""), nicV6Config("c6", ids["b"], v6, dyn, ""),
			},
			wantIPs: []string{"10.0.1.4", "fd00:db8:0:1::4"},
		},
		{
			name: "v6 listed first",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c6", ids["b"], v6, dyn, ""), nicV6Config("c4", ids["a"], v4, dyn, ""),
			},
			wantIPs: []string{"fd00:db8:0:1::5", "10.0.1.5"},
		},
		{
			name: "static v6",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c4", ids["a"], v4, dyn, ""), nicV6Config("c6", ids["b"], v6, stat, "fd00:db8:0:1::100"),
			},
			wantIPs: []string{"10.0.1.6", "fd00:db8:0:1::100"},
		},
		{
			name: "v6 taken",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c4", ids["a"], v4, dyn, ""), nicV6Config("c6", ids["b"], v6, stat, "fd00:db8:0:1::100"),
			},
			wantCode: "PrivateIPAddressIsAllocated",
		},
		{
			name: "v6 outside",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c4", ids["a"], v4, dyn, ""), nicV6Config("c6", ids["b"], v6, stat, "fd00:db8:0:2::9"),
			},
			wantCode: "PrivateIPAddressNotInSubnet",
		},
		{
			name: "v6 static v4 addr",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c4", ids["a"], v4, dyn, ""), nicV6Config("c6", ids["b"], v6, stat, "10.0.1.50"),
			},
			wantCode: "InvalidRequestFormat",
		},
		{
			name:     "v6 only",
			cfgs:     []*armnetwork.InterfaceIPConfiguration{nicV6Config("c6", ids["b"], v6, dyn, "")},
			wantCode: "InvalidRequestFormat",
		},
		{
			name: "v6 in v4 subnet",
			cfgs: []*armnetwork.InterfaceIPConfiguration{
				nicV6Config("c4", ids["a"], v4, dyn, ""), nicV6Config("c6", ids["a"], v6, dyn, ""),
			},
			wantCode: "SubnetIsFull",
		},
	}

	// sequential: each case builds on the allocations of the earlier ones
	for i, tt := range tests {
		poller, err := s.net.NewInterfacesClient().BeginCreateOrUpdate(t.Context(), netRG, "nic6-"+string(rune('a'+i)),
			armnetwork.Interface{
				Location:   new("westus"),
				Properties: &armnetwork.InterfacePropertiesFormat{IPConfigurations: tt.cfgs},
			}, nil)
		if err == nil {
			var res armnetwork.InterfacesClientCreateOrUpdateResponse

			res, err = poller.PollUntilDone(t.Context(), pollFast())
			if err == nil {
				require.Empty(t, tt.wantCode, tt.name)

				var got []string

				for _, c := range res.Properties.IPConfigurations {
					got = append(got, *c.Properties.PrivateIPAddress)
				}

				assert.Equal(t, tt.wantIPs, got, tt.name)

				for _, c := range res.Properties.IPConfigurations {
					primary := c.Properties.Primary != nil && *c.Properties.Primary
					assert.Equal(t, *c.Name == "c4", primary, tt.name)
				}

				continue
			}
		}

		require.Error(t, err, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)
	}
}

func TestNetworkSDK_SubnetIPv6Size(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	poller, err := s.net.NewVirtualNetworksClient().BeginCreateOrUpdate(t.Context(), netRG, "vnet6size",
		armnetwork.VirtualNetwork{
			Location: new("westus"),
			Properties: &armnetwork.VirtualNetworkPropertiesFormat{
				AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{new("fd00:db8::/48")}},
				Subnets: []*armnetwork.Subnet{{
					Name:       new("small"),
					Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: new("fd00:db8:0:1::/80")},
				}},
			},
		}, nil)
	if err == nil {
		_, err = poller.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "NetcfgInvalidSubnet", sdkErrCode(err))
}
