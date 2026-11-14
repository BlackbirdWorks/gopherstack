package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNetworkDDoSProtectionPlans(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	plans := s.net.NewDdosProtectionPlansClient()

	_, err := plans.BeginCreateOrUpdate(t.Context(), netRG, "noloc", armnetwork.DdosProtectionPlan{}, nil)
	require.Error(t, err)
	assert.Equal(t, "LocationRequired", sdkErrCode(err))

	pp, err := plans.BeginCreateOrUpdate(t.Context(), netRG, "plan1", armnetwork.DdosProtectionPlan{
		Location: new("westus"), Tags: map[string]*string{"k": new("v")},
	}, nil)
	require.NoError(t, err)
	plan, err := pp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.NotEmpty(t, *plan.Properties.ResourceGUID)
	assert.Empty(t, plan.Properties.VirtualNetworks)

	vnets := s.net.NewVirtualNetworksClient()
	vnet := armnetwork.VirtualNetwork{
		Location: new("westus"),
		Properties: &armnetwork.VirtualNetworkPropertiesFormat{
			AddressSpace:         &armnetwork.AddressSpace{AddressPrefixes: []*string{new("10.0.0.0/16")}},
			EnableDdosProtection: new(true),
			DdosProtectionPlan:   &armnetwork.SubResource{ID: new(*plan.ID + "-ghost")},
		},
	}

	vp, err := vnets.BeginCreateOrUpdate(t.Context(), netRG, "vnet1", vnet, nil)
	if err == nil {
		_, err = vp.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	vnet.Properties.DdosProtectionPlan.ID = plan.ID
	vp, err = vnets.BeginCreateOrUpdate(t.Context(), netRG, "vnet1", vnet, nil)
	require.NoError(t, err)
	v, err := vp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	got, err := plans.Get(t.Context(), netRG, "plan1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.VirtualNetworks, 1)
	assert.Equal(t, *v.ID, *got.Properties.VirtualNetworks[0].ID)

	tagged, err := plans.UpdateTags(t.Context(), netRG, "plan1", armnetwork.TagsObject{
		Tags: map[string]*string{"k": new("v2")},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "v2", *tagged.Tags["k"])

	dp, err := plans.BeginDelete(t.Context(), netRG, "plan1", nil)
	if err == nil {
		_, err = dp.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "InUseDdosProtectionPlanCannotBeDeleted", sdkErrCode(err))

	vnet.Properties.DdosProtectionPlan = nil
	vnet.Properties.EnableDdosProtection = new(false)
	vp, err = vnets.BeginCreateOrUpdate(t.Context(), netRG, "vnet1", vnet, nil)
	require.NoError(t, err)
	_, err = vp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	dp, err = plans.BeginDelete(t.Context(), netRG, "plan1", nil)
	require.NoError(t, err)
	_, err = dp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
}

func TestNetworkPublicIPDDoSSettings(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)

	pp, err := s.net.NewDdosProtectionPlansClient().BeginCreateOrUpdate(t.Context(), netRG, "plan1",
		armnetwork.DdosProtectionPlan{Location: new("westus")}, nil)
	require.NoError(t, err)
	plan, err := pp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	tests := []struct {
		name     string
		mode     string
		plan     *string
		wantCode string
	}{
		{name: "enabled with plan", mode: "Enabled", plan: plan.ID},
		{name: "inherited", mode: "VirtualNetworkInherited"},
		{name: "bad mode", mode: "Always", wantCode: "InvalidRequestFormat"},
		{name: "plan without enabled", mode: "Disabled", plan: plan.ID, wantCode: "InvalidRequestFormat"},
		{name: "missing plan", mode: "Enabled", plan: new(*plan.ID + "-ghost"), wantCode: "InvalidResourceReference"},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			settings := &armnetwork.DdosSettings{ProtectionMode: new(armnetwork.DdosSettingsProtectionMode(tt.mode))}
			if tt.plan != nil {
				settings.DdosProtectionPlan = &armnetwork.SubResource{ID: tt.plan}
			}

			ip, perr := s.net.NewPublicIPAddressesClient().
				BeginCreateOrUpdate(t.Context(), netRG, "pip"+string(rune('a'+i)),
					armnetwork.PublicIPAddress{
						Location: new("westus"),
						SKU:      &armnetwork.PublicIPAddressSKU{Name: new(armnetwork.PublicIPAddressSKUNameStandard)},
						Properties: &armnetwork.PublicIPAddressPropertiesFormat{
							DdosSettings:             settings,
							PublicIPAllocationMethod: new(armnetwork.IPAllocationMethodStatic),
						},
					}, nil)
			if perr == nil {
				_, perr = ip.PollUntilDone(t.Context(), pollFast())
			}

			if tt.wantCode != "" {
				require.Error(t, perr)
				assert.Equal(t, tt.wantCode, sdkErrCode(perr))

				return
			}

			require.NoError(t, perr)
		})
	}
}
