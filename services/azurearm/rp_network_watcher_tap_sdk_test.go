package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNetworkSDK_NetworkWatcherFlowLogs(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	watchers := s.net.NewWatchersClient()
	logs := s.net.NewFlowLogsClient()

	_, err := watchers.CreateOrUpdate(
		ctx,
		netRG,
		"NetworkWatcher_westus",
		armnetwork.Watcher{Location: new("westus")},
		nil,
	)
	require.NoError(t, err)

	st := netProv("Microsoft.Storage", "storageAccounts", "flowacct")
	code, _ := s.rawJSON(t, http.MethodPut, st+"?api-version=2023-01-01",
		`{"location":"westus","kind":"StorageV2","sku":{"name":"Standard_LRS"}}`)
	require.Less(t, code, 300)

	np, err := s.net.NewSecurityGroupsClient().BeginCreateOrUpdate(ctx, netRG, "nsg1",
		armnetwork.SecurityGroup{Location: new("westus")}, nil)
	nsg := netPoll(t, np, err)

	flow := func(target, storage string) armnetwork.FlowLog {
		return armnetwork.FlowLog{
			Location: new("westus"),
			Properties: &armnetwork.FlowLogPropertiesFormat{
				TargetResourceID: new(target), StorageID: new(storage), Enabled: new(true),
				RetentionPolicy: &armnetwork.RetentionPolicyParameters{Days: new(int32(7)), Enabled: new(true)},
				Format: &armnetwork.FlowLogFormatParameters{
					Type: new(armnetwork.FlowLogFormatTypeJSON), Version: new(int32(2)),
				},
			},
		}
	}

	tests := []struct {
		mod      func(*armnetwork.FlowLog)
		name     string
		wantCode string
	}{
		{name: "ghost target", wantCode: "InvalidResourceReference", mod: func(f *armnetwork.FlowLog) {
			f.Properties.TargetResourceID = new(*nsg.ID + "x")
		}},
		{name: "ghost storage", wantCode: "InvalidResourceReference", mod: func(f *armnetwork.FlowLog) {
			f.Properties.StorageID = new(st + "x")
		}},
		{name: "retention", wantCode: "InvalidRequestFormat", mod: func(f *armnetwork.FlowLog) {
			f.Properties.RetentionPolicy.Days = new(int32(366))
		}},
		{name: "version", wantCode: "InvalidRequestFormat", mod: func(f *armnetwork.FlowLog) {
			f.Properties.Format.Version = new(int32(3))
		}},
		{name: "no location", wantCode: "LocationRequired", mod: func(f *armnetwork.FlowLog) { f.Location = nil }},
	}

	for _, tt := range tests {
		f := flow(*nsg.ID, st)
		tt.mod(&f)

		fp, ferr := logs.BeginCreateOrUpdate(ctx, netRG, "NetworkWatcher_westus", "fl", f, nil)
		if ferr == nil {
			_, ferr = fp.PollUntilDone(ctx, pollFast())
		}

		require.Error(t, ferr, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(ferr), tt.name)
	}

	fp, err := logs.BeginCreateOrUpdate(ctx, netRG, "NetworkWatcher_westus", "fl", flow(*nsg.ID, st), nil)
	fl := netPoll(t, fp, err)
	assert.Equal(t, *nsg.Properties.ResourceGUID, *fl.Properties.TargetResourceGUID)
	assert.Equal(t, int32(7), *fl.Properties.RetentionPolicy.Days)

	tagged, err := logs.UpdateTags(ctx, netRG, "NetworkWatcher_westus", "fl",
		armnetwork.TagsObject{Tags: map[string]*string{"k": new("v")}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *tagged.Tags["k"])

	n := 0

	for pager := logs.NewListPager(netRG, "NetworkWatcher_westus", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	// other watcher children stay in the generic store
	code, _ = s.rawJSON(
		t,
		http.MethodPut,
		netProv(
			"Microsoft.Network",
			"networkWatchers",
			"NetworkWatcher_westus",
		)+"/packetCaptures/pc?api-version=2023-01-01",
		`{"properties":{"target":"x"}}`,
	)
	assert.Less(t, code, 300)

	// deleting the monitored resource removes its flow log
	dp, err := s.net.NewSecurityGroupsClient().BeginDelete(ctx, netRG, "nsg1", nil)
	netPoll(t, dp, err)

	_, err = logs.Get(ctx, netRG, "NetworkWatcher_westus", "fl", nil)
	require.Error(t, err)

	wd, err := watchers.BeginDelete(ctx, netRG, "NetworkWatcher_westus", nil)
	netPoll(t, wd, err)
}

func TestNetworkSDK_VirtualNetworkTaps(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	taps := s.net.NewVirtualNetworkTapsClient()
	cfgs := s.net.NewInterfaceTapConfigurationsClient()

	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sid := *v.Properties.Subnets[0].ID

	dest, err := s.nic(t, "dest", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	src, err := s.nic(t, "src", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	destCfg := *dest.Properties.IPConfigurations[0].ID

	tests := []struct {
		props    armnetwork.VirtualNetworkTapPropertiesFormat
		name     string
		wantCode string
	}{
		{name: "neither", props: armnetwork.VirtualNetworkTapPropertiesFormat{}, wantCode: "InvalidRequestFormat"},
		{name: "both", wantCode: "InvalidRequestFormat", props: armnetwork.VirtualNetworkTapPropertiesFormat{
			DestinationNetworkInterfaceIPConfiguration:     &armnetwork.InterfaceIPConfiguration{ID: new(destCfg)},
			DestinationLoadBalancerFrontEndIPConfiguration: &armnetwork.FrontendIPConfiguration{ID: new(destCfg)},
		}},
		{name: "ghost", wantCode: "InvalidResourceReference", props: armnetwork.VirtualNetworkTapPropertiesFormat{
			DestinationNetworkInterfaceIPConfiguration: &armnetwork.InterfaceIPConfiguration{ID: new(destCfg + "x")},
		}},
		{name: "port", wantCode: "InvalidRequestFormat", props: armnetwork.VirtualNetworkTapPropertiesFormat{
			DestinationNetworkInterfaceIPConfiguration: &armnetwork.InterfaceIPConfiguration{ID: new(destCfg)},
			DestinationPort: new(int32(70000)),
		}},
	}

	for _, tt := range tests {
		_, terr := taps.BeginCreateOrUpdate(ctx, netRG, "tap1",
			armnetwork.VirtualNetworkTap{Location: new("westus"), Properties: &tt.props}, nil)
		require.Error(t, terr, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(terr), tt.name)
	}

	tp, err := taps.BeginCreateOrUpdate(ctx, netRG, "tap1", armnetwork.VirtualNetworkTap{
		Location: new("westus"),
		Properties: &armnetwork.VirtualNetworkTapPropertiesFormat{
			DestinationNetworkInterfaceIPConfiguration: &armnetwork.InterfaceIPConfiguration{ID: new(destCfg)},
			DestinationPort: new(int32(4789)),
		},
	}, nil)
	tap := netPoll(t, tp, err)
	assert.NotEmpty(t, *tap.Properties.ResourceGUID)
	assert.Empty(t, tap.Properties.NetworkInterfaceTapConfigurations)

	_, err = cfgs.BeginCreateOrUpdate(ctx, netRG, "src", "tc", armnetwork.InterfaceTapConfiguration{
		Properties: &armnetwork.InterfaceTapConfigurationPropertiesFormat{
			VirtualNetworkTap: &armnetwork.VirtualNetworkTap{ID: new(*tap.ID + "x")},
		},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	cp, err := cfgs.BeginCreateOrUpdate(ctx, netRG, "src", "tc", armnetwork.InterfaceTapConfiguration{
		Properties: &armnetwork.InterfaceTapConfigurationPropertiesFormat{
			VirtualNetworkTap: &armnetwork.VirtualNetworkTap{ID: tap.ID},
		},
	}, nil)
	cfg := netPoll(t, cp, err)
	assert.Equal(t, *src.ID+"/tapConfigurations/tc", *cfg.ID)

	got, err := taps.Get(ctx, netRG, "tap1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.NetworkInterfaceTapConfigurations, 1)
	assert.Equal(t, *cfg.ID, *got.Properties.NetworkInterfaceTapConfigurations[0].ID)

	nicGot, err := s.net.NewInterfacesClient().Get(ctx, netRG, "src", nil)
	require.NoError(t, err)
	assert.Len(t, nicGot.Properties.TapConfigurations, 1)

	_, err = taps.BeginDelete(ctx, netRG, "tap1", nil)
	require.Error(t, err)
	assert.Equal(t, "InUseVirtualNetworkTapCannotBeDeleted", sdkErrCode(err))

	tagged, err := taps.UpdateTags(
		ctx,
		netRG,
		"tap1",
		armnetwork.TagsObject{Tags: map[string]*string{"k": new("v")}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "v", *tagged.Tags["k"])

	dp, err := cfgs.BeginDelete(ctx, netRG, "src", "tc", nil)
	netPoll(t, dp, err)

	tdp, err := taps.BeginDelete(ctx, netRG, "tap1", nil)
	netPoll(t, tdp, err)
}
