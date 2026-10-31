package azurearm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *netStack) rawJSON(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()

	resp := s.rawDo(t, method, path, body)

	var out map[string]any

	_ = json.NewDecoder(resp.Body).Decode(&out)

	return resp.StatusCode, out
}

func netProv(ns, typ, name string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/" + ns + "/" + typ + "/" + name
}

type peEnv struct {
	*netStack
	subnetID  string
	zoneID    string
	storageID string
}

func newPEEnv(t *testing.T) *peEnv {
	t.Helper()

	s := newNetStack(t)
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"pe": "10.0.1.0/24", "other": "10.0.2.0/24"})

	zp, err := s.dns.NewPrivateZonesClient().
		BeginCreateOrUpdate(t.Context(), netRG, "privatelink.blob.core.windows.net",
			armprivatedns.PrivateZone{Location: new("global")}, nil)
	zone := netPoll(t, zp, err)

	st := netProv("Microsoft.Storage", "storageAccounts", "acct1")
	code, _ := s.rawJSON(t, http.MethodPut, st+"?api-version=2023-01-01",
		`{"location":"westus","kind":"StorageV2","sku":{"name":"Standard_LRS"}}`)
	require.Less(t, code, 300)

	return &peEnv{netStack: s, subnetID: *v.Properties.Subnets[0].ID, zoneID: *zone.ID, storageID: st}
}

func (e *peEnv) endpoint(
	t *testing.T,
	name string,
	manual bool,
	target, group string,
) (armnetwork.PrivateEndpoint, error) {
	t.Helper()

	conn := &armnetwork.PrivateLinkServiceConnection{
		Name: new("conn"),
		Properties: &armnetwork.PrivateLinkServiceConnectionProperties{
			PrivateLinkServiceID: new(target), GroupIDs: []*string{new(group)}, RequestMessage: new("please approve"),
		},
	}

	props := &armnetwork.PrivateEndpointProperties{Subnet: &armnetwork.Subnet{ID: new(e.subnetID)}}
	if manual {
		props.ManualPrivateLinkServiceConnections = []*armnetwork.PrivateLinkServiceConnection{conn}
	} else {
		props.PrivateLinkServiceConnections = []*armnetwork.PrivateLinkServiceConnection{conn}
	}

	p, err := e.net.NewPrivateEndpointsClient().BeginCreateOrUpdate(t.Context(), netRG, name,
		armnetwork.PrivateEndpoint{Location: new("westus"), Properties: props}, nil)
	if err != nil {
		return armnetwork.PrivateEndpoint{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.PrivateEndpoint, err
}

func TestNetworkSDK_PrivateEndpointStorage(t *testing.T) {
	t.Parallel()

	e := newPEEnv(t)
	ctx := t.Context()

	pe, err := e.endpoint(t, "pe1", false, e.storageID, "blob")
	require.NoError(t, err)

	require.Len(t, pe.Properties.NetworkInterfaces, 1)
	require.Len(t, pe.Properties.CustomDNSConfigs, 1)
	assert.Equal(t, "acct1.blob.core.windows.net", *pe.Properties.CustomDNSConfigs[0].Fqdn)
	assert.Equal(t, "10.0.1.4", *pe.Properties.CustomDNSConfigs[0].IPAddresses[0])

	state := pe.Properties.PrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState
	assert.Equal(t, "Approved", *state.Status)
	assert.Equal(t, "Auto-Approved", *state.Description)

	nicName := (*pe.Properties.NetworkInterfaces[0].ID)[len(netProv("Microsoft.Network", "networkInterfaces", "")):]
	nic, err := e.net.NewInterfacesClient().Get(ctx, netRG, nicName, nil)
	require.NoError(t, err)
	assert.Equal(t, *pe.ID, *nic.Properties.PrivateEndpoint.ID)
	assert.Equal(t, "10.0.1.4", *nic.Properties.IPConfigurations[0].Properties.PrivateIPAddress)
	assert.Equal(t, "blob", *nic.Properties.IPConfigurations[0].Properties.PrivateLinkConnectionProperties.GroupID)

	// the managed interface cannot be deleted by hand
	_, err = e.net.NewInterfacesClient().BeginDelete(ctx, netRG, nicName, nil)
	require.Error(t, err)
	assert.Equal(t, "NicInUse", sdkErrCode(err))

	// the subnet reports the endpoint and cannot be deleted
	sn, err := e.net.NewSubnetsClient().Get(ctx, netRG, "vnet1", "pe", nil)
	require.NoError(t, err)
	require.Len(t, sn.Properties.PrivateEndpoints, 1)
	assert.Equal(t, *pe.ID, *sn.Properties.PrivateEndpoints[0].ID)

	_, err = e.net.NewSubnetsClient().BeginDelete(ctx, netRG, "vnet1", "pe", nil)
	require.Error(t, err)
	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(err))

	// the target lists the connection
	code, acct := e.rawJSON(t, http.MethodGet, e.storageID+"?api-version=2023-01-01", "")
	require.Equal(t, http.StatusOK, code)

	conns, _ := acct["properties"].(map[string]any)["privateEndpointConnections"].([]any)
	require.Len(t, conns, 1)

	conn, _ := conns[0].(map[string]any)
	cprops, _ := conn["properties"].(map[string]any)
	assert.Equal(t, *pe.ID, cprops["privateEndpoint"].(map[string]any)["id"])
	assert.Equal(t, "Approved", cprops["privateLinkServiceConnectionState"].(map[string]any)["status"])

	code, list := e.rawJSON(t, http.MethodGet, e.storageID+"/privateEndpointConnections?api-version=2023-01-01", "")
	require.Equal(t, http.StatusOK, code)
	assert.Len(t, list["value"], 1)

	code, res := e.rawJSON(t, http.MethodGet, e.storageID+"/privateLinkResources?api-version=2023-01-01", "")
	require.Equal(t, http.StatusOK, code)
	assert.Len(t, res["value"], 12)

	// DNS zone group writes the A record and removes it again
	zg := e.net.NewPrivateDNSZoneGroupsClient()
	zp, err := zg.BeginCreateOrUpdate(ctx, netRG, "pe1", "default", armnetwork.PrivateDNSZoneGroup{
		Properties: &armnetwork.PrivateDNSZoneGroupPropertiesFormat{
			PrivateDNSZoneConfigs: []*armnetwork.PrivateDNSZoneConfig{{
				Name:       new("blob"),
				Properties: &armnetwork.PrivateDNSZonePropertiesFormat{PrivateDNSZoneID: new(e.zoneID)},
			}},
		},
	}, nil)
	group := netPoll(t, zp, err)

	rs := group.Properties.PrivateDNSZoneConfigs[0].Properties.RecordSets
	require.Len(t, rs, 1)
	assert.Equal(t, "acct1", *rs[0].RecordSetName)
	assert.Equal(t, "acct1.privatelink.blob.core.windows.net.", *rs[0].Fqdn)
	assert.EqualValues(t, 10, *rs[0].TTL)
	assert.Equal(t, "10.0.1.4", *rs[0].IPAddresses[0])

	rec, err := e.dns.NewRecordSetsClient().Get(ctx, netRG, "privatelink.blob.core.windows.net",
		armprivatedns.RecordTypeA, "acct1", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.4", *rec.Properties.ARecords[0].IPv4Address)

	_, err = e.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe1", nil)
	require.NoError(t, err)

	gd, err := zg.BeginDelete(ctx, netRG, "pe1", "default", nil)
	netPoll(t, gd, err)

	_, err = e.dns.NewRecordSetsClient().Get(ctx, netRG, "privatelink.blob.core.windows.net",
		armprivatedns.RecordTypeA, "acct1", nil)
	require.Error(t, err)

	// re-create the group, then delete the endpoint: record, NIC and connection all go
	zp, err = zg.BeginCreateOrUpdate(ctx, netRG, "pe1", "default", armnetwork.PrivateDNSZoneGroup{
		Properties: &armnetwork.PrivateDNSZoneGroupPropertiesFormat{
			PrivateDNSZoneConfigs: []*armnetwork.PrivateDNSZoneConfig{{
				Name:       new("blob"),
				Properties: &armnetwork.PrivateDNSZonePropertiesFormat{PrivateDNSZoneID: new(e.zoneID)},
			}},
		},
	}, nil)
	netPoll(t, zp, err)

	dp, err := e.net.NewPrivateEndpointsClient().BeginDelete(ctx, netRG, "pe1", nil)
	netPoll(t, dp, err)

	_, err = e.dns.NewRecordSetsClient().Get(ctx, netRG, "privatelink.blob.core.windows.net",
		armprivatedns.RecordTypeA, "acct1", nil)
	require.Error(t, err)

	_, err = e.net.NewInterfacesClient().Get(ctx, netRG, nicName, nil)
	require.Error(t, err)

	_, acct = e.rawJSON(t, http.MethodGet, e.storageID+"?api-version=2023-01-01", "")
	_, has := acct["properties"].(map[string]any)["privateEndpointConnections"]
	assert.False(t, has)

	dsp, err := e.net.NewSubnetsClient().BeginDelete(ctx, netRG, "vnet1", "pe", nil)
	netPoll(t, dsp, err)
}

func TestNetworkSDK_PrivateEndpointApprovalFlow(t *testing.T) {
	t.Parallel()

	e := newPEEnv(t)
	ctx := t.Context()

	pe, err := e.endpoint(t, "pe1", true, e.storageID, "file")
	require.NoError(t, err)

	state := pe.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState
	assert.Equal(t, "Pending", *state.Status)
	assert.Equal(t, "please approve", *state.Description)

	_, list := e.rawJSON(t, http.MethodGet, e.storageID+"/privateEndpointConnections?api-version=2023-01-01", "")
	items, _ := list["value"].([]any)
	require.Len(t, items, 1)

	item, _ := items[0].(map[string]any)
	name, _ := item["name"].(string)
	assert.Contains(t, name, "pe1.")

	connPath := e.storageID + "/privateEndpointConnections/" + name + "?api-version=2023-01-01"

	tests := []struct {
		status string
		want   string
		code   int
	}{
		{status: "Approved", want: "Approved", code: http.StatusOK},
		{status: "Rejected", want: "Rejected", code: http.StatusOK},
		{status: "Bogus", code: http.StatusBadRequest},
	}

	for _, tt := range tests {
		code, _ := e.rawJSON(t, http.MethodPut, connPath,
			`{"properties":{"privateLinkServiceConnectionState":{"status":"`+tt.status+`","description":"by owner"}}}`)
		require.Equal(t, tt.code, code, tt.status)

		if tt.want == "" {
			continue
		}

		got, gerr := e.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe1", nil)
		require.NoError(t, gerr)

		st := got.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState
		assert.Equal(t, tt.want, *st.Status)
		assert.Equal(t, "by owner", *st.Description)
	}

	// owner deletes the connection: the endpoint turns Disconnected
	code, _ := e.rawJSON(t, http.MethodDelete, connPath, "")
	require.Equal(t, http.StatusOK, code)

	got, err := e.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe1", nil)
	require.NoError(t, err)
	assert.Equal(t, "Disconnected",
		*got.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status)

	_, list = e.rawJSON(t, http.MethodGet, e.storageID+"/privateEndpointConnections?api-version=2023-01-01", "")
	assert.Empty(t, list["value"])

	// re-PUT of the endpoint keeps the disconnected state and the same address
	again, err := e.endpoint(t, "pe1", true, e.storageID, "file")
	require.NoError(t, err)
	assert.Equal(
		t,
		*pe.Properties.CustomDNSConfigs[0].IPAddresses[0],
		*again.Properties.CustomDNSConfigs[0].IPAddresses[0],
	)
	assert.Equal(t, *pe.Properties.NetworkInterfaces[0].ID, *again.Properties.NetworkInterfaces[0].ID)
}

func TestNetworkSDK_PrivateEndpointValidation(t *testing.T) {
	t.Parallel()

	e := newPEEnv(t)
	ctx := t.Context()

	dp, err := e.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "deleg", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix: new("10.0.3.0/24"),
			Delegations: []*armnetwork.Delegation{
				{
					Name: new("d"),
					Properties: &armnetwork.ServiceDelegationPropertiesFormat{
						ServiceName: new("Microsoft.Web/serverFarms"),
					},
				},
			},
		},
	}, nil)
	deleg := netPoll(t, dp, err)

	missing := netProv("Microsoft.Storage", "storageAccounts", "ghost")

	tests := []struct {
		name     string
		target   string
		group    string
		subnet   string
		wantCode string
	}{
		{name: "valid", target: e.storageID, group: "blob"},
		{name: "case insensitive group", target: e.storageID, group: "BLOB_SECONDARY"},
		{name: "bad group", target: e.storageID, group: "sql", wantCode: "PrivateEndpointGroupIdsInvalid"},
		{name: "missing target", target: missing, group: "blob", wantCode: "InvalidResourceReference"},
		{name: "bad id", target: "storage", group: "blob", wantCode: "LinkedInvalidPropertyId"},
		{
			name: "unsupported type", target: netProv("Microsoft.Foo", "things", "x"), group: "x",
			wantCode: "InvalidRequestFormat",
		},
		{name: "delegated subnet", target: e.storageID, group: "blob", subnet: *deleg.ID,
			wantCode: "PrivateEndpointCreationNotAllowedAsSubnetIsDelegated"},
		{
			name:     "missing subnet",
			target:   e.storageID,
			group:    "blob",
			subnet:   e.subnetID + "x",
			wantCode: "InvalidResourceReference",
		},
	}

	for i, tt := range tests {
		env := *e
		if tt.subnet != "" {
			env.subnetID = tt.subnet
		}

		_, perr := env.endpoint(t, "pe-"+string(rune('a'+i)), false, tt.target, tt.group)
		if tt.wantCode != "" {
			require.Error(t, perr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(perr), tt.name)

			continue
		}

		require.NoError(t, perr, tt.name)
	}

	// no connections at all
	_, err = e.net.NewPrivateEndpointsClient().BeginCreateOrUpdate(ctx, netRG, "empty", armnetwork.PrivateEndpoint{
		Location:   new("westus"),
		Properties: &armnetwork.PrivateEndpointProperties{Subnet: &armnetwork.Subnet{ID: new(e.subnetID)}},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	// wrong region for the virtual network
	_, err = e.net.NewPrivateEndpointsClient().BeginCreateOrUpdate(ctx, netRG, "wrong", armnetwork.PrivateEndpoint{
		Location: new("eastus"),
		Properties: &armnetwork.PrivateEndpointProperties{
			Subnet: &armnetwork.Subnet{ID: new(e.subnetID)},
			PrivateLinkServiceConnections: []*armnetwork.PrivateLinkServiceConnection{{
				Name: new("c"),
				Properties: &armnetwork.PrivateLinkServiceConnectionProperties{
					PrivateLinkServiceID: new(e.storageID), GroupIDs: []*string{new("blob")},
				},
			}},
		},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "PrivateEndpointAndVnetLocationMismatch", sdkErrCode(err))

	// zone groups need an existing zone
	zg := e.net.NewPrivateDNSZoneGroupsClient()
	_, err = zg.BeginCreateOrUpdate(ctx, netRG, "pe-a", "default", armnetwork.PrivateDNSZoneGroup{
		Properties: &armnetwork.PrivateDNSZoneGroupPropertiesFormat{
			PrivateDNSZoneConfigs: []*armnetwork.PrivateDNSZoneConfig{{
				Name:       new("z"),
				Properties: &armnetwork.PrivateDNSZonePropertiesFormat{PrivateDNSZoneID: new(e.zoneID + "x")},
			}},
		},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))
}

func TestNetworkSDK_PrivateEndpointAddressAllocation(t *testing.T) {
	t.Parallel()

	e := newPEEnv(t)

	// two endpoints get distinct addresses, and a static NIC cannot reuse one
	pe1, err := e.endpoint(t, "pe1", false, e.storageID, "blob")
	require.NoError(t, err)

	pe2, err := e.endpoint(t, "pe2", false, e.storageID, "file")
	require.NoError(t, err)

	a := *pe1.Properties.CustomDNSConfigs[0].IPAddresses[0]
	b := *pe2.Properties.CustomDNSConfigs[0].IPAddresses[0]
	assert.NotEqual(t, a, b)

	_, err = e.nic(t, "nic-static", e.subnetID, armnetwork.IPAllocationMethodStatic, a, nil)
	require.Error(t, err)
	assert.Equal(t, "PrivateIPAddressIsAllocated", sdkErrCode(err))
}

func TestNetworkSDK_PrivateEndpointSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newPEEnv(t)
	ctx := t.Context()

	_, err := e.endpoint(t, "pe1", false, e.storageID, "blob")
	require.NoError(t, err)

	zp, err := e.net.NewPrivateDNSZoneGroupsClient().
		BeginCreateOrUpdate(ctx, netRG, "pe1", "default", armnetwork.PrivateDNSZoneGroup{
			Properties: &armnetwork.PrivateDNSZoneGroupPropertiesFormat{
				PrivateDNSZoneConfigs: []*armnetwork.PrivateDNSZoneConfig{{
					Name:       new("blob"),
					Properties: &armnetwork.PrivateDNSZonePropertiesFormat{PrivateDNSZoneID: new(e.zoneID)},
				}},
			},
		}, nil)
	netPoll(t, zp, err)

	pip, err := e.pip(t, "pip", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	lb, err := e.putLB(t, "lb1", stdLB("lb1", *pip.ID, armnetwork.LoadBalancerSKUNameStandard))
	require.NoError(t, err)

	nat, err := e.natGW(t, "gw", nil)
	require.NoError(t, err)

	pf, err := e.prefix(t, "pf", 30)
	require.NoError(t, err)

	asg, err := e.asg(t, "asg", "westus")
	require.NoError(t, err)

	nic, err := e.nicWith(t, "nic1", e.subnetID, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		LoadBalancerBackendAddressPools: []*armnetwork.BackendAddressPool{
			{ID: lbSub("lb1", "backendAddressPools", "pool").ID},
		},
		ApplicationSecurityGroups: []*armnetwork.ApplicationSecurityGroup{{ID: asg.ID}},
	})
	require.NoError(t, err)

	snap := e.handler.Snapshot(ctx)
	require.NotEmpty(t, snap)

	fresh := newNetStack(t)
	require.NoError(t, fresh.handler.Restore(ctx, snap))

	gotPE, err := fresh.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe1", nil)
	require.NoError(t, err)
	assert.Equal(t, "Approved",
		*gotPE.Properties.PrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status)
	assert.Len(t, gotPE.Properties.CustomDNSConfigs, 1)

	zg, err := fresh.net.NewPrivateDNSZoneGroupsClient().Get(ctx, netRG, "pe1", "default", nil)
	require.NoError(t, err)
	assert.Len(t, zg.Properties.PrivateDNSZoneConfigs[0].Properties.RecordSets, 1)

	rec, err := fresh.dns.NewRecordSetsClient().Get(ctx, netRG, "privatelink.blob.core.windows.net",
		armprivatedns.RecordTypeA, "acct1", nil)
	require.NoError(t, err)
	assert.Len(t, rec.Properties.ARecords, 1)

	gotLB, err := fresh.net.NewLoadBalancersClient().Get(ctx, netRG, "lb1", nil)
	require.NoError(t, err)
	assert.Equal(t, *lb.Properties.ResourceGUID, *gotLB.Properties.ResourceGUID)

	pool, err := fresh.net.NewLoadBalancerBackendAddressPoolsClient().Get(ctx, netRG, "lb1", "pool", nil)
	require.NoError(t, err)
	require.Len(t, pool.Properties.BackendIPConfigurations, 1)
	assert.Equal(t, *nic.Properties.IPConfigurations[0].ID, *pool.Properties.BackendIPConfigurations[0].ID)

	gotNAT, err := fresh.net.NewNatGatewaysClient().Get(ctx, netRG, "gw", nil)
	require.NoError(t, err)
	assert.Equal(t, *nat.ID, *gotNAT.ID)

	gotPF, err := fresh.net.NewPublicIPPrefixesClient().Get(ctx, netRG, "pf", nil)
	require.NoError(t, err)
	assert.Equal(t, *pf.Properties.IPPrefix, *gotPF.Properties.IPPrefix)

	_, err = fresh.net.NewApplicationSecurityGroupsClient().Get(ctx, netRG, "asg", nil)
	require.NoError(t, err)
}
