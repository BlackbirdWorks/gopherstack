package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *cmpStack) linkedZone(t *testing.T, zone, link string, registration bool) error {
	t.Helper()

	zp, err := s.dns.NewPrivateZonesClient().BeginCreateOrUpdate(t.Context(), netRG, zone,
		armprivatedns.PrivateZone{Location: new("global")}, nil)
	if err == nil {
		_, err = zp.PollUntilDone(t.Context(), pollFast())
	}

	require.NoError(t, err)

	vnetID := s.subnetID[:len(s.subnetID)-len("/subnets/s1")]

	lp, err := s.dns.NewVirtualNetworkLinksClient().BeginCreateOrUpdate(t.Context(), netRG, zone, link,
		armprivatedns.VirtualNetworkLink{
			Location: new("global"),
			Properties: &armprivatedns.VirtualNetworkLinkProperties{
				VirtualNetwork: &armprivatedns.SubResource{ID: new(vnetID)}, RegistrationEnabled: new(registration),
			},
		}, nil)
	if err != nil {
		return err
	}

	_, err = lp.PollUntilDone(t.Context(), pollFast())

	return err
}

func (s *cmpStack) autoRecords(t *testing.T) map[string]*armprivatedns.RecordSet {
	t.Helper()

	out := map[string]*armprivatedns.RecordSet{}
	pager := s.dns.NewRecordSetsClient().NewListByTypePager(netRG, "contoso.internal", armprivatedns.RecordTypeA, nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, r := range page.Value {
			out[*r.Name] = r
		}
	}

	return out
}

func TestPrivateDNSAutoRegistration(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	require.NoError(t, s.linkedZone(t, "contoso.internal", "reg", true))

	nic := s.newNIC(t, "nic1")
	s.mustVM(t, "vm1", linuxVM(nic))

	recs := s.autoRecords(t)
	require.Contains(t, recs, "vm1")
	assert.True(t, *recs["vm1"].Properties.IsAutoRegistered)
	assert.EqualValues(t, 10, *recs["vm1"].Properties.TTL)

	n, err := s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic1", nil)
	require.NoError(t, err)
	assert.Equal(t, *n.Properties.IPConfigurations[0].Properties.PrivateIPAddress,
		*recs["vm1"].Properties.ARecords[0].IPv4Address)

	zone, err := s.dns.NewPrivateZonesClient().Get(t.Context(), netRG, "contoso.internal", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 2, *zone.Properties.NumberOfRecordSets, "the SOA record and the auto-registered record")

	vms := s.cf.NewVirtualMachinesClient()

	off, err := vms.BeginPowerOff(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, off, err))
	assert.NotContains(t, s.autoRecords(t), "vm1", "a stopped machine loses its record")

	on, err := vms.BeginStart(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, on, err))
	assert.Contains(t, s.autoRecords(t), "vm1")

	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))
	assert.Contains(t, dst.autoRecords(t), "vm1", "auto-registered records survive a snapshot")

	del, err := vms.BeginDelete(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, del, err))
	assert.NotContains(t, s.autoRecords(t), "vm1")
}

func TestPrivateDNSAutoRegistrationRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		registered bool
		wantRecord bool
	}{
		{name: "registration disabled", registered: false, wantRecord: false},
		{name: "registration enabled", registered: true, wantRecord: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			require.NoError(t, s.linkedZone(t, "contoso.internal", "l", tt.registered))
			s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

			assert.Equal(t, tt.wantRecord, s.autoRecords(t)["vm1"] != nil)
		})
	}

	s := newCmpStack(t)
	require.NoError(t, s.linkedZone(t, "one.internal", "reg", true))

	err := s.linkedZone(t, "two.internal", "reg", true)
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err), "one registration link per virtual network")

	require.NoError(t, s.linkedZone(t, "three.internal", "plain", false), "non-registering links are unlimited")
}

func TestPrivateDNSOneLinkPerZoneAndNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		zone     string
		link     string
		wantCode string
	}{
		{name: "same link updated", zone: "a.internal", link: "first"},
		{name: "second link to same network", zone: "a.internal", link: "second", wantCode: "Conflict"},
		{name: "other zone same network", zone: "b.internal", link: "first"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			require.NoError(t, s.linkedZone(t, "a.internal", "first", false))

			err := s.linkedZone(t, tt.zone, tt.link, false)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}
