package azurearm_test

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dnsSeededZone(t *testing.T, names []string) *netStack {
	t.Helper()

	s := newNetStack(t)

	zp, err := s.dns.NewPrivateZonesClient().BeginCreateOrUpdate(t.Context(), netRG, "contoso.internal",
		armprivatedns.PrivateZone{Location: new("global")}, nil)
	require.NoError(t, err)
	_, err = zp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	for _, n := range names {
		_, err = s.dns.NewRecordSetsClient().CreateOrUpdate(t.Context(), netRG, "contoso.internal",
			armprivatedns.RecordTypeA, n, armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: new(int64(60)), ARecords: []*armprivatedns.ARecord{{IPv4Address: new("10.0.0.4")}},
			}}, nil)
		require.NoError(t, err)
	}

	return s
}

func TestPrivateDNSRecordSetNameSuffix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
		want   []string
	}{
		{name: "no suffix", want: []string{"a.web", "b.web", "c.db", "web"}},
		{name: "web", suffix: "web", want: []string{"a.web", "b.web"}},
		{name: "none match", suffix: "cache", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := dnsSeededZone(t, []string{"a.web", "b.web", "c.db", "web"})

			var opts *armprivatedns.RecordSetsClientListByTypeOptions
			if tt.suffix != "" {
				opts = &armprivatedns.RecordSetsClientListByTypeOptions{Recordsetnamesuffix: new(tt.suffix)}
			}

			var got []string

			pager := s.dns.NewRecordSetsClient().
				NewListByTypePager(netRG, "contoso.internal", armprivatedns.RecordTypeA, opts)
			for pager.More() {
				page, err := pager.NextPage(t.Context())
				require.NoError(t, err)

				for _, rs := range page.Value {
					got = append(got, *rs.Name)
				}
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPrivateDNSRecordSetDefaultPageSize(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, 104)
	for i := range 104 {
		names = append(names, fmt.Sprintf("h%03d", i))
	}

	s := dnsSeededZone(t, names)

	pager := s.dns.NewRecordSetsClient().NewListByTypePager(netRG, "contoso.internal", armprivatedns.RecordTypeA, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 100, "a list without $top returns up to 100 record sets")
	assert.True(t, pager.More())

	page, err = pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 4)
	assert.False(t, pager.More())
}

func TestPrivateDNSZoneDeleteNeedsNoLinks(t *testing.T) {
	t.Parallel()

	s := dnsSeededZone(t, nil)
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, nil)
	links := s.dns.NewVirtualNetworkLinksClient()

	lp, err := links.BeginCreateOrUpdate(
		t.Context(),
		netRG,
		"contoso.internal",
		"link1",
		armprivatedns.VirtualNetworkLink{
			Location: new("global"),
			Properties: &armprivatedns.VirtualNetworkLinkProperties{
				VirtualNetwork: &armprivatedns.SubResource{ID: v.ID},
			},
		},
		nil,
	)
	require.NoError(t, err)
	_, err = lp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	zones := s.dns.NewPrivateZonesClient()

	_, err = zones.BeginDelete(t.Context(), netRG, "contoso.internal", nil)
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err))

	dp, err := links.BeginDelete(t.Context(), netRG, "contoso.internal", "link1", nil)
	require.NoError(t, err)
	_, err = dp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	zp, err := zones.BeginDelete(t.Context(), netRG, "contoso.internal", nil)
	require.NoError(t, err)
	_, err = zp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
}
