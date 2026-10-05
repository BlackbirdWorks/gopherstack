package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListOutposts_AZFilters covers AvailabilityZoneFilter/AvailabilityZoneIdFilter (serializers.go:2500-2506).
func TestListOutposts_AZFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in     func(az, azID string) *outpostssdk.ListOutpostsInput
		name   string
		wantEq bool
	}{
		{name: "az_hit", wantEq: true, in: func(az, _ string) *outpostssdk.ListOutpostsInput {
			return &outpostssdk.ListOutpostsInput{AvailabilityZoneFilter: []string{az}}
		}},
		{name: "az_miss", in: func(string, string) *outpostssdk.ListOutpostsInput {
			return &outpostssdk.ListOutpostsInput{AvailabilityZoneFilter: []string{"nowhere-1z"}}
		}},
		{name: "az_id_hit", wantEq: true, in: func(_, azID string) *outpostssdk.ListOutpostsInput {
			return &outpostssdk.ListOutpostsInput{AvailabilityZoneIdFilter: []string{azID}}
		}},
		{name: "az_id_miss", in: func(string, string) *outpostssdk.ListOutpostsInput {
			return &outpostssdk.ListOutpostsInput{AvailabilityZoneIdFilter: []string{"nowhere-az9"}}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			created := createTestOutpost(t, client, createTestSite(t, client))

			out, err := client.ListOutposts(t.Context(), tt.in(
				aws.ToString(created.AvailabilityZone), aws.ToString(created.AvailabilityZoneId),
			))
			require.NoError(t, err)
			assert.Equal(t, tt.wantEq, len(out.Outposts) == 1)
		})
	}
}

// TestListSites_AddressFilters covers the city and state/region filters (serializers.go:2666-2678).
func TestListSites_AddressFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   *outpostssdk.ListSitesInput
		name string
		want int
	}{
		{name: "city_hit", want: 1, in: &outpostssdk.ListSitesInput{OperatingAddressCityFilter: []string{"Seattle"}}},
		{name: "city_miss", in: &outpostssdk.ListSitesInput{OperatingAddressCityFilter: []string{"Paris"}}},
		{name: "state_hit", want: 1, in: &outpostssdk.ListSitesInput{
			OperatingAddressStateOrRegionFilter: []string{"WA"},
		}},
		{name: "state_miss", in: &outpostssdk.ListSitesInput{OperatingAddressStateOrRegionFilter: []string{"OR"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			_, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
				Name: aws.String("filter-site"),
				OperatingAddress: &types.Address{
					AddressLine1: aws.String("L1"), City: aws.String("Seattle"),
					ContactName: aws.String("A"), ContactPhoneNumber: aws.String("+12065550100"),
					CountryCode: aws.String("US"), PostalCode: aws.String("11111"), StateOrRegion: aws.String("WA"),
				},
			})
			require.NoError(t, err)

			out, err := client.ListSites(t.Context(), tt.in)
			require.NoError(t, err)
			assert.Len(t, out.Sites, tt.want)
		})
	}
}
