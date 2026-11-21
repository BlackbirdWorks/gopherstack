package gcpcompute_test

import (
	"fmt"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// legacyRegions is the 15-region catalogue before the catalogue widened, in its original order: the order
// selects the external pool, so snapshots taken then must keep their addresses.
type legacyRegion struct {
	region string
	auto   string
}

func legacyRegions() []legacyRegion {
	return []legacyRegion{
		{"asia-east1", "10.140.0.0/20"},
		{"asia-northeast1", "10.146.0.0/20"},
		{"asia-south1", "10.160.0.0/20"},
		{"asia-southeast1", "10.148.0.0/20"},
		{"australia-southeast1", "10.152.0.0/20"},
		{"europe-west1", "10.132.0.0/20"},
		{"europe-west2", "10.154.0.0/20"},
		{"europe-west3", "10.156.0.0/20"},
		{"europe-west4", "10.164.0.0/20"},
		{"southamerica-east1", "10.158.0.0/20"},
		{"us-central1", "10.128.0.0/20"},
		{"us-east1", "10.142.0.0/20"},
		{"us-east4", "10.150.0.0/20"},
		{"us-west1", "10.138.0.0/20"},
		{"us-west2", "10.168.0.0/20"},
	}
}

func TestLegacyRegionPoolsAndAutoRanges(t *testing.T) {
	t.Parallel()

	for i, tt := range legacyRegions() {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
				Project: project, Region: tt.region, AddressResource: &computepb.Address{Name: s("a")},
			})
			e.done(t, op, err)

			a, err := e.addresses(t).Get(t.Context(), &computepb.GetAddressRequest{
				Project: project, Region: tt.region, Address: "a",
			})
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("34.%d.0.1", 100+i), a.GetAddress())

			e.network(t, "auto", true)

			sub, err := e.subnets(t).Get(t.Context(), &computepb.GetSubnetworkRequest{
				Project: project, Region: tt.region, Subnetwork: "auto",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.auto, sub.GetIpCidrRange())
		})
	}
}

func TestExternalPoolsAreDistinct(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seen := map[string]string{}

	it := e.regions(t).List(t.Context(), &computepb.ListRegionsRequest{Project: project})
	for {
		r, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			break
		}

		op, aerr := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
			Project: project, Region: r.GetName(), AddressResource: &computepb.Address{Name: s("a")},
		})
		e.done(t, op, aerr)

		a, gerr := e.addresses(t).Get(t.Context(), &computepb.GetAddressRequest{
			Project: project, Region: r.GetName(), Address: "a",
		})
		require.NoError(t, gerr)
		assert.NotContains(t, seen, a.GetAddress())
		assert.NotRegexp(t, `^34\.120\.`, a.GetAddress(), "the global pool")
		seen[a.GetAddress()] = r.GetName()
	}

	assert.Len(t, seen, 43)
}
