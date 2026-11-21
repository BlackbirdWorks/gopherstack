package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func TestZonesAndRegions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	z, err := e.zones(t).Get(t.Context(), &computepb.GetZoneRequest{Project: project, Zone: zone})
	require.NoError(t, err)
	assert.Equal(t, "UP", z.GetStatus())
	assert.Equal(t, "https://www.googleapis.com/compute/v1/projects/"+project+"/regions/"+region, z.GetRegion())
	assert.NotEmpty(t, z.GetAvailableCpuPlatforms())

	_, err = e.zones(t).Get(t.Context(), &computepb.GetZoneRequest{Project: project, Zone: "mars-central1-a"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	r, err := e.regions(t).Get(t.Context(), &computepb.GetRegionRequest{Project: project, Region: region})
	require.NoError(t, err)
	assert.Len(t, r.GetZones(), 4)
	assert.Contains(t, r.GetZones(), "https://www.googleapis.com/compute/v1/projects/"+project+"/zones/"+zone)

	var names []string

	it := e.zones(t).List(t.Context(), &computepb.ListZonesRequest{Project: project, Filter: s(`name = "us-east1-.*"`)})

	for {
		got, nerr := it.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		names = append(names, got.GetName())
	}

	assert.Equal(t, []string{"us-east1-b", "us-east1-c", "us-east1-d"}, names)

	rit := e.regions(t).List(t.Context(), &computepb.ListRegionsRequest{Project: project})
	count := 0

	for {
		if _, nerr := rit.Next(); nerr != nil {
			break
		}

		count++
	}

	assert.Equal(t, 43, count)
}

func TestMachineTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mt     string
		cpus   int32
		memMb  int32
		shared bool
		status int
	}{
		{"e2-micro", "e2-micro", 2, 1024, true, 200},
		{"e2-medium", "e2-medium", 2, 4096, true, 200},
		{"e2-standard-4", "e2-standard-4", 4, 16384, false, 200},
		{"e2-highmem-8", "e2-highmem-8", 8, 65536, false, 200},
		{"e2-highcpu-16", "e2-highcpu-16", 16, 16384, false, 200},
		{"n2-standard-8", "n2-standard-8", 8, 32768, false, 200},
		{"n2-highmem-2", "n2-highmem-2", 2, 16384, false, 200},
		{"n1-standard-1", "n1-standard-1", 1, 3840, false, 200},
		{"n1-highmem-2", "n1-highmem-2", 2, 13312, false, 200},
		{"n1-highcpu-2", "n1-highcpu-2", 2, 1843, false, 200},
		{"n1-standard-96", "n1-standard-96", 96, 368640, false, 200},
		{"f1-micro", "f1-micro", 1, 614, true, 200},
		{"g1-small", "g1-small", 1, 1740, true, 200},
		{"n2-highmem-128 lists 864 GB", "n2-highmem-128", 128, 884736, false, 200},
		{"n2d-standard-224", "n2d-standard-224", 224, 917504, false, 200},
		{"n2d-highcpu-128", "n2d-highcpu-128", 128, 131072, false, 200},
		{"n4-highcpu-8", "n4-highcpu-8", 8, 16384, false, 200},
		{"n4-highmem-80", "n4-highmem-80", 80, 655360, false, 200},
		{"c3-standard-22", "c3-standard-22", 22, 90112, false, 200},
		{"c3-highmem-176", "c3-highmem-176", 176, 1441792, false, 200},
		{"c3-highcpu-44", "c3-highcpu-44", 44, 90112, false, 200},
		{"t2d-standard-60", "t2d-standard-60", 60, 245760, false, 200},
		{"t2a-standard-48", "t2a-standard-48", 48, 196608, false, 200},
		{"n2-highmem-129", "n2-highmem-129", 0, 0, false, 404},
		{"n1-highmem-1", "n1-highmem-1", 0, 0, false, 404},
		{"custom", "custom-6-12288", 6, 12288, false, 200},
		{"unknown", "c9-mega-1", 0, 0, false, 404},
		{"bad custom", "custom-3-1024", 0, 0, false, 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			got, err := e.machineTypes(t).
				Get(
					t.Context(),
					&computepb.GetMachineTypeRequest{Project: project, Zone: zone, MachineType: tt.mt},
				)
			if tt.status != http.StatusOK {
				assertAPIError(t, err, tt.status, "notFound")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.cpus, got.GetGuestCpus())
			assert.Equal(t, tt.memMb, got.GetMemoryMb())
			assert.Equal(t, tt.shared, got.GetIsSharedCpu())
			assert.Equal(
				t,
				"https://www.googleapis.com/compute/v1/projects/"+project+"/zones/"+zone+"/machineTypes/"+tt.mt,
				got.GetSelfLink(),
			)
			assert.Equal(t, zone, got.GetZone())
		})
	}
}

func TestMachineTypeListFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	it := e.machineTypes(t).List(t.Context(), &computepb.ListMachineTypesRequest{
		Project: project, Zone: zone, Filter: s(`name = "n2-standard-.*" AND guestCpus >= 16`), OrderBy: s("name desc"),
	})

	var names []string

	for {
		got, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			break
		}

		names = append(names, got.GetName())
	}

	assert.Equal(
		t,
		[]string{
			"n2-standard-96",
			"n2-standard-80",
			"n2-standard-64",
			"n2-standard-48",
			"n2-standard-32",
			"n2-standard-16",
			"n2-standard-128",
		},
		names,
	)
}

func TestProjectCommonMetadata(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	projects := e.projects(t)

	p, err := projects.Get(t.Context(), &computepb.GetProjectRequest{Project: project})
	require.NoError(t, err)
	assert.Equal(t, project, p.GetName())
	assert.Equal(t, "PREMIUM", p.GetDefaultNetworkTier())

	fp := p.GetCommonInstanceMetadata().GetFingerprint()
	require.NotEmpty(t, fp)

	_, err = projects.SetCommonInstanceMetadata(t.Context(), &computepb.SetCommonInstanceMetadataProjectRequest{
		Project: project,
		MetadataResource: &computepb.Metadata{
			Fingerprint: s("stale"),
			Items:       []*computepb.Items{{Key: s("enable-oslogin"), Value: s("TRUE")}},
		},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	op, err := projects.SetCommonInstanceMetadata(t.Context(), &computepb.SetCommonInstanceMetadataProjectRequest{
		Project: project,
		MetadataResource: &computepb.Metadata{
			Fingerprint: s(fp),
			Items:       []*computepb.Items{{Key: s("enable-oslogin"), Value: s("TRUE")}},
		},
	})
	e.done(t, op, err)

	p, err = projects.Get(t.Context(), &computepb.GetProjectRequest{Project: project})
	require.NoError(t, err)
	assert.Equal(t, "TRUE", p.GetCommonInstanceMetadata().GetItems()[0].GetValue())
	assert.NotEqual(t, fp, p.GetCommonInstanceMetadata().GetFingerprint())
}

func TestRegionCatalogue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		auto   string
		zones  []string
	}{
		{region: "africa-south1", auto: "10.218.0.0/20", zones: []string{"a", "b", "c"}},
		{region: "asia-southeast3", auto: "10.232.0.0/20", zones: []string{"a", "b", "c"}},
		{region: "me-central2", auto: "10.216.0.0/20", zones: []string{"a", "b", "c"}},
		{region: "northamerica-south1", auto: "10.224.0.0/20", zones: []string{"a", "b", "c"}},
		{region: "us-west4", auto: "10.182.0.0/20", zones: []string{"a", "b", "c"}},
		{region: "europe-west1", auto: "10.132.0.0/20", zones: []string{"b", "c", "d"}},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			r, err := e.regions(t).Get(t.Context(), &computepb.GetRegionRequest{Project: project, Region: tt.region})
			require.NoError(t, err)
			assert.Len(t, r.GetZones(), len(tt.zones))

			for _, z := range tt.zones {
				assert.Contains(t, r.GetZones(), "https://www.googleapis.com/compute/v1/projects/"+project+
					"/zones/"+tt.region+"-"+z)
			}

			e.network(t, "auto", true)

			sub, serr := e.subnets(t).Get(t.Context(), &computepb.GetSubnetworkRequest{
				Project: project, Region: tt.region, Subnetwork: "auto",
			})
			require.NoError(t, serr)
			assert.Equal(t, tt.auto, sub.GetIpCidrRange())
		})
	}
}
