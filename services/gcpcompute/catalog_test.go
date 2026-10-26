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

	assert.Equal(t, 15, count)
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
