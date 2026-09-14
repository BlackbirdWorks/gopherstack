package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeInstanceTypeOfferings_RealClient covers handler_instance_types.go's
// handleDescribeInstanceTypeOfferings against the real catalog (2026-09-11
// DescribeInstanceTypes de-stub): default availability-zone offerings, the
// newly-supported LocationType "region", and the documented instance-type/
// location Filters.
func TestDescribeInstanceTypeOfferings_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	t.Run("default lists availability-zone offerings", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceTypeOfferings(t.Context(), &ec2sdk.DescribeInstanceTypeOfferingsInput{})
		require.NoError(t, err)
		require.NotEmpty(t, out.InstanceTypeOfferings)

		var foundLocations []string
		for _, o := range out.InstanceTypeOfferings {
			if string(o.InstanceType) == "t3.micro" {
				assert.Equal(t, types.LocationTypeAvailabilityZone, o.LocationType)
				foundLocations = append(foundLocations, *o.Location)
			}
		}
		assert.ElementsMatch(t, []string{"us-east-1a", "us-east-1b", "us-east-1c"}, foundLocations)
	})

	t.Run("region location type returns one row per type", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceTypeOfferings(t.Context(), &ec2sdk.DescribeInstanceTypeOfferingsInput{
			LocationType: types.LocationTypeRegion,
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.InstanceTypeOfferings)

		seen := make(map[string]int)
		for _, o := range out.InstanceTypeOfferings {
			assert.Equal(t, types.LocationTypeRegion, o.LocationType)
			assert.Equal(t, "us-east-1", *o.Location)
			seen[string(o.InstanceType)]++
		}
		assert.Equal(t, 1, seen["t3.micro"], "region-scoped offerings should list each type once")
	})

	t.Run("availability-zone-id has no fabricated data", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceTypeOfferings(t.Context(), &ec2sdk.DescribeInstanceTypeOfferingsInput{
			LocationType: types.LocationTypeAvailabilityZoneId,
		})
		require.NoError(t, err)
		assert.Empty(t, out.InstanceTypeOfferings)
	})

	t.Run("instance-type filter narrows the AZ listing", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceTypeOfferings(t.Context(), &ec2sdk.DescribeInstanceTypeOfferingsInput{
			Filters: []types.Filter{{Name: aws.String("instance-type"), Values: []string{"t3.micro"}}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.InstanceTypeOfferings)

		for _, o := range out.InstanceTypeOfferings {
			assert.Equal(t, types.InstanceType("t3.micro"), o.InstanceType)
		}
	})
}

// TestGetInstanceTypesFromInstanceRequirements_RealClient covers
// handler_instance_types.go's handleGetInstanceTypesFromInstanceRequirements
// real attribute-based matching engine (2026-09-11 de-stub: the op previously
// validated required-field presence only and always returned the same
// hardcoded 5-item list). Each case proves both inclusion and exclusion for
// one requirement attribute.
func TestGetInstanceTypesFromInstanceRequirements_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	baseReq := func() *types.InstanceRequirementsRequest {
		return &types.InstanceRequirementsRequest{
			VCpuCount: &types.VCpuCountRangeRequest{Min: aws.Int32(1)},
			MemoryMiB: &types.MemoryMiBRequest{Min: aws.Int32(512)},
		}
	}

	call := func(t *testing.T, arch types.ArchitectureType, req *types.InstanceRequirementsRequest) []string {
		t.Helper()

		out, err := client.GetInstanceTypesFromInstanceRequirements(
			t.Context(),
			&ec2sdk.GetInstanceTypesFromInstanceRequirementsInput{
				ArchitectureTypes:    []types.ArchitectureType{arch},
				VirtualizationTypes:  []types.VirtualizationType{types.VirtualizationTypeHvm},
				InstanceRequirements: req,
			},
		)
		require.NoError(t, err)

		names := make([]string, 0, len(out.InstanceTypes))
		for _, it := range out.InstanceTypes {
			names = append(names, *it.InstanceType)
		}

		return names
	}

	t.Run("default excludes burstable performance types", func(t *testing.T) {
		t.Parallel()

		names := call(t, types.ArchitectureTypeX8664, baseReq())
		assert.Contains(t, names, "m5.large")
		assert.NotContains(t, names, "t3.micro")
	})

	t.Run("burstable required returns only burstable types", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.BurstablePerformance = types.BurstablePerformanceRequired
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "t3.micro")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("arm64 architecture excludes x86_64 types", func(t *testing.T) {
		t.Parallel()

		names := call(t, types.ArchitectureTypeArm64, baseReq())
		assert.Contains(t, names, "m6g.large")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("excluded instance types wildcard removes a family", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.ExcludedInstanceTypes = []string{"m5.*"}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.NotContains(t, names, "m5.large")
		assert.Contains(t, names, "m6i.large")
	})

	t.Run("allowed instance types wildcard restricts to one family", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.AllowedInstanceTypes = []string{"c5.*"}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "c5.large")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("accelerator count minimum returns only gpu types", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.AcceleratorCount = &types.AcceleratorCountRequest{Min: aws.Int32(1)}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "g4dn.xlarge")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("accelerator manufacturer nvidia matches gpu families", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.AcceleratorManufacturers = []types.AcceleratorManufacturer{types.AcceleratorManufacturerNvidia}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "g5.xlarge")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("accelerator count max zero excludes gpu types", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.AcceleratorCount = &types.AcceleratorCountRequest{Max: aws.Int32(0)}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "m5.large")
		assert.NotContains(t, names, "g4dn.xlarge")
	})

	t.Run("local storage required returns only instance-store types", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.LocalStorage = types.LocalStorageRequired
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "i3.large")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("bare metal required returns nothing (no bare metal type is cataloged)", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.BareMetal = types.BareMetalRequired
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Empty(t, names)
	})

	t.Run("cpu manufacturer amd narrows to amd families", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.CpuManufacturers = []types.CpuManufacturer{types.CpuManufacturerAmd}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "m5a.large")
		assert.NotContains(t, names, "m5.large")
	})

	t.Run("instance generation previous excludes current generation", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.InstanceGenerations = []types.InstanceGeneration{types.InstanceGenerationPrevious}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "m5.large")
		assert.NotContains(t, names, "m6i.large")
	})

	t.Run("vcpu and memory ceilings exclude oversized types", func(t *testing.T) {
		t.Parallel()

		req := baseReq()
		req.VCpuCount = &types.VCpuCountRangeRequest{Min: aws.Int32(1), Max: aws.Int32(2)}
		req.MemoryMiB = &types.MemoryMiBRequest{Min: aws.Int32(512), Max: aws.Int32(8192)}
		names := call(t, types.ArchitectureTypeX8664, req)
		assert.Contains(t, names, "m5.large")
		assert.NotContains(t, names, "m5.4xlarge")
	})

	t.Run("required fields missing is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetInstanceTypesFromInstanceRequirements(
			t.Context(),
			&ec2sdk.GetInstanceTypesFromInstanceRequirementsInput{
				VirtualizationTypes:  []types.VirtualizationType{types.VirtualizationTypeHvm},
				InstanceRequirements: baseReq(),
			},
		)
		require.Error(t, err)
	})
}
