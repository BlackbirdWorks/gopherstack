package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeInstanceTypes_RealClient covers handler_instance_types.go's
// handleDescribeInstanceTypes against the real instance_type_catalog.go data
// (2026-09-11 de-stub: the op previously just echoed back whatever
// InstanceType.N values a caller named, with no real attributes).
func TestDescribeInstanceTypes_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	t.Run("known types return real catalog attributes", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceTypes(t.Context(), &ec2sdk.DescribeInstanceTypesInput{
			InstanceTypes: []types.InstanceType{"t3.micro", "m5.large"},
		})
		require.NoError(t, err)
		require.Len(t, out.InstanceTypes, 2)

		byType := make(map[string]types.InstanceTypeInfo, 2)
		for _, it := range out.InstanceTypes {
			byType[string(it.InstanceType)] = it
		}

		t3micro, ok := byType["t3.micro"]
		require.True(t, ok)
		assert.True(t, *t3micro.CurrentGeneration)
		assert.True(t, *t3micro.FreeTierEligible)
		assert.True(t, *t3micro.BurstablePerformanceSupported)
		assert.Equal(t, types.InstanceTypeHypervisorNitro, t3micro.Hypervisor)
		require.NotNil(t, t3micro.VCpuInfo)
		assert.EqualValues(t, 2, *t3micro.VCpuInfo.DefaultVCpus)
		require.NotNil(t, t3micro.MemoryInfo)
		assert.EqualValues(t, 1024, *t3micro.MemoryInfo.SizeInMiB)
		require.NotNil(t, t3micro.ProcessorInfo)
		assert.Equal(
			t,
			[]types.ArchitectureType{types.ArchitectureTypeX8664},
			t3micro.ProcessorInfo.SupportedArchitectures,
		)

		m5large, ok := byType["m5.large"]
		require.True(t, ok)
		assert.False(t, *m5large.CurrentGeneration)
		assert.False(t, *m5large.FreeTierEligible)
		assert.False(t, *m5large.BurstablePerformanceSupported)
		require.NotNil(t, m5large.VCpuInfo)
		assert.EqualValues(t, 2, *m5large.VCpuInfo.DefaultVCpus)
		require.NotNil(t, m5large.MemoryInfo)
		assert.EqualValues(t, 8192, *m5large.MemoryInfo.SizeInMiB)
	})

	t.Run("gpu family reports GpuInfo and instance storage", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceTypes(t.Context(), &ec2sdk.DescribeInstanceTypesInput{
			InstanceTypes: []types.InstanceType{"g4dn.xlarge"},
		})
		require.NoError(t, err)
		require.Len(t, out.InstanceTypes, 1)

		g4dn := out.InstanceTypes[0]
		require.NotNil(t, g4dn.GpuInfo)
		require.Len(t, g4dn.GpuInfo.Gpus, 1)
		assert.Equal(t, "T4", *g4dn.GpuInfo.Gpus[0].Name)
		assert.EqualValues(t, 1, *g4dn.GpuInfo.Gpus[0].Count)
		assert.True(t, *g4dn.InstanceStorageSupported)
		require.NotNil(t, g4dn.InstanceStorageInfo)
		assert.EqualValues(t, 125, *g4dn.InstanceStorageInfo.TotalSizeInGB)
	})

	t.Run("unknown instance type is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeInstanceTypes(t.Context(), &ec2sdk.DescribeInstanceTypesInput{
			InstanceTypes: []types.InstanceType{"bogus.type"},
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidInstanceType", apiErr.ErrorCode())
	})

	t.Run("filters narrow the catalog", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name      string
			wantIn    string
			wantNotIn string
			filters   []types.Filter
		}{
			{
				name:      "current-generation",
				filters:   []types.Filter{{Name: aws.String("current-generation"), Values: []string{"true"}}},
				wantIn:    "t3.micro",
				wantNotIn: "m5.large",
			},
			{
				name: "burstable-performance-supported",
				filters: []types.Filter{
					{Name: aws.String("burstable-performance-supported"), Values: []string{"true"}},
				},
				wantIn:    "t3.micro",
				wantNotIn: "c5.large",
			},
			{
				name: "processor-info.supported-architecture arm64",
				filters: []types.Filter{
					{Name: aws.String("processor-info.supported-architecture"), Values: []string{"arm64"}},
				},
				wantIn:    "m6g.large",
				wantNotIn: "m5.large",
			},
			{
				name:      "hypervisor xen",
				filters:   []types.Filter{{Name: aws.String("hypervisor"), Values: []string{"xen"}}},
				wantIn:    "t2.micro",
				wantNotIn: "t3.micro",
			},
			{
				name:      "instance-type wildcard",
				filters:   []types.Filter{{Name: aws.String("instance-type"), Values: []string{"c5.*"}}},
				wantIn:    "c5.large",
				wantNotIn: "c6i.large",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				out, err := client.DescribeInstanceTypes(t.Context(), &ec2sdk.DescribeInstanceTypesInput{
					Filters: tt.filters,
				})
				require.NoError(t, err)

				var names []string
				for _, it := range out.InstanceTypes {
					names = append(names, string(it.InstanceType))
				}

				assert.Contains(t, names, tt.wantIn)
				assert.NotContains(t, names, tt.wantNotIn)
			})
		}
	})

	t.Run("pagination pages through the whole catalog", func(t *testing.T) {
		t.Parallel()

		first, err := client.DescribeInstanceTypes(t.Context(), &ec2sdk.DescribeInstanceTypesInput{
			MaxResults: aws.Int32(10),
		})
		require.NoError(t, err)
		assert.Len(t, first.InstanceTypes, 10)
		require.NotNil(t, first.NextToken)
		assert.NotEmpty(t, *first.NextToken)

		second, err := client.DescribeInstanceTypes(t.Context(), &ec2sdk.DescribeInstanceTypesInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		assert.NotEqual(t, first.InstanceTypes[0].InstanceType, second.InstanceTypes[0].InstanceType)
	})
}
