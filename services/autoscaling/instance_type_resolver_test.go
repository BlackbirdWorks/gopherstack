package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// fakeInstanceTypeResolver is a minimal autoscaling.InstanceTypeResolver fake
// for backend tests. It records every call so tests can assert on the
// InstanceRequirements/architectures/virtualizationTypes it was given, and
// returns a fixed match list (nil meaning "no real catalog match").
type fakeInstanceTypeResolver struct {
	matches []string
	calls   []resolveInstanceTypesCall
}

type resolveInstanceTypesCall struct {
	req                 autoscaling.InstanceRequirements
	architectures       []string
	virtualizationTypes []string
}

func (f *fakeInstanceTypeResolver) ResolveInstanceTypes(
	req autoscaling.InstanceRequirements, architectures, virtualizationTypes []string,
) []string {
	f.calls = append(f.calls, resolveInstanceTypesCall{
		req: req, architectures: architectures, virtualizationTypes: virtualizationTypes,
	})

	return f.matches
}

// newMixedInstancesGroupWithRequirements creates a MixedInstancesPolicy-backed
// group with a single override carrying ir and no explicit InstanceType,
// backed by fakeEC2Launcher's fixed "ami-template-123"/"t3.medium" template
// resolution (see ec2_launch_test.go).
func newMixedInstancesGroupWithRequirements(
	b *autoscaling.InMemoryBackend, name string, ir *autoscaling.InstanceRequirements,
) (*autoscaling.AutoScalingGroup, error) {
	return b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: name,
		MixedInstancesPolicy: &autoscaling.MixedInstancesPolicy{
			LaunchTemplate: autoscaling.MixedInstancesLaunchTemplate{
				LaunchTemplateSpecification: autoscaling.LaunchTemplateSpecification{
					LaunchTemplateName: "my-template",
				},
				Overrides: []autoscaling.LaunchTemplateOverride{
					{InstanceRequirements: ir},
				},
			},
		},
		MinSize:           0,
		MaxSize:           5,
		DesiredCapacity:   1,
		AvailabilityZones: []string{"us-east-1a"},
	})
}

// TestInMemoryBackend_InstanceTypeResolver proves a MixedInstancesPolicy
// override carrying only InstanceRequirements (no InstanceType) resolves
// through the wired InstanceTypeResolver -- picking the first real catalog
// match -- and that a nil resolver or an empty match list fall back to the
// launch template's own resolved instance type ("t3.medium" here) rather
// than fabricating one, per instanceTypeForOverride's documented fallback.
func TestInMemoryBackend_InstanceTypeResolver(t *testing.T) {
	t.Parallel()

	sampleReq := &autoscaling.InstanceRequirements{
		VCpuCount:            &autoscaling.IntRangeRequest{Min: new(int32(2)), Max: new(int32(2))},
		MemoryMiB:            &autoscaling.IntRangeRequest{Min: new(int32(8192)), Max: new(int32(8192))},
		AllowedInstanceTypes: []string{"m5.*"},
	}

	tests := []struct {
		ir                 *autoscaling.InstanceRequirements
		resolver           *fakeInstanceTypeResolver
		name               string
		wantType           string
		wantResolverCalled bool
	}{
		{
			name:     "no_resolver_falls_back_to_template_instance_type",
			ir:       sampleReq,
			resolver: nil,
			wantType: "t3.medium",
		},
		{
			name:               "resolver_wired_uses_first_match",
			ir:                 sampleReq,
			resolver:           &fakeInstanceTypeResolver{matches: []string{"m5.large", "m5.xlarge"}},
			wantType:           "m5.large",
			wantResolverCalled: true,
		},
		{
			name:               "resolver_wired_no_matches_falls_back_to_template_instance_type",
			ir:                 sampleReq,
			resolver:           &fakeInstanceTypeResolver{matches: nil},
			wantType:           "t3.medium",
			wantResolverCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			t.Cleanup(b.Close)

			launcher := &fakeEC2Launcher{}
			b.SetEC2Launcher(launcher)

			if tt.resolver != nil {
				b.SetInstanceTypeResolver(tt.resolver)
			}

			g, err := newMixedInstancesGroupWithRequirements(b, "asg-"+tt.name, tt.ir)
			require.NoError(t, err)
			require.Len(t, g.Instances, 1)
			require.Len(t, launcher.launches, 1)

			assert.Equal(t, tt.wantType, launcher.launches[0].spec.InstanceType)

			if tt.resolver == nil {
				return
			}

			if !tt.wantResolverCalled {
				assert.Empty(t, tt.resolver.calls)

				return
			}

			require.Len(t, tt.resolver.calls, 1)
			assert.Equal(t, *tt.ir, tt.resolver.calls[0].req)
			assert.ElementsMatch(t, []string{"x86_64", "arm64"}, tt.resolver.calls[0].architectures)
			assert.Equal(t, []string{"hvm"}, tt.resolver.calls[0].virtualizationTypes)
		})
	}
}

// TestInMemoryBackend_InstanceTypeResolver_ExplicitInstanceTypeWins proves an
// override's own explicit InstanceType always wins over InstanceRequirements
// when both are set, and that the resolver isn't even consulted in that case
// -- matching the pre-existing precedence AWS documents for LaunchTemplateOverrides.
func TestInMemoryBackend_InstanceTypeResolver_ExplicitInstanceTypeWins(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	launcher := &fakeEC2Launcher{}
	b.SetEC2Launcher(launcher)

	resolver := &fakeInstanceTypeResolver{matches: []string{"m5.large"}}
	b.SetInstanceTypeResolver(resolver)

	g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "asg-explicit-wins",
		MixedInstancesPolicy: &autoscaling.MixedInstancesPolicy{
			LaunchTemplate: autoscaling.MixedInstancesLaunchTemplate{
				LaunchTemplateSpecification: autoscaling.LaunchTemplateSpecification{
					LaunchTemplateName: "my-template",
				},
				Overrides: []autoscaling.LaunchTemplateOverride{
					{
						InstanceType: "c5.large",
						InstanceRequirements: &autoscaling.InstanceRequirements{
							VCpuCount: &autoscaling.IntRangeRequest{Min: new(int32(2))},
							MemoryMiB: &autoscaling.IntRangeRequest{Min: new(int32(4096))},
						},
					},
				},
			},
		},
		MinSize:           0,
		MaxSize:           5,
		DesiredCapacity:   1,
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)
	require.Len(t, g.Instances, 1)
	require.Len(t, launcher.launches, 1)

	assert.Equal(t, "c5.large", launcher.launches[0].spec.InstanceType)
	assert.Empty(t, resolver.calls, "resolver must not be consulted when the override already sets InstanceType")
}
