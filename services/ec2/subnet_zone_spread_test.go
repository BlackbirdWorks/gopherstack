package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestCreateSubnet_ZoneLessSpreadsAcrossZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// zones holds the AvailabilityZone of each CreateSubnet call, in order ("" = omitted).
		zones []string
		want  []string
	}{
		{
			name:  "all_omitted_cycle_by_name",
			zones: []string{"", "", "", "", ""},
			want:  []string{"us-east-1a", "us-east-1b", "us-east-1c", "us-east-1a", "us-east-1b"},
		},
		{
			name:  "explicit_zone_counts_toward_usage",
			zones: []string{"us-east-1a", "", ""},
			want:  []string{"us-east-1a", "us-east-1b", "us-east-1c"},
		},
		{
			name:  "explicit_zone_kept_verbatim",
			zones: []string{"us-east-1c", "us-east-1c", ""},
			want:  []string{"us-east-1c", "us-east-1c", "us-east-1a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			vpc, err := b.CreateVpc("10.9.0.0/16", "default")
			require.NoError(t, err)

			got := make([]string, 0, len(tt.zones))

			for i, az := range tt.zones {
				s, serr := b.CreateSubnet(vpc.ID, "10.9."+string(rune('0'+i))+".0/24", az)
				require.NoError(t, serr)

				got = append(got, s.AvailabilityZone)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateSubnet_ZoneLessSpreadIsPerVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	v1, err := b.CreateVpc("10.1.0.0/16", "default")
	require.NoError(t, err)

	v2, err := b.CreateVpc("10.2.0.0/16", "default")
	require.NoError(t, err)

	_, err = b.CreateSubnet(v1.ID, "10.1.0.0/24", "")
	require.NoError(t, err)

	s2, err := b.CreateSubnet(v2.ID, "10.2.0.0/24", "")
	require.NoError(t, err)

	assert.Equal(t, "us-east-1a", s2.AvailabilityZone)
}
