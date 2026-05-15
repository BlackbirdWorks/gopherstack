package ec2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyntheticPublicDNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instanceID string
		region     string
		want       string
	}{
		{
			name:       "us_east_1_collapses_to_compute_1",
			instanceID: "i-3b492e3c6823a",
			region:     "us-east-1",
			want:       "ec2-3b492e3c6823a.compute-1.amazonaws.com",
		},
		{
			name:       "empty_region_defaults_to_compute_1",
			instanceID: "i-cafef00dcafef00d",
			region:     "",
			want:       "ec2-cafef00dcafef00d.compute-1.amazonaws.com",
		},
		{
			name:       "non_default_region_uses_region_compute_suffix",
			instanceID: "i-abc",
			region:     "eu-west-1",
			want:       "ec2-abc.eu-west-1.compute.amazonaws.com",
		},
		{
			name:       "instance_without_i_prefix_is_used_verbatim",
			instanceID: "weirdid",
			region:     "us-east-1",
			want:       "ec2-weirdid.compute-1.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := syntheticPublicDNS(tt.instanceID, tt.region)
			assert.Equal(t, tt.want, got)
		})
	}
}
