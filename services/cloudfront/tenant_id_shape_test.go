package cloudfront_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDistributionTenantGeneratedIDShapes(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "first"}, {name: "second"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			dist, err := b.CreateDistribution("ref", "test", true, nil)
			require.NoError(t, err)

			tenant, err := b.CreateDistributionTenant(dist.ID, "t", []string{"t.example.com"}, nil)
			require.NoError(t, err)
			assert.Regexp(t, `^dt_[0-9A-Za-z]{27}$`, tenant.ID)

			inv, err := b.CreateInvalidationForTenant(tenant.ID, "cr", []string{"/*"})
			require.NoError(t, err)
			assert.Regexp(t, `^I[0-9A-Z]{13}$`, inv.ID)
		})
	}
}
