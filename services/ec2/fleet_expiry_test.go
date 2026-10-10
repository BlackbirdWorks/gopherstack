package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestFleet_ValidUntilExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantState  string
		validUntil time.Duration
	}{
		{name: "expired", validUntil: -time.Minute, wantState: "deleted_running"},
		{name: "not yet", validUntil: time.Hour, wantState: "active"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			f, _, err := b.CreateFleet(ec2.FleetCreateInput{
				Type:                "maintain",
				TotalTargetCapacity: 1,
				ValidUntil:          time.Now().Add(tc.validUntil),
			})
			require.NoError(t, err)

			b.TickLifecycleForTest()

			got := b.DescribeFleets([]string{f.FleetID})
			require.Len(t, got, 1)
			assert.Equal(t, tc.wantState, got[0].FleetState)
		})
	}
}
