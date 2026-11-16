package appsync

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeltaReadableBoundsRetention(t *testing.T) {
	t.Parallel()

	const now = int64(1_800_000_000_000)

	tests := []struct {
		name     string
		ttl      int64
		lastSync int64
		want     bool
	}{
		{name: "inside", ttl: 60, lastSync: now - 30*millisPerMinute, want: true},
		{name: "outside", ttl: 60, lastSync: now - 61*millisPerMinute},
		{name: "overflowing_ttl_old_sync", ttl: math.MaxInt64 / 2, lastSync: 0},
		{name: "huge_ttl_capped", ttl: 10_000_000, lastSync: now - 2*maxDeltaTTLMinutes*millisPerMinute},
		{name: "negative_ttl", ttl: -5, lastSync: now - millisPerMinute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vs := versionedSource{cfg: &DynamoDBDataSourceConfig{DeltaSyncConfig: &DeltaSyncConfig{
				DeltaSyncTableName: "d", DeltaSyncTableTTL: tt.ttl,
			}}}

			assert.Equal(t, tt.want, vs.deltaReadable(tt.lastSync, now))
		})
	}
}
