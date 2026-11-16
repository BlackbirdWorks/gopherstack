package gcpeventarc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketMatchesLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, bucket, trigger string
		want                  bool
	}{
		{"same region", "US-CENTRAL1", "us-central1", true},
		{"other region", "US-CENTRAL1", "europe-west1", false},
		{"multi region trigger us", "US", "us", true},
		{"multi region trigger eu", "EU", "eu", true},
		{"multi region trigger asia", "ASIA", "asia", true},
		{"multi region region", "EU", "europe-west1", true},
		{"multi region other", "US", "europe-west1", false},
		{"dual region", "NAM4", "us-central1", true},
		{"dual region eur4", "EUR4", "europe-west4", true},
		{"configurable dual", "US-CENTRAL1+US-EAST1", "us-east1", true},
		{"global", "US-CENTRAL1", "global", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, bucketMatchesLocation(tt.bucket, tt.trigger))
		})
	}
}
