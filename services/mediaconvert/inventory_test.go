package mediaconvert_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestInventoryResources_Jobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]string
		name       string
		wantTagged bool
	}{
		{name: "untagged_job", tags: nil, wantTagged: false},
		{name: "tagged_job", tags: map[string]string{"team": "emu"}, wantTagged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend("123456789012", "us-east-1")

			job, err := b.CreateJob("arn:aws:iam::123456789012:role/r", "", "", nil, tt.tags, nil, "")
			require.NoError(t, err)

			inventory := map[string]map[string]string{}
			for _, e := range b.InventoryResources() {
				inventory[e.ARN] = e.Tags
			}

			assert.Contains(t, inventory, job.Arn)
			assert.Equal(t, tt.tags, inventory[job.Arn])

			tagged := map[string]bool{}
			for _, e := range b.TaggedResources() {
				tagged[e.ARN] = true
			}

			assert.Equal(t, tt.wantTagged, tagged[job.Arn])
		})
	}
}
