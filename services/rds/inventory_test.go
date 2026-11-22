package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestInventoryResources_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []rds.Tag
		wantTagged bool
	}{
		{name: "untagged_integration", tags: nil, wantTagged: false},
		{name: "tagged_integration", tags: []rds.Tag{{Key: "team", Value: "emu"}}, wantTagged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			intg, err := b.CreateIntegration("intg", "arn:aws:rds:us-east-1:123456789012:cluster:src",
				"arn:aws:redshift:us-east-1:123456789012:namespace:dst", "", "", "")
			require.NoError(t, err)

			if len(tt.tags) > 0 {
				b.AddTagsToResource(intg.IntegrationArn, tt.tags)
			}

			inventory := map[string]bool{}
			for _, e := range b.InventoryResources() {
				inventory[e.ARN] = true
			}

			tagged := map[string]bool{}
			for _, e := range b.TaggedResources() {
				tagged[e.ARN] = true
			}

			assert.True(t, inventory[intg.IntegrationArn])
			assert.Equal(t, tt.wantTagged, tagged[intg.IntegrationArn])
		})
	}
}
