package guardduty_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func TestInventoryResources_CustomRuleAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]string
		name       string
		wantTagged bool
	}{
		{name: "untagged_association", tags: nil, wantTagged: false},
		{name: "tagged_association", tags: map[string]string{"team": "emu"}, wantTagged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			rule := guardduty.CustomDetectionRule{RuleID: "rule-a", Name: "r"}
			require.NoError(t, b.RegisterCustomDetectionRule(rule))

			assoc, err := b.CreateCustomDetectionRuleAssociation("rule-a", "LIVE", tt.tags)
			require.NoError(t, err)

			inventory := map[string]bool{}
			for _, e := range b.InventoryResources() {
				inventory[e.ARN] = true
			}

			tagged := map[string]bool{}
			for _, e := range b.TaggedResources() {
				tagged[e.ARN] = true
			}

			var assocARN string

			for a := range inventory {
				if strings.HasSuffix(a, assoc.AssociationID) {
					assocARN = a
				}
			}

			require.NotEmpty(t, assocARN, "inventory must list the association")
			assert.Equal(t, tt.wantTagged, tagged[assocARN])
		})
	}
}
