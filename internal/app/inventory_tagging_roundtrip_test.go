package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

const inventoryRoundTripKey = "RoundTrip"

// countAliases counts inventory entries other than arnStr that share its last segment, which is what a tag
// store keyed by a different ARN spelling than the enumerator produces: one resource listed twice.
func countAliases(inv map[string]rgtapibackend.TaggedResource, arnStr string) int {
	n := 0

	for other := range inv {
		if other != arnStr && lastARNSegment(other) == lastARNSegment(arnStr) {
			n++
		}
	}

	return n
}

// TestInventoryTaggingRoundTrip tags an untagged resource through the tagging API, then untags it, and holds
// the tagged listing and the inventory to the same view of it at each step: tagging must surface it in both with
// the tag and never list it twice, and untagging must drop it from GetResources while the inventory keeps it.
func TestInventoryTaggingRoundTrip(t *testing.T) {
	t.Parallel()

	env := newInventoryEnv(t)
	ctx := t.Context()

	arns := make(map[string]string)

	for _, tc := range inventoryCases() {
		arns[tc.name] = tc.create(ctx, t, env.cfg, "rt-"+tc.name+"-plain", false)
	}

	created := env.inventoryARNs(ctx)

	failed := make(map[string]bool)

	for name, arnStr := range arns {
		out, err := env.rgt.TagResources(ctx, &rgtapibackend.TagResourcesInput{
			ResourceARNList: []string{arnStr},
			Tags:            map[string]string{inventoryRoundTripKey: "yes"},
		})
		require.NoError(t, err)

		failed[name] = len(out.FailedResourcesMap) > 0
	}

	afterTag := env.inventoryARNs(ctx)
	taggedAfterTag := env.taggedARNs(ctx, t)

	for name, arnStr := range arns {
		_, err := env.rgt.UntagResources(ctx, &rgtapibackend.UntagResourcesInput{
			ResourceARNList: []string{arnStr},
			TagKeys:         []string{inventoryRoundTripKey},
		})
		require.NoError(t, err, name)
	}

	afterUntag := env.inventoryARNs(ctx)
	taggedAfterUntag := env.taggedARNs(ctx, t)

	for _, tc := range inventoryCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			arnStr := arns[tc.name]
			require.NotEmpty(t, arnStr)
			require.False(t, failed[tc.name], "tagging API failed to tag %s", arnStr)

			assert.Contains(t, created, arnStr)

			assert.Equal(t, "yes", afterTag[arnStr].Tags[inventoryRoundTripKey], "inventory tags after tagging")
			assert.True(t, taggedAfterTag[arnStr], "GetResources lists the tagged resource")
			assert.Zero(t, countAliases(afterTag, arnStr), "tagged resource listed under a second ARN")

			assert.Contains(t, afterUntag, arnStr, "inventory keeps the untagged resource")
			assert.Empty(t, afterUntag[arnStr].Tags[inventoryRoundTripKey])
			assert.False(t, taggedAfterUntag[arnStr], "GetResources drops the untagged resource")
			assert.Zero(t, countAliases(afterUntag, arnStr), "untagged resource listed under a second ARN")
		})
	}
}
