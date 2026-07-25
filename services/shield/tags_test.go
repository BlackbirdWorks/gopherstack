package shield_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestAudit_Gap1_TagResourceByShieldARN verifies TagResource accepts Shield protection ARNs.
func TestInMemoryBackend_TagResourceByShieldARN(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	err := b.TagResource(p.ProtectionArn, map[string]string{"key": "val"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

// TestAudit_Gap1_TagResourceByResourceARN verifies TagResource accepts resource ARNs.
func TestInMemoryBackend_TagResourceByResourceARN(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	resourceARN := eipARN("99")
	b.AddProtectionInternal("prot", resourceARN)

	err := b.TagResource(resourceARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(resourceARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
}

// TestAudit_Gap1_UntagResourceByShieldARN verifies UntagResource resolves Shield ARN.
func TestInMemoryBackend_UntagResourceByShieldARN(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("2"))
	require.NoError(t, b.TagResource(p.ProtectionArn, map[string]string{"k": "v"}))

	err := b.UntagResource(p.ProtectionArn, []string{"k"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestInMemoryBackend_TagResourceByShieldARNGovCloudPartition verifies TagResource resolves a
// Shield protection ARN whose partition is derived from the backend's region (arn:aws-us-gov:
// shield::...) rather than the hardcoded "arn:aws:shield::" prefix -- GovCloud/China/ISO region
// backends must be able to resolve their own protection ARNs back to a Protection.
func TestInMemoryBackend_TagResourceByShieldARNGovCloudPartition(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-gov-west-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", "arn:aws-us-gov:ec2:us-gov-west-1::eip-allocation/eipalloc-1")

	require.Contains(t, p.ProtectionArn, "arn:aws-us-gov:shield::")

	err := b.TagResource(p.ProtectionArn, map[string]string{"key": "val"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

// --- Gap 2: DescribeSubscription includes ProactiveEngagementStatus, Limits, SubscriptionLimits ---

// TestAudit_Gap20_TagResourceRequiresSubscription verifies subscription gate.
func TestInMemoryBackend_TagResourceRequiresSubscription(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	p := b.AddProtectionInternal("prot", eipARN("1"))

	err := b.TagResource(p.ProtectionArn, map[string]string{"k": "v"})
	require.Error(t, err, "TagResource without subscription should fail")
}

// TestAudit_Gap20_TagResourceKeyLengthLimit verifies 128-char key limit.
func TestInMemoryBackend_TagResourceKeyLengthLimit(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	longKey := strings.Repeat("k", 129)
	err := b.TagResource(p.ProtectionArn, map[string]string{longKey: "val"})
	require.Error(t, err, "Tag key > 128 chars should be rejected")
}

// TestAudit_Gap20_TagResourceValueLengthLimit verifies 256-char value limit.
func TestInMemoryBackend_TagResourceValueLengthLimit(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	longVal := strings.Repeat("v", 257)
	err := b.TagResource(p.ProtectionArn, map[string]string{"key": longVal})
	require.Error(t, err, "Tag value > 256 chars should be rejected")
}

// TestAudit_Gap20_TagResourceCap50Tags verifies 50-tag limit.
func TestInMemoryBackend_TagResourceCap50Tags(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	// Add 50 tags.
	const maxTags = 50
	tags := make(map[string]string, maxTags)

	for i := range maxTags {
		tags["key"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "val"
	}

	require.NoError(t, b.TagResource(p.ProtectionArn, tags))

	// Attempt to add one more.
	err := b.TagResource(p.ProtectionArn, map[string]string{"extra": "tag"})
	require.Error(t, err, "Adding beyond 50-tag limit should fail")
}

// --- Gap 22: SubscriptionArn format ---

// TestRefinement1_TagResource tests tagging.
func TestInMemoryBackend_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend) string
		tags    map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "tag_existing",
			setup: func(b *shield.InMemoryBackend) string {
				require.NoError(t, b.CreateSubscription())
				p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

				return p.ProtectionArn
			},
			tags: map[string]string{"env": "prod"},
		},
		{
			name: "tag_not_found",
			setup: func(*shield.InMemoryBackend) string {
				return "no-such-id"
			},
			tags:    map[string]string{"env": "prod"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(b)

			err := b.TagResource(id, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_UntagResource tests untagging.
func TestInMemoryBackend_UntagResource(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
	require.NoError(t, b.TagResource(p.ProtectionArn, map[string]string{"env": "prod", "owner": "team"}))

	err := b.UntagResource(p.ProtectionArn, []string{"env"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Contains(t, tags, "owner")
}
