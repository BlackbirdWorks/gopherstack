package mediapackage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestBackend_CreateOriginEndpoint_SetsCreatedAt verifies CreateOriginEndpoint
// populates CreatedAt, matching real MediaPackage's OriginEndpoint shape.
func TestBackend_CreateOriginEndpoint_SetsCreatedAt(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	ep, err := b.CreateOriginEndpoint("chan1", "ep1", "", "", 0, 0, "", nil, nil, mediapackage.PackagingConfig{})
	require.NoError(t, err)
	assert.NotEmpty(t, ep.CreatedAt)
}

// TestBackend_OriginEndpoint_PackagingConfigRoundTrip verifies that the
// opaque CDN-authorization and per-protocol packaging blocks
// (authorization/cmafPackage/dashPackage/hlsPackage/mssPackage) survive a
// Create then Describe -- these were previously accepted in
// CreateOriginEndpoint's request shape but silently discarded, so a
// Terraform/CDK OriginEndpoint configured with e.g. hlsPackage never
// round-tripped.
func TestBackend_OriginEndpoint_PackagingConfigRoundTrip(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	pkg := mediapackage.PackagingConfig{
		Authorization: map[string]any{
			"cdnIdentifierSecret": "arn:aws:secretsmanager:1",
			"secretsRoleArn":      "arn:aws:iam:1",
		},
		HlsPackage:  map[string]any{"segmentDurationSeconds": float64(6)},
		DashPackage: map[string]any{"segmentDurationSeconds": float64(4)},
		CmafPackage: map[string]any{"segmentDurationSeconds": float64(2)},
		MssPackage:  map[string]any{"segmentDurationSeconds": float64(10)},
	}

	created, err := b.CreateOriginEndpoint("chan1", "ep1", "", "", 0, 0, "", nil, nil, pkg)
	require.NoError(t, err)
	assert.Equal(t, pkg.Authorization, created.Authorization)
	assert.Equal(t, pkg.HlsPackage, created.HlsPackage)
	assert.Equal(t, pkg.DashPackage, created.DashPackage)
	assert.Equal(t, pkg.CmafPackage, created.CmafPackage)
	assert.Equal(t, pkg.MssPackage, created.MssPackage)

	described, err := b.DescribeOriginEndpoint("ep1")
	require.NoError(t, err)
	assert.Equal(t, pkg.Authorization, described.Authorization)
	assert.Equal(t, pkg.HlsPackage, described.HlsPackage)
	assert.Equal(t, pkg.DashPackage, described.DashPackage)
	assert.Equal(t, pkg.CmafPackage, described.CmafPackage)
	assert.Equal(t, pkg.MssPackage, described.MssPackage)
}

// TestBackend_OriginEndpoint_PackagingConfig_UpdatePartial verifies
// UpdateOriginEndpoint only overwrites the packaging blocks explicitly
// provided (nil map means "not sent"), leaving the others as configured at
// create time.
func TestBackend_OriginEndpoint_PackagingConfig_UpdatePartial(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	initial := mediapackage.PackagingConfig{
		HlsPackage:  map[string]any{"segmentDurationSeconds": float64(6)},
		DashPackage: map[string]any{"segmentDurationSeconds": float64(4)},
	}
	_, err = b.CreateOriginEndpoint("chan1", "ep1", "", "", 0, 0, "", nil, nil, initial)
	require.NoError(t, err)

	update := mediapackage.PackagingConfig{
		HlsPackage: map[string]any{"segmentDurationSeconds": float64(8)},
	}
	updated, err := b.UpdateOriginEndpoint("ep1", "", "", -1, -1, "", nil, update)
	require.NoError(t, err)

	assert.Equal(t, update.HlsPackage, updated.HlsPackage, "hlsPackage should be replaced")
	assert.Equal(t, initial.DashPackage, updated.DashPackage, "dashPackage should be unchanged")
}
