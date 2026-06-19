package mediapackage_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestEndpointURLsFollowConfiguredRegion verifies ingest and egress endpoint
// URLs are built from the backend's configured region rather than a hardcoded
// us-east-1, matching how the resource ARNs are already built.
func TestEndpointURLsFollowConfiguredRegion(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("123412341234", "eu-central-1")

	ch, err := b.CreateChannel("chan1", "desc", nil)
	require.NoError(t, err)
	require.NotNil(t, ch.HlsIngest)
	require.NotEmpty(t, ch.HlsIngest.IngestEndpoints)

	for _, ep := range ch.HlsIngest.IngestEndpoints {
		assert.Contains(t, ep.URL, ".mediapackage.eu-central-1.amazonaws.com")
		assert.NotContains(t, ep.URL, "us-east-1")
	}

	ep, err := b.CreateOriginEndpoint("chan1", "ep1", "d", "index", 0, 0, "ALLOW", nil, nil)
	require.NoError(t, err)
	assert.True(t, strings.Contains(ep.URL, "cf.mediapackage.eu-central-1.amazonaws.com"),
		"egress URL should carry the configured region, got %q", ep.URL)
}
