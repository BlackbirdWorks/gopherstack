package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestPutPlaybackConfiguration_OnlyNameRequired verifies the backend only
// requires Name. A previous pass required AdDecisionServerUrl and
// VideoContentSourceUrl too, which rejects requests a real SDK client is
// allowed to send — confirmed against aws-sdk-go-v2/service/mediatailor's
// validators.go and botocore's service-2.json, whose only required member for
// PutPlaybackConfigurationInput is Name.
func TestPutPlaybackConfiguration_OnlyNameRequired(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	cfg, err := b.PutPlaybackConfiguration("cfg", "", "", nil)
	require.NoError(t, err, "AdDecisionServerUrl and VideoContentSourceUrl must both be optional")
	assert.Equal(t, "cfg", cfg.Name)

	_, err = b.PutPlaybackConfiguration("", "", "", nil)
	assert.Error(t, err, "Name must still be required")
}
