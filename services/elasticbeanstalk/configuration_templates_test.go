package elasticbeanstalk_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_UpdateConfigurationTemplate_BumpsDateUpdated verifies that
// UpdateConfigurationTemplate advances DateUpdated on every mutation, not just
// at creation time.
func TestInMemoryBackend_UpdateConfigurationTemplate_BumpsDateUpdated(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateApplication(context.Background(), "app3", "", nil)
	require.NoError(t, err)
	tmpl, err := b.CreateConfigurationTemplate(context.Background(), "app3", "tmpl1", "orig", "", nil)
	require.NoError(t, err)
	created := tmpl.DateUpdated

	time.Sleep(time.Second)

	updated, err := b.UpdateConfigurationTemplate(context.Background(), "app3", "tmpl1", "new desc")
	require.NoError(t, err)
	assert.NotEqual(t, created, updated.DateUpdated)
}
