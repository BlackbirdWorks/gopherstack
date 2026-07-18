package resourcegroups_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestAccountSettings_StatusMessage verifies GroupLifecycleEventsStatus mirrors desired.
func TestAccountSettings_StatusMessage(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	settings := b.GetAccountSettings()
	assert.Empty(t, settings.GroupLifecycleEventsDesiredStatus)

	err := b.UpdateAccountSettings("ACTIVE")
	require.NoError(t, err)
	settings = b.GetAccountSettings()
	assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsDesiredStatus)
	assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsStatus)

	err = b.UpdateAccountSettings("INACTIVE")
	require.NoError(t, err)
	settings = b.GetAccountSettings()
	assert.Equal(t, "INACTIVE", settings.GroupLifecycleEventsDesiredStatus)
	assert.Equal(t, "INACTIVE", settings.GroupLifecycleEventsStatus)
}

// TestAccountSettings_InvalidStatus verifies invalid status is rejected.
func TestAccountSettings_InvalidStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "empty", status: ""},
		{name: "invalid", status: "PENDING"},
		{name: "lowercase_active", status: "active"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.UpdateAccountSettings(tt.status)
			require.Error(t, err)
			assert.ErrorIs(t, err, resourcegroups.ErrValidation)
		})
	}
}
