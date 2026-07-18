package resourcegroups_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceGroupsHandler_GetAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "GetAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AccountSettings")
}

// TestGetAccountSettingsDefaults verifies default settings are returned.
func TestGetAccountSettingsDefaults(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "GetAccountSettings", nil)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AccountSettings")
}

// TestUpdateAccountSettings verifies the settings can be changed.
func TestUpdateAccountSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		desiredStatus  string
		wantStatusInDB string
		wantCode       int
	}{
		{
			name:           "set_active",
			desiredStatus:  "ACTIVE",
			wantCode:       http.StatusOK,
			wantStatusInDB: "ACTIVE",
		},
		{
			name:          "set_inactive",
			desiredStatus: "INACTIVE",
			wantCode:      http.StatusOK,
		},
		{
			name:          "invalid_status",
			desiredStatus: "BOGUS",
			wantCode:      http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "UpdateAccountSettings", map[string]any{
				"GroupLifecycleEventsDesiredStatus": tt.desiredStatus,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
