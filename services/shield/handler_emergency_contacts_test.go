package shield_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestHandler_AssociateProactiveEngagementDetails tests AssociateProactiveEngagementDetails.
func TestHandler_AssociateProactiveEngagementDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success with email",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{
					{"EmailAddress": "security@example.com"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "success with full contact details",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{
					{
						"EmailAddress": "security@example.com",
						"PhoneNumber":  "+15555551234",
						"ContactNotes": "24/7 security team",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "empty contact list",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing email in contact",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{
					{"PhoneNumber": "+15555551234"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing contact list",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doShieldRequest(t, h, "AssociateProactiveEngagementDetails", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_HTTPDescribeEmergencyContactSettings tests via HTTP.
func TestHandler_DescribeEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "ops@example.com"},
	}))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeEmergencyContactSettings", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPUpdateEmergencyContactSettings tests via HTTP.
func TestHandler_UpdateEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(shield.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doShieldRequest(t, h, "UpdateEmergencyContactSettings", map[string]any{
		"EmergencyContactList": []map[string]any{
			{"EmailAddress": "sec@example.com"},
		},
	})
	assert.Equal(t, 200, rec.Code)
}
