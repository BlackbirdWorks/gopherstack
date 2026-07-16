package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminListUserAuthEvents_EmptyButValid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "auth-events-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "events-user")

	rec := doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
		"UserPoolId": poolID,
		"Username":   "events-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken  string           `json:"NextToken,omitempty"`
		AuthEvents []map[string]any `json:"AuthEvents,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.AuthEvents)
	assert.Empty(t, resp.NextToken)

	// Invalid pool.
	rec = doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
		"UserPoolId": "nonexistent-pool",
		"Username":   "events-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Unknown user.
	rec = doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
		"UserPoolId": poolID,
		"Username":   "ghost",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAuthEventFeedback_NotFoundAndValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "auth-event-feedback-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "feedback-user")

	// AdminUpdateAuthEventFeedback: no such event exists (no sign-in-event
	// producer is modeled), so this is a real, correct ResourceNotFoundException.
	rec := doCognitoRequest(t, h, "AdminUpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Invalid FeedbackValue is rejected before the not-found check would apply.
	rec = doCognitoRequest(t, h, "AdminUpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackValue": "Bogus",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Unknown user.
	rec = doCognitoRequest(t, h, "AdminUpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "ghost",
		"EventId":       "evt-1",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Non-admin UpdateAuthEventFeedback requires a FeedbackToken.
	rec = doCognitoRequest(t, h, "UpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// With a FeedbackToken present, it still 404s since no event is on record.
	rec = doCognitoRequest(t, h, "UpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackToken": "tok-123",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_AdminListUserAuthEvents_Validation covers the HTTP handler for
// AdminListUserAuthEvents: a valid pool/user returns an empty AuthEvents list, while
// unknown pools/users are rejected.
func TestHandler_AdminListUserAuthEvents_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "authevents-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "ae-user")

	t.Run("success_empty", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
			"UserPoolId": poolID,
			"Username":   "ae-user",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			AuthEvents []map[string]any `json:"AuthEvents"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Empty(t, resp.AuthEvents)
	})

	t.Run("user_not_found", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
			"UserPoolId": poolID,
			"Username":   "ghost",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
