package glacier_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		notifBody string
	}{
		{
			name:      "set_get_delete",
			vaultName: "notif-vault",
			notifBody: `{"SNSTopic":"arn:aws:sns:us-east-1:000000000000:topic","Events":["ArchiveRetrievalCompleted"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Set notifications
			rec = doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName+"/notification-configuration", tt.notifBody)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get notifications
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/notification-configuration", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete notifications
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/notification-configuration", "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete = 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/notification-configuration", "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestVaultNotifications_EventValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		events     []string
		wantStatus int
	}{
		{
			name:       "archive_retrieval_completed",
			events:     []string{"ArchiveRetrievalCompleted"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "inventory_retrieval_completed",
			events:     []string{"InventoryRetrievalCompleted"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "both_events",
			events:     []string{"ArchiveRetrievalCompleted", "InventoryRetrievalCompleted"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "unknown_event_rejected",
			events:     []string{"SomeUnknownEvent"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "mixed_valid_invalid_rejected",
			events:     []string{"ArchiveRetrievalCompleted", "BadEvent"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "notif-vault")

			eventsJSON, _ := json.Marshal(tt.events)
			body := fmt.Sprintf(`{"SNSTopic":"arn:aws:sns:us-east-1:000000000000:test","Events":%s}`, eventsJSON)
			rec := doRequest(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/notif-vault/notification-configuration", body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// -------------------------------------------------------------------------
// Issue 11: Vault lock 24-hour expiry enforcement
// -------------------------------------------------------------------------

func TestSetVaultNotifications_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid_config_ok",
			body:       `{"SNSTopic":"arn:aws:sns:us-east-1:123:topic","Events":["ArchiveRetrievalCompleted"]}`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "missing_sns_topic_rejected",
			body:       `{"SNSTopic":"","Events":["ArchiveRetrievalCompleted"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_events_rejected",
			body:       `{"SNSTopic":"arn:aws:sns:us-east-1:123:topic","Events":[]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no_topic_field_rejected",
			body:       `{"Events":["ArchiveRetrievalCompleted"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no_events_field_rejected",
			body:       `{"SNSTopic":"arn:aws:sns:us-east-1:123:topic"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "notif-req-vault")

			rec := doRequest(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/notif-req-vault/notification-configuration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 28: ListJobs completed param must be "true" or "false"
// -------------------------------------------------------------------------

func TestVaultNotifications_SetGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		topic  string
		events []string
	}{
		{
			name:   "set_and_get_notifications",
			topic:  "arn:aws:sns:us-east-1:123456789012:my-topic",
			events: []string{"ArchiveRetrievalCompleted", "InventoryRetrievalCompleted"},
		},
		{
			name:   "single_event",
			topic:  "arn:aws:sns:us-east-1:111111111111:single",
			events: []string{"InventoryRetrievalCompleted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "notif-vault-"+tt.name)

			// GET before set → 404.
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// SET.
			eventsJSON, _ := json.Marshal(tt.events)
			body := `{"SNSTopic":"` + tt.topic + `","Events":` + string(eventsJSON) + `}`
			rec = doRequestWithHeaders(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", body, nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET → matches.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var notifResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &notifResp))
			assert.Equal(t, tt.topic, notifResp["SNSTopic"])
			events, ok := notifResp["Events"].([]any)
			require.True(t, ok)
			assert.Len(t, events, len(tt.events))

			// DELETE.
			rec = doRequestWithHeaders(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET after delete → 404.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestVaultNotifications_InvalidEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		event      string
		wantStatus int
	}{
		{name: "invalid_event_rejected", event: "BogusEvent", wantStatus: http.StatusBadRequest},
		{name: "valid_event_accepted", event: "ArchiveRetrievalCompleted", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "notif-event-"+tt.name)

			body := `{"SNSTopic":"arn:aws:sns:us-east-1:000:t","Events":["` + tt.event + `"]}`
			rec := doRequestWithHeaders(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/notif-event-"+tt.name+"/notification-configuration", body, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 17. Access policy
// ─────────────────────────────────────────────────────────────────────────────
