package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutEvents_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			// AWS's PutEventsRequestEntryList requires at least 1 item
			// ("Array Members: Minimum number of 1 item"); an empty batch is
			// a validation error, not a no-op success. Previously this
			// backend accepted an empty batch as a 200 with zero results,
			// which is the wrong AWS behavior.
			name:     "empty entries returns validation error",
			body:     `{"Entries":[]}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, "PutEvents", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantCode        int
		wantEntryCount  int
		wantFailedCount int
	}{
		{
			name: "put multiple events returns entries with IDs and no failures",
			body: `{"Entries":[` +
				`{"Source":"my.app","DetailType":"UserCreated","Detail":"{\"userId\":\"1\"}"},` +
				`{"Source":"my.app","DetailType":"UserDeleted","Detail":"{}"}]}`,
			wantCode:        http.StatusOK,
			wantEntryCount:  2,
			wantFailedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			rec := makeRequestWithHandler(t, handler, e, "PutEvents", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp struct {
				Entries          []eventbridge.EventResultEntry `json:"Entries"`
				FailedEntryCount int                            `json:"FailedEntryCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantFailedCount, resp.FailedEntryCount)
			assert.Len(t, resp.Entries, tt.wantEntryCount)
			for _, entry := range resp.Entries {
				assert.NotEmpty(t, entry.EventID)
			}
		})
	}
}
