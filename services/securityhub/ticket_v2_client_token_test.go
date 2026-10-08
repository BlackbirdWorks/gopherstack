package securityhub_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTicketV2_ClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		firstTok   string
		secondTok  string
		secondUID  string
		wantSame   bool
		wantStatus int
	}{
		{
			name: "same_token_replays", firstTok: "tok", secondTok: "tok", secondUID: "uid-1",
			wantSame: true, wantStatus: http.StatusOK,
		},
		{name: "no_token_creates_new", secondUID: "uid-1", wantStatus: http.StatusOK},
		{
			name: "token_with_other_params", firstTok: "tok", secondTok: "tok", secondUID: "uid-2",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/connectorsv2", map[string]any{
				"Name": "c", "Provider": map[string]any{"JiraCloud": map[string]any{"ProjectKey": "SEC"}},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var conn map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &conn))

			create := func(token, uid string) *httptest.ResponseRecorder {
				body := map[string]any{"ConnectorId": conn["ConnectorId"], "FindingMetadataUid": uid}
				if token != "" {
					body["ClientToken"] = token
				}

				return doRequest(t, h, http.MethodPost, "/ticketsv2", body)
			}

			first := create(tt.firstTok, "uid-1")
			require.Equal(t, http.StatusOK, first.Code, first.Body.String())
			second := create(tt.secondTok, tt.secondUID)
			require.Equal(t, tt.wantStatus, second.Code, second.Body.String())

			if tt.wantStatus != http.StatusOK {
				return
			}

			var a, b map[string]any
			require.NoError(t, json.Unmarshal(first.Body.Bytes(), &a))
			require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b))
			assert.Equal(t, tt.wantSame, a["TicketId"] == b["TicketId"])
		})
	}
}
