package athena_test

// parity_deepen: AWS-accuracy fixes for Athena (go-h2imn).
//
// Covers:
//   - GetSessionEndpoint: URL uses the backend's configured region, not hardcoded us-east-1
//   - StartQueryExecution: empty QueryString returns InvalidRequestException
//   - CreateWorkGroup: invalid State value returns InvalidRequestException
//   - UpdateWorkGroup: invalid State value returns InvalidRequestException
//   - ListDataCatalogs: NextToken field is omitted from the response (not sent as empty string)
//   - GetQueryResults: non-SUCCEEDED query returns InvalidRequestException

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestParity_GetSessionEndpoint_UsesConfiguredRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantPrefix string
	}{
		{
			name:       "us_east_1",
			region:     "us-east-1",
			wantPrefix: "https://athena.us-east-1.amazonaws.com/sessions/",
		},
		{
			name:       "eu_west_1",
			region:     "eu-west-1",
			wantPrefix: "https://athena.eu-west-1.amazonaws.com/sessions/",
		},
		{
			name:       "ap_southeast_2",
			region:     "ap-southeast-2",
			wantPrefix: "https://athena.ap-southeast-2.amazonaws.com/sessions/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := athena.NewHandler(athena.NewInMemoryBackend(tt.region, ""))

			startRec := doRequest(t, h, "StartSession", `{"WorkGroup":"primary"}`)
			require.Equal(t, http.StatusOK, startRec.Code)
			sessionID := jsonField(t, startRec.Body.Bytes(), "SessionId")
			require.NotEmpty(t, sessionID)

			body := `{"SessionId":"` + sessionID + `"}`
			rec := doRequest(t, h, "GetSessionEndpoint", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			url, _ := resp["SessionEndpoint"].(string)
			assert.Contains(t, url, tt.wantPrefix,
				"GetSessionEndpoint URL must use the configured region %q, not hardcoded us-east-1", tt.region)
		})
	}
}

func TestParity_StartQueryExecution_EmptyQueryStringRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_string_query_returns_400",
			body:       `{"QueryString":""}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "omitted_query_string_returns_400",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non_empty_query_string_succeeds",
			body:       `{"QueryString":"SELECT 1"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := doRequest(t, h, "StartQueryExecution", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException",
					"empty QueryString must return InvalidRequestException")
			}
		})
	}
}

func TestParity_WorkGroup_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		body       string
		wantStatus int
	}{
		{
			name:       "create_enabled_state_accepted",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-a","State":"ENABLED"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_disabled_state_accepted",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-b","State":"DISABLED"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_empty_state_defaults_to_enabled",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-c","State":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_invalid_state_returns_400",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-d","State":"ACTIVE"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update_invalid_state_returns_400",
			action:     "UpdateWorkGroup",
			body:       `{"WorkGroup":"primary","State":"UNKNOWN"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update_valid_state_accepted",
			action:     "UpdateWorkGroup",
			body:       `{"WorkGroup":"primary","State":"DISABLED"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException",
					"invalid State must return InvalidRequestException")
			}
		})
	}
}

func TestParity_ListDataCatalogs_NextTokenOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
	rec := doRequest(t, h, "ListDataCatalogs", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasNextToken := resp["NextToken"]
	assert.False(t, hasNextToken,
		"ListDataCatalogs must not include NextToken on last page; got %q", resp["NextToken"])
}

func TestParity_GetQueryResults_NonSucceededQueryRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setState   string
		wantStatus int
	}{
		{
			name:       "succeeded_query_returns_results",
			setState:   "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "cancelled_query_returns_400",
			setState:   "CANCELLED",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "failed_query_returns_400",
			setState:   "FAILED",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")
			h := athena.NewHandler(b)

			startRec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
			require.Equal(t, http.StatusOK, startRec.Code)
			execID := jsonField(t, startRec.Body.Bytes(), "QueryExecutionId")

			if tt.setState != "" {
				b.SetQueryExecutionState(execID, tt.setState, 0)
			}

			rec := doRequest(t, h, "GetQueryResults", `{"QueryExecutionId":"`+execID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException",
					"GetQueryResults on a non-SUCCEEDED query must return InvalidRequestException")
				msg, _ := errResp["message"].(string)
				assert.Contains(t, msg, tt.setState,
					"error message must include the current state")
			}
		})
	}
}
