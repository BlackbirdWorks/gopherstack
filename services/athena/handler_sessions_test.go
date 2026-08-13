package athena_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jsonNested(t *testing.T, body []byte, keys ...string) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	cur := m

	for _, k := range keys {
		next, ok := cur[k].(map[string]any)
		require.True(t, ok, "missing nested key %q", k)
		cur = next
	}

	return cur
}

func startSession(t *testing.T, h *athena.Handler) string {
	t.Helper()

	rec := doRequest(t, h, "StartSession", `{"WorkGroup":"primary","Description":"test","NotebookVersion":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "SessionId")
}

func TestHandler_StartSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"WorkGroup":"primary","Description":"test","NotebookVersion":"v1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_workgroup",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_workgroup",
			body:       `{"WorkGroup":"nope"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "StartSession", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		wantField  string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantField:  "SessionId",
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			rec := doRequest(t, h, "GetSession", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.NotEmpty(t, jsonField(t, rec.Body.Bytes(), tt.wantField))
			}
		})
	}
}

func TestHandler_StartSession_MonitoringConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		block     string
		wantKey   string
		wantField string
		wantValue string
	}{
		{
			name:      "cloudwatch logging configuration round trips",
			block:     `"CloudWatchLoggingConfiguration": {"Enabled": true, "LogGroup": "/athena/sessions"}`,
			wantKey:   "CloudWatchLoggingConfiguration",
			wantField: "LogGroup",
			wantValue: "/athena/sessions",
		},
		{
			name:      "s3 logging configuration round trips",
			block:     `"S3LoggingConfiguration": {"Enabled": true, "LogLocation": "s3://bucket/logs/"}`,
			wantKey:   "S3LoggingConfiguration",
			wantField: "LogLocation",
			wantValue: "s3://bucket/logs/",
		},
		{
			name:      "managed logging configuration round trips",
			block:     `"ManagedLoggingConfiguration": {"Enabled": true, "KmsKey": "arn:aws:kms:us-east-1:0:key/k"}`,
			wantKey:   "ManagedLoggingConfiguration",
			wantField: "KmsKey",
			wantValue: "arn:aws:kms:us-east-1:0:key/k",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := `{"WorkGroup":"primary","MonitoringConfiguration":{` + tt.block + `}}`
			rec := doRequest(t, h, "StartSession", body)
			require.Equal(t, http.StatusOK, rec.Code)

			id := jsonField(t, rec.Body.Bytes(), "SessionId")

			getRec := doRequest(t, h, "GetSession", `{"SessionId":"`+id+`"}`)
			require.Equal(t, http.StatusOK, getRec.Code)

			monitoringCfg := jsonNested(t, getRec.Body.Bytes(), "MonitoringConfiguration")
			block, ok := monitoringCfg[tt.wantKey].(map[string]any)
			require.True(t, ok, "%s should round-trip", tt.wantKey)
			assert.Equal(t, true, block["Enabled"])
			assert.Equal(t, tt.wantValue, block[tt.wantField])
		})
	}
}

func TestHandler_GetSessionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		wantState  string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantState:  "IDLE",
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			rec := doRequest(t, h, "GetSessionStatus", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantState != "" {
				st := jsonNested(t, rec.Body.Bytes(), "Status")
				assert.Equal(t, tt.wantState, st["State"])
			}
		})
	}
}

func TestHandler_GetSessionEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sessionID    string
		wantContains string
		wantStatus   int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			rec := doRequest(t, h, "GetSessionEndpoint", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				assert.Contains(t, jsonField(t, rec.Body.Bytes(), "EndpointUrl"), id)
			}
		})
	}
}

func TestHandler_ListSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantNonEmpty bool
	}{
		{
			name:         "success",
			body:         `{"WorkGroup":"primary"}`,
			wantStatus:   http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name:       "filter_excludes",
			body:       `{"WorkGroup":"other","StateFilter":"BUSY"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_state_filter",
			body:       `{"WorkGroup":"primary","StateFilter":"NEVER"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			startSession(t, h)

			rec := doRequest(t, h, "ListSessions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantNonEmpty {
				var m map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
				assert.NotEmpty(t, m["Sessions"])
			}
		})
	}
}

func TestHandler_TerminateSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		wantState  string
		wantStatus int
		double     bool
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantState:  "TERMINATED",
		},
		{
			name:       "double_terminate",
			double:     true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			if tt.double {
				doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
			}

			rec := doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantState != "" {
				assert.Equal(t, tt.wantState, jsonField(t, rec.Body.Bytes(), "State"))
			}
		})
	}
}

func TestHandler_ListNotebookSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_empty_id",
			notebookID: "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.notebookID

			if tt.name == "success" {
				createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"nb1"}`)
				require.Equal(t, http.StatusOK, createRec.Code)
				notebookID = jsonField(t, createRec.Body.Bytes(), "NotebookId")

				startRec := doRequest(t, h, "StartSession",
					`{"WorkGroup":"primary","NotebookVersion":"v1","NotebookId":"`+notebookID+`"}`)
				require.Equal(t, http.StatusOK, startRec.Code)
			}

			rec := doRequest(t, h, "ListNotebookSessions", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Calculation tests ---

func TestHandler_ListEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "success",
			wantStatus:   http.StatusOK,
			wantContains: "engine version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListEngineVersions", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_ListEngineVersions_NoInventedAuthEngineVersionField locks in
// that ListEngineVersionsOutput's EngineVersions entries carry only the two
// fields the real types.EngineVersion has (EffectiveEngineVersion,
// SelectedEngineVersion) -- a previous "AuthEngineVersion" field here was a
// gopherstack invention with no counterpart on the real SDK type.
func TestHandler_ListEngineVersions_NoInventedAuthEngineVersionField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListEngineVersions", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string][]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp["EngineVersions"])

	for _, ev := range resp["EngineVersions"] {
		_, hasAuthEngineVersion := ev["AuthEngineVersion"]
		assert.False(t, hasAuthEngineVersion, "EngineVersion must not carry an invented AuthEngineVersion field")
		assert.NotEmpty(t, ev["EffectiveEngineVersion"])
	}
}

func TestHandler_ListApplicationDPUSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "success",
			wantStatus:   http.StatusOK,
			wantContains: "dpu",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListApplicationDPUSizes", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, strings.ToLower(rec.Body.String()), tt.wantContains)
		})
	}
}

func TestHandler_ListExecutors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sessionID   string
		stateFilter string
		terminate   bool
		wantStatus  int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:        "filtered_out",
			stateFilter: "TERMINATING",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "invalid_state_filter",
			stateFilter: "NEVER",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:       "validation_no_session",
			sessionID:  "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "terminated_session",
			terminate:  true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			sid := tt.sessionID

			needsSession := sid == "" && tt.name != "validation_no_session"
			if needsSession {
				sid = startSession(t, h)
			}

			if tt.terminate {
				doRequest(t, h, "TerminateSession", `{"SessionId":"`+sid+`"}`)
			}

			body := `{"SessionId":"` + sid + `"`
			if tt.stateFilter != "" {
				body += `,"ExecutorStateFilter":"` + tt.stateFilter + `"`
			}

			body += `}`

			rec := doRequest(t, h, "ListExecutors", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetResourceDashboard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"ResourceARN":"arn:aws:athena:us-east-1:000000000000:session/sess-1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_resource_arn_rejected",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetResourceDashboard", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				url := jsonField(t, rec.Body.Bytes(), "Url")
				assert.Contains(t, url, "athena.")
				assert.NotContains(t, rec.Body.String(), "ResourceDashboard")
			}
		})
	}
}

func TestSession_Calculation_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "session_start_and_get_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "StartSession",
					`{"WorkGroup":"primary","Description":"audit","NotebookVersion":"v1"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				sid, ok := m["SessionId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, sid)
				assert.NotEmpty(t, m["State"])

				rec = a1Do(t, h, "GetSession", `{"SessionId":"`+sid+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				gs := a1Unmarshal(t, rec)
				assert.Equal(t, sid, gs["SessionId"])
				assert.Equal(t, "primary", gs["WorkGroup"])
			},
		},
		{
			name: "calculation_start_get_code",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				sRec := a1Do(t, h, "StartSession",
					`{"WorkGroup":"primary","Description":"calc-test","NotebookVersion":"v1"}`)
				require.Equal(t, http.StatusOK, sRec.Code)
				sid := a1Unmarshal(t, sRec)["SessionId"].(string)

				cRec := a1Do(t, h, "StartCalculationExecution",
					`{"SessionId":"`+sid+`","CodeBlock":"print('hello')","Description":"test calc"}`)
				require.Equal(t, http.StatusOK, cRec.Code)
				cm := a1Unmarshal(t, cRec)
				cid, ok := cm["CalculationExecutionId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, cid)
				assert.NotEmpty(t, cm["State"])

				codeRec := a1Do(t, h, "GetCalculationExecutionCode",
					`{"CalculationExecutionId":"`+cid+`"}`)
				require.Equal(t, http.StatusOK, codeRec.Code)
				assert.Equal(t, "print('hello')", a1Unmarshal(t, codeRec)["CodeBlock"])
			},
		},
		{
			name: "terminate_session_makes_it_inaccessible_for_calcs",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				sRec := a1Do(t, h, "StartSession",
					`{"WorkGroup":"primary","Description":"term-test","NotebookVersion":"v1"}`)
				require.Equal(t, http.StatusOK, sRec.Code)
				sid := a1Unmarshal(t, sRec)["SessionId"].(string)

				rec := a1Do(t, h, "TerminateSession", `{"SessionId":"`+sid+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "TERMINATED", a1Unmarshal(t, rec)["State"])

				// Cannot start calculation in terminated session.
				cRec := a1Do(t, h, "StartCalculationExecution",
					`{"SessionId":"`+sid+`","CodeBlock":"x"}`)
				assert.Equal(t, http.StatusBadRequest, cRec.Code)
				assert.NotEmpty(t, a1Unmarshal(t, cRec)["__type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := a1Handler(t)
			tt.fn(t, h)
		})
	}
}

func TestGetSessionEndpoint_UsesConfiguredRegion(t *testing.T) {
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

			url, _ := resp["EndpointUrl"].(string)
			assert.Contains(t, url, tt.wantPrefix,
				"GetSessionEndpoint URL must use the configured region %q, not hardcoded us-east-1", tt.region)
		})
	}
}
