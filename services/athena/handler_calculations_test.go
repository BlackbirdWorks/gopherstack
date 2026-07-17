package athena_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startCalc(t *testing.T, h *athena.Handler, sessionID string) string {
	t.Helper()

	rec := doRequest(t, h, "StartCalculationExecution",
		`{"SessionId":"`+sessionID+`","CodeBlock":"print(1)","Description":"hi"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "CalculationExecutionId")
}

func TestHandler_StartCalculationExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		terminate  bool
		wantStatus int
	}{
		{
			name:       "no_session",
			body:       `{"CodeBlock":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_session",
			body:       `{"SessionId":"missing","CodeBlock":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "terminated_session",
			terminate:  true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body

			if tt.terminate {
				id := startSession(t, h)
				doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
				body = `{"SessionId":"` + id + `","CodeBlock":"x"}`
			}

			rec := doRequest(t, h, "StartCalculationExecution", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetCalculationExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
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
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "GetCalculationExecution", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.NotEmpty(t, jsonField(t, rec.Body.Bytes(), tt.wantField))
			}
		})
	}
}

func TestHandler_GetCalculationExecutionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "GetCalculationExecutionStatus", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetCalculationExecutionCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantCode:   "print(1)",
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "GetCalculationExecutionCode", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, jsonField(t, rec.Body.Bytes(), "CodeBlock"))
			}
		})
	}
}

func TestHandler_StopCalculationExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantStatus int
	}{
		{
			name:       "terminal_fails",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "StopCalculationExecution", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBackend_StopCalculation_Cancellable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     string
		wantState string
		wantErr   bool
	}{
		{
			name:      "running_can_be_canceled",
			state:     "RUNNING",
			wantState: "CANCELED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend("", "")
			sid, _, err := backend.StartSession("primary", "", "",
				athena.EngineConfiguration{}, athena.SessionConfiguration{}, "")
			require.NoError(t, err)

			cid, _, err := backend.StartCalculationExecution(sid, "", "x")
			require.NoError(t, err)

			backend.SetCalculationState(cid, tt.state)
			got, err := backend.StopCalculationExecution(cid)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, got)
		})
	}
}

func TestHandler_ListCalculationExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		useSession bool
		wantStatus int
	}{
		{
			name:       "success",
			useSession: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "filter",
			useSession: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_session",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_session",
			body:       `{"SessionId":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body

			if tt.useSession {
				sid := startSession(t, h)
				startCalc(t, h, sid)

				if tt.name == "filter" {
					body = `{"SessionId":"` + sid + `","StateFilter":"NEVER"}`
				} else {
					body = `{"SessionId":"` + sid + `"}`
				}
			}

			rec := doRequest(t, h, "ListCalculationExecutions", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Capacity tests ---
