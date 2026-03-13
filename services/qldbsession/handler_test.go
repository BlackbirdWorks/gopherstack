package qldbsession_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/qldbsession"
)

func newTestHandler() *qldbsession.Handler {
	backend := qldbsession.NewInMemoryBackend("000000000000", "us-east-1")

	return qldbsession.NewHandler(backend)
}

func sendCommand(t *testing.T, h *qldbsession.Handler, payload any) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "QLDBSession.SendCommand")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_StartSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ledgerName  string
		wantErrType string
		wantStatus  int
		wantToken   bool
	}{
		{
			name:       "success",
			ledgerName: "my-ledger",
			wantStatus: http.StatusOK,
			wantToken:  true,
		},
		{
			name:        "missing ledger name",
			ledgerName:  "",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var payload map[string]any
			if tt.ledgerName != "" {
				payload = map[string]any{
					"StartSession": map[string]any{"LedgerName": tt.ledgerName},
				}
			} else {
				payload = map[string]any{
					"StartSession": map[string]any{},
				}
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantToken {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				startSession, ok := resp["StartSession"].(map[string]any)
				require.True(t, ok, "StartSession should be present in response")
				assert.NotEmpty(t, startSession["SessionToken"])
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_StartTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*qldbsession.Handler) string
		name        string
		wantErrType string
		wantStatus  int
		wantTxID    bool
	}{
		{
			name: "success",
			setup: func(h *qldbsession.Handler) string {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return ""
				}

				return sess.Token
			},
			wantStatus: http.StatusOK,
			wantTxID:   true,
		},
		{
			name: "missing session token",
			setup: func(_ *qldbsession.Handler) string {
				return ""
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
		{
			name: "invalid session token",
			setup: func(_ *qldbsession.Handler) string {
				return "invalid-token"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			token := tt.setup(h)

			payload := map[string]any{
				"StartTransaction": map[string]any{},
			}

			if token != "" {
				payload["SessionToken"] = token
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantTxID {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				startTx, ok := resp["StartTransaction"].(map[string]any)
				require.True(t, ok, "StartTransaction should be present in response")
				assert.NotEmpty(t, startTx["TransactionId"])
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_ExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*qldbsession.Handler) (string, string)
		name        string
		wantErrType string
		wantStatus  int
		wantResult  bool
	}{
		{
			name: "success",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				txID, err := h.Backend.StartTransaction(sess.Token)
				if err != nil {
					return "", ""
				}

				return sess.Token, txID
			},
			wantStatus: http.StatusOK,
			wantResult: true,
		},
		{
			name: "missing session token",
			setup: func(_ *qldbsession.Handler) (string, string) {
				return "", "some-tx-id"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
		{
			name: "invalid transaction",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				return sess.Token, "nonexistent-tx"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			token, txID := tt.setup(h)

			payload := map[string]any{
				"ExecuteStatement": map[string]any{
					"Statement":     "SELECT * FROM mytable",
					"TransactionId": txID,
				},
			}

			if token != "" {
				payload["SessionToken"] = token
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				execStmt, ok := resp["ExecuteStatement"].(map[string]any)
				require.True(t, ok, "ExecuteStatement should be present in response")
				assert.NotNil(t, execStmt["FirstPage"])
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_FetchPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*qldbsession.Handler) (string, string)
		name        string
		wantErrType string
		wantStatus  int
		wantResult  bool
	}{
		{
			name: "success",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				txID, err := h.Backend.StartTransaction(sess.Token)
				if err != nil {
					return "", ""
				}

				return sess.Token, txID
			},
			wantStatus: http.StatusOK,
			wantResult: true,
		},
		{
			name: "missing session token",
			setup: func(_ *qldbsession.Handler) (string, string) {
				return "", "some-tx-id"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
		{
			name: "invalid session token",
			setup: func(_ *qldbsession.Handler) (string, string) {
				return "bad-token", "some-tx-id"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
		{
			name: "invalid transaction",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				return sess.Token, "nonexistent-tx"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
		{
			name: "missing transaction id",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				return sess.Token, ""
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			token, txID := tt.setup(h)

			payload := map[string]any{
				"FetchPage": map[string]any{
					"TransactionId": txID,
					"NextPageToken": "token",
				},
			}

			if token != "" {
				payload["SessionToken"] = token
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				fetchPage, ok := resp["FetchPage"].(map[string]any)
				require.True(t, ok, "FetchPage should be present in response")
				assert.NotNil(t, fetchPage["Page"])
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_CommitTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*qldbsession.Handler) (string, string)
		name        string
		wantErrType string
		wantStatus  int
		wantResult  bool
	}{
		{
			name: "success",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				txID, err := h.Backend.StartTransaction(sess.Token)
				if err != nil {
					return "", ""
				}

				return sess.Token, txID
			},
			wantStatus: http.StatusOK,
			wantResult: true,
		},
		{
			name: "invalid session",
			setup: func(_ *qldbsession.Handler) (string, string) {
				return "bad-token", "some-tx-id"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
		{
			name: "invalid transaction",
			setup: func(h *qldbsession.Handler) (string, string) {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				return sess.Token, "nonexistent-tx"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			token, txID := tt.setup(h)

			payload := map[string]any{
				"SessionToken": token,
				"CommitTransaction": map[string]any{
					"TransactionId": txID,
					"CommitDigest":  []byte("digest"),
				},
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				commitTx, ok := resp["CommitTransaction"].(map[string]any)
				require.True(t, ok, "CommitTransaction should be present in response")
				assert.Equal(t, txID, commitTx["TransactionId"])
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_AbortTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*qldbsession.Handler) string
		name        string
		wantErrType string
		wantStatus  int
		wantResult  bool
	}{
		{
			name: "success",
			setup: func(h *qldbsession.Handler) string {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return ""
				}

				return sess.Token
			},
			wantStatus: http.StatusOK,
			wantResult: true,
		},
		{
			name: "clears pending transactions",
			setup: func(h *qldbsession.Handler) string {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return ""
				}

				// Start a transaction so there's something to abort.
				_, err = h.Backend.StartTransaction(sess.Token)
				if err != nil {
					return ""
				}

				return sess.Token
			},
			wantStatus: http.StatusOK,
			wantResult: true,
		},
		{
			name: "invalid session",
			setup: func(_ *qldbsession.Handler) string {
				return "bad-token"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
		{
			name: "missing session token",
			setup: func(_ *qldbsession.Handler) string {
				return ""
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			token := tt.setup(h)

			payload := map[string]any{
				"AbortTransaction": map[string]any{},
			}

			if token != "" {
				payload["SessionToken"] = token
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				_, ok := resp["AbortTransaction"].(map[string]any)
				require.True(t, ok, "AbortTransaction should be present in response")
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_EndSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*qldbsession.Handler) string
		name        string
		wantErrType string
		wantStatus  int
		wantResult  bool
	}{
		{
			name: "success",
			setup: func(h *qldbsession.Handler) string {
				sess, err := h.Backend.StartSession("my-ledger")
				if err != nil {
					return ""
				}

				return sess.Token
			},
			wantStatus: http.StatusOK,
			wantResult: true,
		},
		{
			name: "invalid session",
			setup: func(_ *qldbsession.Handler) string {
				return "bad-token"
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidSessionException",
		},
		{
			name: "missing session token",
			setup: func(_ *qldbsession.Handler) string {
				return ""
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			token := tt.setup(h)

			payload := map[string]any{
				"EndSession": map[string]any{},
			}

			if token != "" {
				payload["SessionToken"] = token
			}

			rec := sendCommand(t, h, payload)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				_, ok := resp["EndSession"].(map[string]any)
				require.True(t, ok, "EndSession should be present in response")
			}

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetHeader string
		wantMatch    bool
	}{
		{
			name:         "matches QLDBSession target",
			targetHeader: "QLDBSession.SendCommand",
			wantMatch:    true,
		},
		{
			name:         "does not match other target",
			targetHeader: "DynamoDB_20120810.PutItem",
			wantMatch:    false,
		},
		{
			name:         "does not match empty target",
			targetHeader: "",
			wantMatch:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			require.NoError(t, err)

			if tt.targetHeader != "" {
				req.Header.Set("X-Amz-Target", tt.targetHeader)
			}

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_UnknownCommand(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := sendCommand(t, h, map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BadRequestException", resp["__type"])
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader([]byte("not-json")))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "QLDBSession.SendCommand")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_FullSessionLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Start session.
	startRec := sendCommand(t, h, map[string]any{
		"StartSession": map[string]any{"LedgerName": "test-ledger"},
	})

	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	startSessData, ok := startResp["StartSession"].(map[string]any)
	require.True(t, ok)
	sessionToken, ok := startSessData["SessionToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, sessionToken)

	// Start transaction.
	txRec := sendCommand(t, h, map[string]any{
		"SessionToken":     sessionToken,
		"StartTransaction": map[string]any{},
	})

	require.Equal(t, http.StatusOK, txRec.Code)

	var txResp map[string]any
	require.NoError(t, json.Unmarshal(txRec.Body.Bytes(), &txResp))
	startTxData, ok := txResp["StartTransaction"].(map[string]any)
	require.True(t, ok)
	txID, ok := startTxData["TransactionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, txID)

	// Execute statement.
	execRec := sendCommand(t, h, map[string]any{
		"SessionToken": sessionToken,
		"ExecuteStatement": map[string]any{
			"Statement":     "SELECT * FROM table",
			"TransactionId": txID,
		},
	})

	require.Equal(t, http.StatusOK, execRec.Code)

	// Commit transaction.
	commitRec := sendCommand(t, h, map[string]any{
		"SessionToken": sessionToken,
		"CommitTransaction": map[string]any{
			"TransactionId": txID,
			"CommitDigest":  []byte("digest"),
		},
	})

	require.Equal(t, http.StatusOK, commitRec.Code)

	// End session.
	endRec := sendCommand(t, h, map[string]any{
		"SessionToken": sessionToken,
		"EndSession":   map[string]any{},
	})

	require.Equal(t, http.StatusOK, endRec.Code)
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, "qldb", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_ServiceInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, "QLDBSession", h.Name())
	assert.Equal(t, []string{"SendCommand"}, h.GetSupportedOperations())
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	e := echo.New()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	require.NoError(t, err)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "SendCommand", h.ExtractOperation(c))
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantRes string
		body    []byte
	}{
		{
			name:    "start_session_returns_ledger_name",
			body:    []byte(`{"StartSession":{"LedgerName":"my-ledger"}}`),
			wantRes: "my-ledger",
		},
		{
			name:    "other_command_returns_empty",
			body:    []byte(`{"StartTransaction":{}}`),
			wantRes: "",
		},
		{
			name:    "invalid_json_returns_empty",
			body:    []byte(`not-json`),
			wantRes: "",
		},
		{
			name:    "empty_body_returns_empty",
			body:    []byte(`{}`),
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader(tt.body))
			require.NoError(t, err)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &qldbsession.Provider{}
	assert.Equal(t, "QLDBSession", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	backend := qldbsession.NewInMemoryBackend("000000000000", "us-west-2")
	assert.Equal(t, "us-west-2", backend.Region())
}

func TestBackend_ListSessions(t *testing.T) {
	t.Parallel()

	backend := qldbsession.NewInMemoryBackend("000000000000", "us-east-1")

	// Empty initially.
	assert.Empty(t, backend.ListSessions())

	// Start a session.
	_, err := backend.StartSession("my-ledger")
	require.NoError(t, err)

	sessions := backend.ListSessions()
	require.Len(t, sessions, 1)
	assert.Equal(t, "my-ledger", sessions[0].LedgerName)
}

func TestBackend_AbortTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*qldbsession.InMemoryBackend) (string, string)
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *qldbsession.InMemoryBackend) (string, string) {
				sess, err := b.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				txID, err := b.StartTransaction(sess.Token)
				if err != nil {
					return "", ""
				}

				return sess.Token, txID
			},
		},
		{
			name: "session_not_found",
			setup: func(_ *qldbsession.InMemoryBackend) (string, string) {
				return "bad-token", "some-tx"
			},
			wantErr: true,
		},
		{
			name: "transaction_not_found",
			setup: func(b *qldbsession.InMemoryBackend) (string, string) {
				sess, err := b.StartSession("my-ledger")
				if err != nil {
					return "", ""
				}

				return sess.Token, "nonexistent-tx"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := qldbsession.NewInMemoryBackend("000000000000", "us-east-1")
			token, txID := tt.setup(backend)

			err := backend.AbortTransaction(token, txID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
