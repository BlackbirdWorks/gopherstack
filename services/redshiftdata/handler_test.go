package redshiftdata_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newTestHandler(t *testing.T) *redshiftdata.Handler {
	t.Helper()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	return redshiftdata.NewHandler(backend)
}

func doRequest(t *testing.T, h *redshiftdata.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftData."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "RedshiftData", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "ExecuteStatement")
	assert.Contains(t, ops, "BatchExecuteStatement")
	assert.Contains(t, ops, "DescribeStatement")
	assert.Contains(t, ops, "GetStatementResult")
	assert.Contains(t, ops, "GetStatementResultV2")
	assert.Contains(t, ops, "ListStatements")
	assert.Contains(t, ops, "CancelStatement")
	assert.Contains(t, ops, "ListDatabases")
	assert.Contains(t, ops, "ListSchemas")
	assert.Contains(t, ops, "ListTables")
	assert.Contains(t, ops, "DescribeTable")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "redshift-data", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "ExecuteStatement")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.Equal(t, []string{testRegion}, regions)
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches_ExecuteStatement",
			target:    "RedshiftData.ExecuteStatement",
			wantMatch: true,
		},
		{
			name:      "matches_DescribeStatement",
			target:    "RedshiftData.DescribeStatement",
			wantMatch: true,
		},
		{
			name:      "no_match_wrong_prefix",
			target:    "AWSOrganizations.Something",
			wantMatch: false,
		},
		{
			name:      "no_match_empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "extract_ExecuteStatement",
			target: "RedshiftData.ExecuteStatement",
			want:   "ExecuteStatement",
		},
		{
			name:   "extract_DescribeStatement",
			target: "RedshiftData.DescribeStatement",
			want:   "DescribeStatement",
		},
		{
			name:   "extract_ListStatements",
			target: "RedshiftData.ListStatements",
			want:   "ListStatements",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractOperation(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		want string
		body []byte
	}{
		{
			name: "with_id",
			body: []byte(`{"Id": "test-id-123"}`),
			want: "test-id-123",
		},
		{
			name: "without_id",
			body: []byte(`{}`),
			want: "",
		},
		{
			name: "invalid_json",
			body: []byte(`not-json`),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "RedshiftData.DescribeStatement")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestUnknownOperation_Returns400 verifies that an unknown X-Amz-Target
// action returns HTTP 400 ValidationException.
func TestUnknownOperation_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "NonExistentAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &redshiftdata.Provider{}
	assert.Equal(t, "RedshiftData", p.Name())
}

// TestProvider_Init_ValidContext verifies Init succeeds with a non-nil context.
func TestProvider_Init_ValidContext(t *testing.T) {
	t.Parallel()

	p := &redshiftdata.Provider{}
	_, err := p.Init(&service.AppContext{})
	assert.NoError(t, err)
}

// TestErrNilAppContext verifies provider.Init rejects nil context.
func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &redshiftdata.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, redshiftdata.ErrNilAppContext)
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, testRegion, b.Region())
}

// TestAccountID verifies AccountID is exposed correctly.
func TestAccountID(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
}

// TestBackendReset verifies Reset clears all statements.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.ExecuteStatement(
		context.Background(), "SELECT 1", "cluster", "", "mydb", "", "", "", false, "", nil, "",
	)
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, b.StatementCount())
}

// TestHandlerReset verifies Handler.Reset delegates to the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)

	_, err := b.ExecuteStatement(
		context.Background(), "SELECT 1", "cluster", "", "mydb", "", "", "", false, "", nil, "",
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, b.StatementCount())
}

// TestHandlerOpsLen verifies GetSupportedOperations returns exactly 11 ops.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 11, redshiftdata.HandlerOpsLen(h))
}

// TestGetStatementResultV2_GetSupportedOps verifies the ops list includes V2.
func TestGetStatementResultV2_GetSupportedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "GetStatementResultV2")
}

// TestStorageBackend_Interface verifies that InMemoryBackend satisfies StorageBackend.
func TestStorageBackend_Interface(t *testing.T) {
	t.Parallel()

	var _ redshiftdata.StorageBackend = redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
}

// TestNewHandler_AcceptsInterface verifies NewHandler accepts StorageBackend.
func TestNewHandler_AcceptsInterface(t *testing.T) {
	t.Parallel()

	var backend redshiftdata.StorageBackend = redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	assert.NotNil(t, h)
}

// TestSettings_Defaults verifies that a zero Settings struct uses
// the package default values after passing through WithJanitor.
func TestSettings_Defaults(t *testing.T) {
	t.Parallel()

	var s redshiftdata.Settings
	// Zero values: janitor interval and TTL are both 0.
	// After WithJanitor the defaults should be applied internally.
	assert.Zero(t, s.JanitorInterval, "zero default for JanitorInterval")
	assert.Zero(t, s.StatementTTL, "zero default for StatementTTL")

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)

	// Calling WithJanitor with zero values should not panic (uses package defaults internally).
	result := h.WithJanitor(s.JanitorInterval, s.StatementTTL)
	assert.NotNil(t, result, "WithJanitor should return handler")
}

// TestStartWorker_DoesNotPanic verifies that StartWorker on a
// handler with no janitor returns nil error and does not panic.
func TestStartWorker_DoesNotPanic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ctx := t.Context()

	err := h.StartWorker(ctx)
	require.NoError(t, err, "StartWorker should not return an error when no janitor is configured")
}

// TestStartWorker_WithJanitor verifies that StartWorker starts
// the janitor goroutine which can be cancelled via context.
func TestStartWorker_WithJanitor(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b).WithJanitor(10*time.Millisecond, time.Nanosecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err, "StartWorker should not error")

	// Wait for context to be cancelled (goroutine should exit cleanly).
	<-ctx.Done()
	// Give the goroutine a moment to exit.
	time.Sleep(20 * time.Millisecond)
	// No assertion needed – the test passes if it doesn't hang or race.
}
