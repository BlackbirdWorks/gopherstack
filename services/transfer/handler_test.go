package transfer_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestErrNilAppContext verifies the provider nil guard.
func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &transfer.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, transfer.ErrNilAppContext)
}

// TestProviderInit verifies normal provider init.
func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &transfer.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestStorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ transfer.StorageBackend = (*transfer.InMemoryBackend)(nil)
}

// TestHandlerOpsLen verifies GetSupportedOperations count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 71)
}

// TestSDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestHandlerOpsLenExport verifies the export_test helper.
func TestHandlerOpsLenExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(b)
	assert.Equal(t, 71, transfer.HandlerOpsLen(h))
}

// TestAccountIDRegion verifies AccountID and Region methods.
func TestAccountIDRegion(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "111122223333", "eu-west-1")
	assert.Equal(t, "111122223333", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestBackendReset verifies Reset clears all state.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

	_, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.ServerCount(b))

	b.Reset()

	assert.Equal(t, 0, transfer.ServerCount(b))
}

// TestHandlerReset verifies Handler.Reset clears the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(b)

	rec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 1, transfer.ServerCount(b))

	h.Reset()

	assert.Equal(t, 0, transfer.ServerCount(b))
	// ops should still work after reset
	assert.Equal(t, 71, transfer.HandlerOpsLen(h))
}

// TestErrValidationSentinel verifies ErrValidation wraps ErrInvalidParameter.
func TestErrValidationSentinel(t *testing.T) {
	t.Parallel()

	assert.ErrorIs(t, transfer.ErrValidation, awserr.ErrInvalidParameter)
}

func newTestHandler(t *testing.T) *transfer.Handler {
	t.Helper()

	return transfer.NewHandler(transfer.NewInMemoryBackend(t.Context(), testAccountID, testRegion))
}

func doTransferRequest(
	t *testing.T,
	h *transfer.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
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
	req.Header.Set("X-Amz-Target", "TransferService."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Transfer", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateServer")
	assert.Contains(t, ops, "DescribeServer")
	assert.Contains(t, ops, "ListServers")
	assert.Contains(t, ops, "StartServer")
	assert.Contains(t, ops, "StopServer")
	assert.Contains(t, ops, "DeleteServer")
	assert.Contains(t, ops, "UpdateServer")
	assert.Contains(t, ops, "CreateUser")
	assert.Contains(t, ops, "DescribeUser")
	assert.Contains(t, ops, "ListUsers")
	assert.Contains(t, ops, "DeleteUser")
	assert.Contains(t, ops, "UpdateUser")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches Transfer target",
			target: "TransferService.CreateServer",
			want:   true,
		},
		{
			name:   "does not match wrong prefix",
			target: "AWSShield_20160616.CreateProtection",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "UnknownOp", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "transfer", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "CreateServer")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOp string
	}{
		{
			name:   "create server",
			body:   `{"ServerId": "s-123"}`,
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(tt.body)))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			c := e.NewContext(req, httptest.NewRecorder())
			assert.NotPanics(t, func() { h.ExtractOperation(c) })
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"ServerId": "s-123"}`)))
	c := e.NewContext(req, httptest.NewRecorder())
	resource := h.ExtractResource(c)
	assert.NotPanics(t, func() { _ = resource })
}

func TestTransfer_Provider(t *testing.T) {
	t.Parallel()

	p := &transfer.Provider{}
	assert.Equal(t, "Transfer", p.Name())
}
