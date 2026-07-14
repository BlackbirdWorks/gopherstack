package kms_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)
	assert.Positive(t, kms.HandlerOpsLen(h), "handler should have pre-built dispatch ops")
}

func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	// Verify sorted.
	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(
			t,
			ops[i-1],
			ops[i],
			"GetSupportedOperations should be sorted; found %q > %q",
			ops[i-1],
			ops[i],
		)
	}

	// Spot-check expected operations.
	for _, expected := range []string{
		"CreateKey", "Encrypt", "Decrypt", "VerifyMac", "GenerateMac",
		"ConnectCustomKeyStore", "DeriveSharedSecret",
	} {
		assert.Contains(t, ops, expected)
	}
}

func TestErrValidation_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *kms.InMemoryBackend) string
		name  string
		op    string
	}{
		{
			name: "ScheduleKeyDeletion_too_few_days",
			op:   "ScheduleKeyDeletion",
			setup: func(b *kms.InMemoryBackend) string {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				if err != nil {
					return `{}`
				}
				body, _ := json.Marshal(map[string]any{"KeyId": key.KeyMetadata.KeyID, "PendingWindowInDays": 3})

				return string(body)
			},
		},
		{
			name:  "GenerateRandom_zero_bytes",
			op:    "GenerateRandom",
			setup: func(_ *kms.InMemoryBackend) string { return `{"NumberOfBytes":0}` },
		},
		{
			name:  "GenerateRandom_over_limit",
			op:    "GenerateRandom",
			setup: func(_ *kms.InMemoryBackend) string { return `{"NumberOfBytes":2048}` },
		},
		{
			name:  "CreateCustomKeyStore_empty_name",
			op:    "CreateCustomKeyStore",
			setup: func(_ *kms.InMemoryBackend) string { return `{"CustomKeyStoreName":""}` },
		},
		{
			name: "GenerateMac_empty_algorithm",
			op:   "GenerateMac",
			setup: func(_ *kms.InMemoryBackend) string {
				return `{"KeyId":"nonexistent","MacAlgorithm":"","Message":"dGVzdA=="}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := kms.NewHandler(b)
			body := tt.setup(b)
			rec := sendKMSOp(t, h, tt.op, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}

// TestKMSHandlerMethodNotAllowed verifies non-POST requests are rejected.
func TestKMSHandlerMethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	req := httptest.NewRequest(http.MethodPut, "/something", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestKMSHandlerChaosOperations verifies the chaos-related handler methods.
func TestKMSHandlerChaosOperations(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)
	h.DefaultRegion = "eu-west-1"

	assert.Equal(t, "kms", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"eu-west-1"}, h.ChaosRegions())
}
