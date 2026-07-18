package acm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACMHandler_GetAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantDaysContains string
		wantCode         int
	}{
		{
			name:             "returns_default_45_days",
			wantCode:         http.StatusOK,
			wantDaysContains: "45",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "GetAccountConfiguration", `{}`)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantDaysContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantDaysContains)
			}
		})
	}
}

func TestACMHandler_PutAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "set_30_days",
			body:     `{"IdempotencyToken":"tok1","ExpiryEvents":{"DaysBeforeExpiry":30}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "idempotent_second_call_same_token",
			body:     `{"IdempotencyToken":"tok1","ExpiryEvents":{"DaysBeforeExpiry":30}}`,
			wantCode: http.StatusOK,
		},
		{
			name:         "missing_idempotency_token",
			body:         `{"ExpiryEvents":{"DaysBeforeExpiry":30}}`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
		{
			name:         "negative_days_before_expiry",
			body:         `{"IdempotencyToken":"tok2","ExpiryEvents":{"DaysBeforeExpiry":-1}}`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "PutAccountConfiguration", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_PutAndGetAccountConfiguration(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Set to 30 days
	putRec := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"abc","ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	// Get should reflect new value
	getRec := postACMJSON(t, h, "GetAccountConfiguration", `{}`)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "30")
	assert.NotContains(t, getRec.Body.String(), "45")
}

// TestACMHandler_PutAccountConfiguration_ConflictDetection verifies ErrConflict on token reuse.
func TestACMHandler_PutAccountConfiguration_ConflictDetection(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// First call: 30 days
	rec1 := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"conftest-1","ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second call: same token, same settings → idempotent OK
	rec2 := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"conftest-1","ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Third call: same token, different settings → conflict
	rec3 := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"conftest-1","ExpiryEvents":{"DaysBeforeExpiry":60}}`)
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
	assert.Contains(t, rec3.Body.String(), "ConflictException")
}
