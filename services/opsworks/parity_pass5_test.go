package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_UnknownAction_ReturnsValidationException verifies an unrecognized
// X-Amz-Target action returns HTTP 400 ValidationException, matching AWS, rather
// than HTTP 501 UnsupportedOperationException.
func TestParity_UnknownAction_ReturnsValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		wantType  string
		wantCode  int
	}{
		{
			name:      "unknown_action",
			operation: "ThisActionDoesNotExist",
			wantCode:  http.StatusBadRequest,
			wantType:  "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantType)
		})
	}
}
