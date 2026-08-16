package iot_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "empty_thing_name",
			method: http.MethodPost,
			path:   "/things/",
			body:   map[string]any{},
		},
		{
			name:   "empty_policy_name",
			method: http.MethodPost,
			path:   "/policies/",
			body:   map[string]any{"policyDocument": "{}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_ErrAlreadyExists verifies duplicate resources return HTTP 409.
func TestErrAlreadyExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "duplicate_thing",
			method: http.MethodPost,
			path:   "/things/dup-thing",
			body:   map[string]any{},
		},
		{
			name:   "duplicate_policy",
			method: http.MethodPost,
			path:   "/policies/dup-policy",
			body:   map[string]any{"policyDocument": "{}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			// First call succeeds.
			rec1 := doRefRequest(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Second call conflicts.
			rec2 := doRefRequest(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, http.StatusConflict, rec2.Code)
		})
	}
}

func TestErrorFormat_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *iot.Handler)
		name         string
		method       string
		path         string
		wantType     string
		wantStatus   int
		checkMessage bool
	}{
		{
			name:         "NotFound",
			method:       http.MethodGet,
			path:         "/things/nonexistent-thing",
			wantStatus:   http.StatusNotFound,
			wantType:     "ResourceNotFoundException",
			checkMessage: true,
		},
		{
			name: "AlreadyExists",
			setup: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				r3Req(t, h, http.MethodPost, "/things/dup-thing", nil)
			},
			method:       http.MethodPost,
			path:         "/things/dup-thing",
			wantStatus:   http.StatusConflict,
			wantType:     "ResourceAlreadyExistsException",
			checkMessage: true,
		},
		{
			name:       "CertNotFound",
			method:     http.MethodGet,
			path:       "/certificates/" + strings.Repeat("a", 64),
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "RuleNotFound",
			method:     http.MethodGet,
			path:       "/rules/missing-rule",
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newR3Handler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			var out map[string]string
			code := r3JSON(t, h, tt.method, tt.path, nil, &out)
			assert.Equal(t, tt.wantStatus, code)
			assert.Equal(t, tt.wantType, out["__type"])
			if tt.checkMessage {
				assert.NotEmpty(t, out["message"])
			}
		})
	}
}

func TestErrVersionConflict_IsDistinctFromOtherErrors(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, iot.ErrVersionConflict, iot.ErrValidation)
	assert.NotEqual(t, iot.ErrVersionConflict, iot.ErrAlreadyExists)
	assert.NotEqual(t, iot.ErrVersionConflict, iot.ErrDeleteConflict)
}

func TestErrDeleteConflict_IsDistinctFromOtherErrors(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, iot.ErrDeleteConflict, iot.ErrValidation)
	assert.NotEqual(t, iot.ErrDeleteConflict, iot.ErrVersionConflict)
	assert.NotEqual(t, iot.ErrDeleteConflict, iot.ErrAlreadyExists)
}
