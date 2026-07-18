package shield_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestHandler_AssociateDRTLogBucket tests the AssociateDRTLogBucket operation.
func TestHandler_AssociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body:       map[string]any{"LogBucket": "my-shield-logs"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing log bucket",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no subscription returns error",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{"LogBucket": "my-shield-logs"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doShieldRequest(t, h, "AssociateDRTLogBucket", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_AssociateDRTRole tests the AssociateDRTRole operation.
func TestHandler_AssociateDRTRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body:       map[string]any{"RoleArn": "arn:aws:iam::123456789012:role/DRTRole"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing role arn",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no subscription returns error",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{"RoleArn": "arn:aws:iam::123456789012:role/DRTRole"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doShieldRequest(t, h, "AssociateDRTRole", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeDRTAccess tests the DescribeDRTAccess operation.
func TestHandler_DescribeDRTAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*shield.Handler)
		name        string
		wantRoleArn string
		wantBuckets int
		wantStatus  int
	}{
		{
			name:        "empty drt access",
			setup:       func(_ *shield.Handler) {},
			wantStatus:  http.StatusOK,
			wantBuckets: 0,
			wantRoleArn: "",
		},
		{
			name: "with role and bucket",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
				require.NoError(t, h.Backend.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
				require.NoError(t, h.Backend.AssociateDRTLogBucket("my-logs-bucket"))
			},
			wantStatus:  http.StatusOK,
			wantBuckets: 1,
			wantRoleArn: "arn:aws:iam::123:role/DRTRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "DescribeDRTAccess", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			if tt.wantRoleArn != "" {
				assert.Equal(t, tt.wantRoleArn, result["RoleArn"])
			}

			buckets, _ := result["LogBucketList"].([]any)
			assert.Len(t, buckets, tt.wantBuckets)
		})
	}
}

// TestRefinement1_HTTPDisassociateDRTLogBucket tests via HTTP.
func TestHandler_DisassociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTLogBucket("my-bucket"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisassociateDRTLogBucket", map[string]any{"LogBucket": "my-bucket"})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPDisassociateDRTRole tests via HTTP.
func TestHandler_DisassociateDRTRole(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisassociateDRTRole", nil)
	assert.Equal(t, 200, rec.Code)
}
