package shield_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				_ = h.Backend.CreateSubscription()
				p, _ := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)

				return p.ProtectionArn
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ResourceARN": id,
					"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "protection not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{
					"ResourceARN": "nonexistent-protection-id",
					"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{
					"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "TagResource", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				_ = h.Backend.CreateSubscription()
				p, _ := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
					map[string]string{"env": "prod"})

				return p.ProtectionArn
			},
			body: func(id string) map[string]any {
				return map[string]any{"ResourceARN": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "protection not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"ResourceARN": "nonexistent-id"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "ListTagsForResource", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				_ = h.Backend.CreateSubscription()
				p, _ := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
					map[string]string{"env": "prod"})

				return p.ProtectionArn
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ResourceARN": id,
					"TagKeys":     []string{"env"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "protection not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{
					"ResourceARN": "nonexistent-id",
					"TagKeys":     []string{"env"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"TagKeys": []string{"env"}}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "UntagResource", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
