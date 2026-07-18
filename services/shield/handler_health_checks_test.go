package shield_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestHandler_AssociateHealthCheck tests the AssociateHealthCheck operation.
func TestHandler_AssociateHealthCheck(t *testing.T) {
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
				require.NoError(t, h.Backend.CreateSubscription())
				p, err := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
				require.NoError(t, err)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ProtectionId":   id,
					"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "protection not found",
			setup: func(_ *shield.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ProtectionId":   id,
					"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing protection id",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{
					"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing health check arn",
			setup: func(h *shield.Handler) string {
				require.NoError(t, h.Backend.CreateSubscription())
				p, err := h.Backend.CreateProtection("p2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2", nil)
				require.NoError(t, err)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ProtectionId": id,
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
			rec := doShieldRequest(t, h, "AssociateHealthCheck", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_Gap19_AssociateHealthCheckValidARN verifies valid Route53 ARN accepted.
func TestHandler_AssociateHealthCheckValidARN(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "AssociateHealthCheck", map[string]any{
		"ProtectionId":   p.ID,
		"HealthCheckArn": "arn:aws:route53:::healthcheck/abc12345",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAudit_Gap19_AssociateHealthCheckInvalidARNRejected verifies non-Route53 ARN rejected.
func TestHandler_AssociateHealthCheckInvalidARNRejected(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "AssociateHealthCheck", map[string]any{
		"ProtectionId":   p.ID,
		"HealthCheckArn": "arn:aws:ec2:us-east-1::instance/i-1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Gap 20: TagResource subscription gate and limits ---

// TestRefinement1_HTTPDisassociateHealthCheck tests via HTTP.
func TestHandler_DisassociateHealthCheck(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	p := b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
	require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc-1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisassociateHealthCheck", map[string]any{
		"ProtectionId":   p.ID,
		"HealthCheckArn": "arn:aws:route53:::healthcheck/hc-1",
	})
	assert.Equal(t, 200, rec.Code)
}
