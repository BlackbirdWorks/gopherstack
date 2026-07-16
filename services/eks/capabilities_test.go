package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestEKS_CreateCapability(t *testing.T) {
	t.Parallel()

	validBody := map[string]any{
		"capabilityName":          "my-capability",
		"type":                    "ARGOCD",
		"roleArn":                 "arn:aws:iam::123456789012:role/capability-role",
		"deletePropagationPolicy": "RETAIN",
	}

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "create_capability_success",
			body:       validBody,
			wantStatus: http.StatusOK,
		},
		{
			name: "create_capability_missing_name",
			body: map[string]any{
				"type": "ARGOCD", "roleArn": "arn:aws:iam::123456789012:role/x", "deletePropagationPolicy": "RETAIN",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_capability_duplicate",
			body:       validBody,
			wantStatus: http.StatusConflict,
		},
	}

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "cap-cluster"})
	// Pre-create duplicate for the last test.
	doREST(t, h, http.MethodPost, "/clusters/cap-cluster/capabilities", validBody)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var handler *eks.Handler
			if tt.name == "create_capability_duplicate" {
				handler = h
			} else {
				handler = newTestEKSHandler(t)
				doREST(t, handler, http.MethodPost, "/clusters", map[string]any{"name": "cap-cluster"})
			}

			rec := doREST(t, handler, http.MethodPost, "/clusters/cap-cluster/capabilities", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				capa, ok := resp["capability"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-capability", capa["capabilityName"])
				assert.Equal(t, "cap-cluster", capa["clusterName"])
				assert.Equal(t, "ACTIVE", capa["status"])
			}
		})
	}
}
