package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestParity_EnableDisable_PerResourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(t *testing.T, h *inspector2.Handler)
		wantEC2     string
		wantECR     string
		wantLambda  string
		wantOverall string
	}{
		{
			name:        "all_disabled_by_default",
			setup:       func(_ *testing.T, _ *inspector2.Handler) {},
			wantEC2:     "DISABLED",
			wantECR:     "DISABLED",
			wantLambda:  "DISABLED",
			wantOverall: "DISABLED",
		},
		{
			name: "enable_ec2_only",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})
			},
			wantEC2:     "ENABLED",
			wantECR:     "DISABLED",
			wantLambda:  "DISABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_ecr_only",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"ECR"},
				})
			},
			wantEC2:     "DISABLED",
			wantECR:     "ENABLED",
			wantLambda:  "DISABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_lambda_only",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"LAMBDA"},
				})
			},
			wantEC2:     "DISABLED",
			wantECR:     "DISABLED",
			wantLambda:  "ENABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_ec2_ecr_disable_ec2_leaves_ecr_enabled",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2", "ECR"},
				})
				auditDo(t, h, http.MethodPost, "/disable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})
			},
			wantEC2:     "DISABLED",
			wantECR:     "ENABLED",
			wantLambda:  "DISABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_all_then_disable_all_is_disabled",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})
				auditDo(t, h, http.MethodPost, "/disable", map[string]any{})
			},
			wantEC2:     "DISABLED",
			wantECR:     "DISABLED",
			wantLambda:  "DISABLED",
			wantOverall: "DISABLED",
		},
		{
			name: "enable_empty_body_enables_all",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})
			},
			wantEC2:     "ENABLED",
			wantECR:     "ENABLED",
			wantLambda:  "ENABLED",
			wantOverall: "ENABLED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			tc.setup(t, h)

			rec := auditDo(t, h, http.MethodPost, "/status/batch/get", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			accounts := resp["accounts"].([]any)
			require.Len(t, accounts, 1)

			acc := accounts[0].(map[string]any)
			state := acc["state"].(map[string]any)
			status := state["status"].(map[string]any)
			resourceStatus := acc["resourceStatus"].(map[string]any)

			assert.Equal(t, tc.wantOverall, status["status"], "overall status")
			assert.Equal(t, tc.wantEC2, resourceStatus["ec2"], "ec2 status")
			assert.Equal(t, tc.wantECR, resourceStatus["ecr"], "ecr status")
			assert.Equal(t, tc.wantLambda, resourceStatus["lambda"], "lambda status")
		})
	}
}
