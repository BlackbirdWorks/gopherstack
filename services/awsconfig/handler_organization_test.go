package awsconfig_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigHandler_DeleteOrganizationConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutOrganizationConfigRule("org-rule"))
			},
			body:     map[string]any{"OrganizationConfigRuleName": "org-rule"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"OrganizationConfigRuleName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteOrganizationConfigRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteOrganizationConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutOrganizationConformancePack("org-pack"))
			},
			body:     map[string]any{"OrganizationConformancePackName": "org-pack"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"OrganizationConformancePackName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteOrganizationConformancePack", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_GetOrganizationConfigRuleDetailedStatus(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	require.NoError(t, h.Backend.PutOrganizationConfigRule("org-rule"))

	rec := doAWSConfigRequest(t, h, "GetOrganizationConfigRuleDetailedStatus", map[string]any{
		"OrganizationConfigRuleName": "org-rule",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE_SUCCESSFUL")

	notFound := doAWSConfigRequest(t, h, "GetOrganizationConfigRuleDetailedStatus", map[string]any{
		"OrganizationConfigRuleName": "does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, notFound.Code)
}

func TestAWSConfigHandler_GetOrganizationConformancePackDetailedStatus(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	require.NoError(t, h.Backend.PutOrganizationConformancePack("org-pack"))

	rec := doAWSConfigRequest(t, h, "GetOrganizationConformancePackDetailedStatus", map[string]any{
		"OrganizationConformancePackName": "org-pack",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE_SUCCESSFUL")

	notFound := doAWSConfigRequest(t, h, "GetOrganizationConformancePackDetailedStatus", map[string]any{
		"OrganizationConformancePackName": "does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, notFound.Code)
}
