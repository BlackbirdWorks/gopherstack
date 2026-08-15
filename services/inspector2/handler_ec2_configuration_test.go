package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEc2DeepInspectionConfiguration(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Get/Update round-trip",
			steps: []step{
				{
					name:   "get initial config",
					method: http.MethodPost,
					path:   "/ec2deepinspectionconfiguration/get",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						_, ok := resp["status"]
						assert.True(t, ok)
					},
				},
				{
					name:   "update with paths",
					method: http.MethodPost,
					path:   "/ec2deepinspectionconfiguration/update",
					body:   map[string]any{"packagePaths": []string{"/opt/app", "/usr/local"}},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						paths, _ := resp["packagePaths"].([]any)
						assert.Len(t, paths, 2)
					},
				},
				{
					name:   "org update",
					method: http.MethodPost,
					path:   "/ec2deepinspectionconfiguration/org/update",
					body:   map[string]any{"orgPackagePaths": []string{"/shared"}},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
		{
			name: "Batch get/update member status",
			steps: []step{
				{
					name:   "batch get unknown accounts returns defaults",
					method: http.MethodPost,
					path:   "/ec2deepinspectionstatus/member/batch/get",
					body:   map[string]any{"accountIds": []string{"555555555555"}},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						members, _ := resp["accountIds"].([]any)
						assert.Len(t, members, 1)
					},
				},
				{
					name:   "batch update member status",
					method: http.MethodPost,
					path:   "/ec2deepinspectionstatus/member/batch/update",
					body: map[string]any{
						"accountIds": []any{
							map[string]any{
								"accountId":              "555555555555",
								"activateDeepInspection": true,
							},
						},
					},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						accounts, _ := resp["accountIds"].([]any)
						assert.Len(t, accounts, 1)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}
