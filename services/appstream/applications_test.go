package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_Applications covers Application CRUD and fleet associations.
func TestAppStream_Applications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateApplication returns application with ARN",
			action: "CreateApplication",
			body: map[string]any{
				"Name":       "my-app",
				"LaunchPath": "/app/my-app",
				"Platforms":  []string{"WINDOWS_SERVER_2019"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				app := resp["Application"].(map[string]any)
				assert.Equal(t, "my-app", app["Name"])
				assert.Contains(t, app["Arn"], ":application/my-app")
			},
		},
		{
			name:   "CreateApplication duplicate returns error",
			action: "CreateApplication",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "dup-app")
			},
			body:     map[string]any{"Name": "dup-app", "LaunchPath": "/x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeApplications lists all",
			action: "DescribeApplications",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "app-a")
				createApplication(t, h, "app-b")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				apps := resp["Applications"].([]any)
				assert.Len(t, apps, 2)
			},
		},
		{
			name:   "UpdateApplication changes display name",
			action: "UpdateApplication",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "upd-app")
			},
			body:     map[string]any{"Name": "upd-app", "DisplayName": "Updated App"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				app := resp["Application"].(map[string]any)
				assert.Equal(t, "Updated App", app["DisplayName"])
			},
		},
		{
			name:     "DeleteApplication removes application",
			action:   "DeleteApplication",
			setup:    func(h *appstream.Handler) { createApplication(t, h, "del-app") },
			body:     map[string]any{"Name": "del-app"},
			wantCode: http.StatusOK,
		},
		{
			// Real DescribeAppLicenseUsageOutput carries AppLicenseUsages
			// (plural) -- lock the wire field name, not just the status code.
			name:     "DescribeAppLicenseUsage returns empty AppLicenseUsages",
			action:   "DescribeAppLicenseUsage",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				usages, ok := resp["AppLicenseUsages"]
				require.True(t, ok, "response must carry AppLicenseUsages")
				assert.Empty(t, usages)
			},
		},
		{
			name:   "AssociateApplicationFleet creates link",
			action: "AssociateApplicationFleet",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "link-app")
				createFleet(t, h, "link-fleet")
			},
			body: map[string]any{
				"ApplicationArn": "link-app",
				"FleetName":      "link-fleet",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DisassociateApplicationFleet removes link",
			action: "DisassociateApplicationFleet",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "dis-app")
				createFleet(t, h, "dis-fleet")
				rec := doRequest(t, h, "AssociateApplicationFleet", map[string]any{
					"ApplicationArn": "dis-app",
					"FleetName":      "dis-fleet",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ApplicationArn": "dis-app",
				"FleetName":      "dis-fleet",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeApplicationFleetAssociations lists links",
			action: "DescribeApplicationFleetAssociations",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "lst-app")
				createFleet(t, h, "lst-fleet")
				rec := doRequest(t, h, "AssociateApplicationFleet", map[string]any{
					"ApplicationArn": "lst-app",
					"FleetName":      "lst-fleet",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"ApplicationArn": "lst-app"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assocs := resp["ApplicationFleetAssociations"].([]any)
				assert.Len(t, assocs, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_ApplicationARNFormat verifies application ARN format.
func TestAppStream_ApplicationARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name":       "arn-app",
		"LaunchPath": "/path/to/app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	app := resp["Application"].(map[string]any)
	assert.Contains(t, app["Arn"], "arn:aws:appstream:")
	assert.Contains(t, app["Arn"], "application/arn-app")
}
