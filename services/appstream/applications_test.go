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
				"IconS3Location": map[string]any{
					"S3Bucket": "icon-bucket",
					"S3Key":    "icons/my-app.png",
				},
				"InstanceFamilies": []string{"GENERAL_PURPOSE"},
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
			body: map[string]any{
				"Name":       "dup-app",
				"LaunchPath": "/x",
				"IconS3Location": map[string]any{
					"S3Bucket": "icon-bucket",
					"S3Key":    "icons/dup-app.png",
				},
				"InstanceFamilies": []string{"GENERAL_PURPOSE"},
			},
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

// TestAppStream_CreateApplication_RequiredMembersRejected proves
// IconS3Location and InstanceFamilies are enforced as required members
// (api_op_CreateApplication.go:47,53), including IconS3Location's own
// required S3Key leaf when used for CreateApplication
// (appstream@v1.64.5 types/types.go:1434-1451). Against unfixed code (which
// never reads either member) every case here gets 200, not 400.
func TestAppStream_CreateApplication_RequiredMembersRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing iconS3location",
			body: map[string]any{
				"Name":             "no-icon-app",
				"LaunchPath":       "/app/no-icon-app",
				"InstanceFamilies": []string{"GENERAL_PURPOSE"},
			},
		},
		{
			name: "iconS3location missing s3key",
			body: map[string]any{
				"Name":             "no-s3key-app",
				"LaunchPath":       "/app/no-s3key-app",
				"IconS3Location":   map[string]any{"S3Bucket": "icon-bucket"},
				"InstanceFamilies": []string{"GENERAL_PURPOSE"},
			},
		},
		{
			name: "missing instancefamilies",
			body: map[string]any{
				"Name":           "no-families-app",
				"LaunchPath":     "/app/no-families-app",
				"IconS3Location": map[string]any{"S3Bucket": "icon-bucket", "S3Key": "icons/x.png"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApplication", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestAppStream_CreateApplication_RequiredMembersRoundTrip proves
// IconS3Location and InstanceFamilies survive from Create through
// DescribeApplications, not just that Create returns 200 (a field parsed
// and discarded looks identical to one that works if only the status is
// checked).
func TestAppStream_CreateApplication_RequiredMembersRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name":       "roundtrip-app",
		"LaunchPath": "/app/roundtrip-app",
		"IconS3Location": map[string]any{
			"S3Bucket": "roundtrip-bucket",
			"S3Key":    "icons/roundtrip-app.png",
		},
		"InstanceFamilies": []string{"GENERAL_PURPOSE", "GRAPHICS_G4"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	app := createOut["Application"].(map[string]any)

	icon, ok := app["IconS3Location"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "roundtrip-bucket", icon["S3Bucket"])
	assert.Equal(t, "icons/roundtrip-app.png", icon["S3Key"])
	assert.ElementsMatch(t, []any{"GENERAL_PURPOSE", "GRAPHICS_G4"}, app["InstanceFamilies"])

	descRec := doRequest(t, h, "DescribeApplications", map[string]any{"Arns": []string{app["Arn"].(string)}})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	apps := descOut["Applications"].([]any)
	require.Len(t, apps, 1)
	got := apps[0].(map[string]any)

	gotIcon, ok := got["IconS3Location"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "roundtrip-bucket", gotIcon["S3Bucket"])
	assert.Equal(t, "icons/roundtrip-app.png", gotIcon["S3Key"])
	assert.ElementsMatch(t, []any{"GENERAL_PURPOSE", "GRAPHICS_G4"}, got["InstanceFamilies"])
}

// TestAppStream_ApplicationARNFormat verifies application ARN format.
func TestAppStream_ApplicationARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name":       "arn-app",
		"LaunchPath": "/path/to/app",
		"IconS3Location": map[string]any{
			"S3Bucket": "icon-bucket",
			"S3Key":    "icons/arn-app.png",
		},
		"InstanceFamilies": []string{"GENERAL_PURPOSE"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	app := resp["Application"].(map[string]any)
	assert.Contains(t, app["Arn"], "arn:aws:appstream:")
	assert.Contains(t, app["Arn"], "application/arn-app")
}
