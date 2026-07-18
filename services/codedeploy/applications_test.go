package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// createCDApp creates an application via the handler and requires success.
func createCDApp(t *testing.T, h *codedeploy.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"applicationName": name,
		"computePlatform": "Server",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantAppID  bool
	}{
		{
			name: "success",
			input: map[string]any{
				"applicationName": "my-app",
				"computePlatform": "Server",
			},
			wantStatus: http.StatusOK,
			wantAppID:  true,
		},
		{
			name:       "missing_name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			input: map[string]any{
				"applicationName": "dup-app",
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate" {
				rec := doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "dup-app"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantAppID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["applicationId"])
			}
		})
	}
}

func TestHandler_CreateApplication_ComputePlatformValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		platform    string
		wantErrType string
		wantStatus  int
	}{
		{name: "server_valid", platform: "Server", wantStatus: http.StatusOK},
		{name: "lambda_valid", platform: "Lambda", wantStatus: http.StatusOK},
		{name: "ecs_valid", platform: "ECS", wantStatus: http.StatusOK},
		{name: "empty_defaults_to_server", platform: "", wantStatus: http.StatusOK},
		{
			name:        "invalid_platform",
			platform:    "Docker",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidComputePlatformException",
		},
		{
			name:        "case_sensitive",
			platform:    "server",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidComputePlatformException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"applicationName": "app-" + tt.name,
				"computePlatform": tt.platform,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestHandler_GetApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:       "success",
			appName:    "my-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			appName:    "nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "empty_name",
			appName:    "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				doRequest(t, h, "CreateApplication", map[string]any{"applicationName": tt.appName})
			}

			rec := doRequest(t, h, "GetApplication", map[string]any{"applicationName": tt.appName})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListApplications", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	apps, ok := resp["applications"].([]any)
	require.True(t, ok)
	assert.Empty(t, apps)

	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "app1"})
	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "app2"})

	rec = doRequest(t, h, "ListApplications", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	apps, ok = resp["applications"].([]any)
	require.True(t, ok)
	assert.Len(t, apps, 2)
}

func TestApplications_SortedList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("zebra", "Server", nil)
	_, _ = h.Backend.CreateApplication("apple", "Server", nil)
	_, _ = h.Backend.CreateApplication("mango", "Server", nil)

	rec := doRequest(t, h, "ListApplications", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Applications []string `json:"applications"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"apple", "mango", "zebra"}, resp.Applications)
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:       "success",
			appName:    "my-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			appName:    "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				doRequest(t, h, "CreateApplication", map[string]any{"applicationName": tt.appName})
			}

			rec := doRequest(t, h, "DeleteApplication", map[string]any{"applicationName": tt.appName})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCDApp(t, h, "update-app")

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"applicationName":    "update-app",
		"newApplicationName": "update-app-v2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestApplications_UpdateApplication_UpdatesDeployments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("old-app", "Server", nil)
	_, _ = createDG(h.Backend, "old-app", "dg", "", "", nil)
	d, _ := createDeploy(h.Backend, "old-app", "dg", "", "")

	// Rename the application.
	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"applicationName":    "old-app",
		"newApplicationName": "new-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The deployment should now reference the new application name.
	got, err := h.Backend.GetDeployment(d.DeploymentID)
	require.NoError(t, err)
	assert.Equal(t, "new-app", got.ApplicationName)

	// Old application name should not exist.
	rec2 := doRequest(t, h, "GetApplication", map[string]any{"applicationName": "old-app"})
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// New application name should exist.
	rec3 := doRequest(t, h, "GetApplication", map[string]any{"applicationName": "new-app"})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_BatchGetApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "two_found",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("app-a", "Server", nil)
				_, _ = h.Backend.CreateApplication("app-b", "Lambda", nil)
			},
			input:      map[string]any{"applicationNames": []string{"app-a", "app-b", "missing"}},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "missing_names",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "BatchGetApplications", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				infos, ok := resp["applicationsInfo"].([]any)
				require.True(t, ok)
				assert.Len(t, infos, tt.wantCount)
			}
		})
	}
}

func TestBackend_ListApplicationDetails(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	_, err := b.CreateApplication("app1", "Server", nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("app2", "Lambda", nil)
	require.NoError(t, err)

	apps := b.ListApplicationDetails()
	assert.Len(t, apps, 2)
}

func TestBackend_ApplicationARN(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	arn := b.ApplicationARN("my-app")
	assert.Contains(t, arn, "codedeploy")
	assert.Contains(t, arn, "my-app")
}
