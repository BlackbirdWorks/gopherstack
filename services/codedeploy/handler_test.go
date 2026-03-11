package codedeploy_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func newTestHandler(t *testing.T) *codedeploy.Handler {
	t.Helper()

	return codedeploy.NewHandler(codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
}

func doRequest(t *testing.T, h *codedeploy.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CodeDeploy_20141006."+action)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodeDeploy", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "codedeploy", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"DeleteApplication",
		"CreateDeploymentGroup",
		"GetDeploymentGroup",
		"ListDeploymentGroups",
		"DeleteDeploymentGroup",
		"CreateDeployment",
		"GetDeployment",
		"ListDeployments",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "codedeploy_target",
			target:    "CodeDeploy_20141006.CreateApplication",
			wantMatch: true,
		},
		{
			name:      "other_target",
			target:    "CodeCommit_20150413.CreateRepository",
			wantMatch: false,
		},
		{
			name:      "empty_target",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
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

func TestHandler_CreateDeploymentGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		setup      func(h *codedeploy.Handler)
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, err := h.Backend.CreateApplication("my-app", "Server", nil)
				if err != nil {
					panic(err)
				}
			},
			input: map[string]any{
				"applicationName":     "my-app",
				"deploymentGroupName": "my-dg",
				"serviceRoleArn":      "arn:aws:iam::123:role/my-role",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "app_not_found",
			input: map[string]any{
				"applicationName":     "nonexistent-app",
				"deploymentGroupName": "my-dg",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_fields",
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

			rec := doRequest(t, h, "CreateDeploymentGroup", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["deploymentGroupId"])
			}
		})
	}
}

func TestHandler_CreateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		setup      func(h *codedeploy.Handler)
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, err := h.Backend.CreateApplication("my-app", "Server", nil)
				if err != nil {
					panic(err)
				}
				_, err = h.Backend.CreateDeploymentGroup("my-app", "my-dg", "", "", nil)
				if err != nil {
					panic(err)
				}
			},
			input: map[string]any{
				"applicationName":     "my-app",
				"deploymentGroupName": "my-dg",
				"description":         "Test deployment",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "app_not_found",
			input: map[string]any{
				"applicationName":     "nonexistent-app",
				"deploymentGroupName": "my-dg",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CreateDeployment", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["deploymentId"])
				assert.True(t, len(resp["deploymentId"]) > 2 && resp["deploymentId"][:2] == "d-")
			}
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Tagging(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app first.
	rec := doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "tagged-app"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get the ARN.
	app, err := h.Backend.GetApplication("tagged-app")
	require.NoError(t, err)
	appARN := h.Backend.ApplicationARN(app.ApplicationName)

	// Tag the resource.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": appARN,
		"tags": []map[string]string{
			{"Key": "env", "Value": "test"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": appARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Untag.
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": appARN,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	bodyBytes, _ := json.Marshal(map[string]string{"applicationName": "my-app"})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	resource := h.ExtractResource(c)
	assert.Equal(t, "my-app", resource)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "CodeDeploy_20141006.CreateApplication")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	operation := h.ExtractOperation(c)
	assert.Equal(t, "CreateApplication", operation)
}

func TestHandler_ExtractOperation_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	operation := h.ExtractOperation(c)
	assert.Equal(t, "Unknown", operation)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.NotEmpty(t, ops)
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, config.DefaultRegion, regions[0])
}

func TestHandler_GetDeploymentGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = h.Backend.CreateDeploymentGroup("my-app", "my-dg", "arn:aws:iam::123:role/role", "", nil)
			},
			input: map[string]any{
				"applicationName":     "my-app",
				"deploymentGroupName": "my-dg",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			input:      map[string]any{"applicationName": "missing", "deploymentGroupName": "dg"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetDeploymentGroup", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDeploymentGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
			},
			input:      map[string]any{"applicationName": "my-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "app_not_found",
			input:      map[string]any{"applicationName": "missing"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListDeploymentGroups", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteDeploymentGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = h.Backend.CreateDeploymentGroup("my-app", "my-dg", "", "", nil)
			},
			input:      map[string]any{"applicationName": "my-app", "deploymentGroupName": "my-dg"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			input:      map[string]any{"applicationName": "missing", "deploymentGroupName": "dg"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteDeploymentGroup", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) string
		input      func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = h.Backend.CreateDeploymentGroup("my-app", "my-dg", "", "", nil)
				d, _ := h.Backend.CreateDeployment("my-app", "my-dg", "test", "user")

				return d.DeploymentID
			},
			input: func(id string) map[string]any {
				return map[string]any{"deploymentId": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "not_found",
			setup: func(_ *codedeploy.Handler) string { return "d-nonexistent" },
			input: func(id string) map[string]any {
				return map[string]any{"deploymentId": id}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:  "empty_id",
			setup: func(_ *codedeploy.Handler) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deployID := tt.setup(h)

			rec := doRequest(t, h, "GetDeployment", tt.input(deployID))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDeployments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty list.
	rec := doRequest(t, h, "ListDeployments", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Create some deployments.
	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = h.Backend.CreateDeploymentGroup("my-app", "my-dg", "", "", nil)
	_, _ = h.Backend.CreateDeployment("my-app", "my-dg", "", "")

	rec = doRequest(t, h, "ListDeployments", map[string]any{
		"applicationName":     "my-app",
		"deploymentGroupName": "my-dg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	deployments, ok := resp["deployments"].([]any)
	require.True(t, ok)
	assert.Len(t, deployments, 1)
}

func TestHandler_BackendErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "deployment_group_already_exists",
			action:     "CreateDeploymentGroup",
			input:      map[string]any{"applicationName": "dup-app", "deploymentGroupName": "dup-dg"},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "deployment_group_not_found_for_deployment",
			action:     "CreateDeployment",
			input:      map[string]any{"applicationName": "app-for-deploy", "deploymentGroupName": "missing-dg"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Pre-create for the "already exists" case.
			if tt.name == "deployment_group_already_exists" {
				_, _ = h.Backend.CreateApplication("dup-app", "Server", nil)
				_, _ = h.Backend.CreateDeploymentGroup("dup-app", "dup-dg", "", "", nil)
			}

			if tt.name == "deployment_group_not_found_for_deployment" {
				_, _ = h.Backend.CreateApplication("app-for-deploy", "Server", nil)
			}

			rec := doRequest(t, h, tt.action, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &codedeploy.Provider{}
	assert.Equal(t, "CodeDeploy", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &codedeploy.Provider{}
	ctx := &service.AppContext{}

	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "CodeDeploy", reg.Name())
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	assert.Equal(t, config.DefaultRegion, b.Region())
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

func TestBackend_ListDeploymentGroupDetails(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	_, err := b.CreateApplication("my-app", "Server", nil)
	require.NoError(t, err)

	_, err = b.CreateDeploymentGroup("my-app", "dg1", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateDeploymentGroup("my-app", "dg2", "", "", nil)
	require.NoError(t, err)

	dgs, err := b.ListDeploymentGroupDetails("my-app")
	require.NoError(t, err)
	assert.Len(t, dgs, 2)

	_, err = b.ListDeploymentGroupDetails("missing")
	require.Error(t, err)
}

func TestBackend_ApplicationARN(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	arn := b.ApplicationARN("my-app")
	assert.Contains(t, arn, "codedeploy")
	assert.Contains(t, arn, "my-app")
}
