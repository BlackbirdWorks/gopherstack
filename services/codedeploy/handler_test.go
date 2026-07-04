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
				_, err = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
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
				_, _ = createDG(h.Backend, "my-app", "my-dg", "arn:aws:iam::123:role/role", "", nil)
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
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
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
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d, _ := createDeploy(h.Backend, "my-app", "my-dg", "test", "user")

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
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
	_, _ = createDeploy(h.Backend, "my-app", "my-dg", "", "")

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
				_, _ = createDG(h.Backend, "dup-app", "dup-dg", "", "", nil)
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

	_, err = createDG(b, "my-app", "dg1", "", "", nil)
	require.NoError(t, err)

	_, err = createDG(b, "my-app", "dg2", "", "", nil)
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

func TestHandler_AddTagsToOnPremisesInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			input: map[string]any{
				"instanceNames": []string{"instance-1", "instance-2"},
				"tags": []map[string]string{
					{"Key": "env", "Value": "prod"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_instance_names",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "AddTagsToOnPremisesInstances", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGetApplicationRevisions(t *testing.T) {
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
			input: map[string]any{
				"applicationName": "my-app",
				"revisions": []map[string]any{
					{"revisionType": "S3"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_application_name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app_not_found",
			input: map[string]any{
				"applicationName": "nonexistent",
				"revisions":       []map[string]any{},
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

			rec := doRequest(t, h, "BatchGetApplicationRevisions", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-app", resp["applicationName"])
			}
		})
	}
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

func TestHandler_BatchGetDeploymentGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "dg1", "", "", nil)
				_, _ = createDG(h.Backend, "my-app", "dg2", "", "", nil)
			},
			input: map[string]any{
				"applicationName":      "my-app",
				"deploymentGroupNames": []string{"dg1", "dg2", "missing"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "missing_app_name",
			input:      map[string]any{"deploymentGroupNames": []string{"dg1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "app_not_found",
			input:      map[string]any{"applicationName": "no-such-app", "deploymentGroupNames": []string{"dg1"}},
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

			rec := doRequest(t, h, "BatchGetDeploymentGroups", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				infos, ok := resp["deploymentGroupsInfo"].([]any)
				require.True(t, ok)
				assert.Len(t, infos, tt.wantCount)
			}
		})
	}
}

func TestHandler_BatchGetDeploymentInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) string
		input      func(deployID string) map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

				return d.DeploymentID
			},
			input: func(deployID string) map[string]any {
				return map[string]any{
					"deploymentId": deployID,
					"instanceIds":  []string{"i-abc123", "i-def456"},
				}
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:  "missing_deployment_id",
			setup: func(_ *codedeploy.Handler) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{"instanceIds": []string{"i-abc"}}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deployID := tt.setup(h)

			rec := doRequest(t, h, "BatchGetDeploymentInstances", tt.input(deployID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				summaries, ok := resp["instancesSummary"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, tt.wantCount)
			}
		})
	}
}

func TestHandler_BatchGetDeploymentTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) string
		input      func(deployID string) map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

				return d.DeploymentID
			},
			input: func(deployID string) map[string]any {
				return map[string]any{
					"deploymentId": deployID,
					"targetIds":    []string{"target-1", "target-2"},
				}
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:  "missing_deployment_id",
			setup: func(_ *codedeploy.Handler) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{"targetIds": []string{"t-1"}}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "deployment_not_found",
			setup: func(_ *codedeploy.Handler) string { return "d-nonexistent" },
			input: func(deployID string) map[string]any {
				return map[string]any{"deploymentId": deployID, "targetIds": []string{"t-1"}}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deployID := tt.setup(h)

			rec := doRequest(t, h, "BatchGetDeploymentTargets", tt.input(deployID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				targets, ok := resp["deploymentTargets"].([]any)
				require.True(t, ok)
				assert.Len(t, targets, tt.wantCount)
			}
		})
	}
}

func TestHandler_BatchGetDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) []string
		input      func(ids []string) map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "two_found",
			setup: func(h *codedeploy.Handler) []string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d1, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")
				d2, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

				return []string{d1.DeploymentID, d2.DeploymentID, "d-notexist"}
			},
			input: func(ids []string) map[string]any {
				return map[string]any{"deploymentIds": ids}
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:  "missing_ids",
			setup: func(_ *codedeploy.Handler) []string { return nil },
			input: func(_ []string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ids := tt.setup(h)

			rec := doRequest(t, h, "BatchGetDeployments", tt.input(ids))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				infos, ok := resp["deploymentsInfo"].([]any)
				require.True(t, ok)
				assert.Len(t, infos, tt.wantCount)
			}
		})
	}
}

func TestHandler_BatchGetOnPremisesInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				kv := map[string]string{"env": "test"}
				err := h.Backend.AddTagsToOnPremisesInstances([]string{"inst-1", "inst-2"}, kv)
				require.NoError(t, err)
			},
			input:      map[string]any{"instanceNames": []string{"inst-1", "inst-2", "missing"}},
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

			rec := doRequest(t, h, "BatchGetOnPremisesInstances", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				infos, ok := resp["instanceInfos"].([]any)
				require.True(t, ok)
				assert.Len(t, infos, tt.wantCount)
			}
		})
	}
}

func TestHandler_ContinueDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) string
		input      func(deployID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

				return d.DeploymentID
			},
			input: func(deployID string) map[string]any {
				return map[string]any{
					"deploymentId":       deployID,
					"deploymentWaitType": "READY_WAIT",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing_deployment_id",
			setup: func(_ *codedeploy.Handler) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "deployment_not_found",
			setup: func(_ *codedeploy.Handler) string { return "d-notexist" },
			input: func(deployID string) map[string]any {
				return map[string]any{"deploymentId": deployID}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deployID := tt.setup(h)

			rec := doRequest(t, h, "ContinueDeployment", tt.input(deployID))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateDeploymentConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			input: map[string]any{
				"deploymentConfigName": "my-config",
				"computePlatform":      "Server",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name:       "missing_name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			input: map[string]any{
				"deploymentConfigName": "dup-config",
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate" {
				rec := doRequest(t, h, "CreateDeploymentConfig", map[string]any{"deploymentConfigName": "dup-config"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateDeploymentConfig", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["deploymentConfigId"])
			}
		})
	}
}

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"AddTagsToOnPremisesInstances",
		"BatchGetApplicationRevisions",
		"BatchGetApplications",
		"BatchGetDeploymentGroups",
		"BatchGetDeploymentInstances",
		"BatchGetDeploymentTargets",
		"BatchGetDeployments",
		"BatchGetOnPremisesInstances",
		"ContinueDeployment",
		"CreateDeploymentConfig",
	} {
		assert.Contains(t, ops, op)
	}
}

// --- Refinement check 1 tests ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("app-1", "Server", nil)
	_, _ = h.Backend.CreateApplication("app-2", "Lambda", nil)

	require.Equal(t, 2, h.Backend.ApplicationCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ApplicationCount())
	assert.Equal(t, 0, h.Backend.DeploymentCount())
	// 9 CodeDeployDefault.* configs are re-seeded on every Reset/NewInMemoryBackend.
	assert.Equal(t, 9, h.Backend.DeploymentConfigCount())
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codedeploy.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	require.ErrorIs(t, err, codedeploy.ErrNilAppContext)
}

func TestRefinement1_SortedListApplications(t *testing.T) {
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

func TestRefinement1_SortedListDeploymentGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "z-group", "", "", nil)
	_, _ = createDG(h.Backend, "my-app", "a-group", "", "", nil)
	_, _ = createDG(h.Backend, "my-app", "m-group", "", "", nil)

	rec := doRequest(t, h, "ListDeploymentGroups", map[string]any{"applicationName": "my-app"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		DeploymentGroups []string `json:"deploymentGroups"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"a-group", "m-group", "z-group"}, resp.DeploymentGroups)
}

func TestRefinement1_SortedListDeployments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
	d1, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")
	d2, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

	rec := doRequest(t, h, "ListDeployments", map[string]any{"applicationName": "my-app"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Deployments []string `json:"deployments"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Deployments, 2)

	ids := []string{d1.DeploymentID, d2.DeploymentID}
	_ = ids // verify both exist
	assert.Contains(t, resp.Deployments, d1.DeploymentID)
	assert.Contains(t, resp.Deployments, d2.DeploymentID)

	// Verify the output is sorted
	assert.LessOrEqual(t, resp.Deployments[0], resp.Deployments[1], "deployments should be sorted")
}

func TestRefinement1_SortedTagsInListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", map[string]string{
		"zzz": "last",
		"aaa": "first",
		"mmm": "middle",
	})

	arn := h.Backend.ApplicationARN("my-app")
	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"tags"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tags, 3)
	assert.Equal(t, "aaa", resp.Tags[0].Key)
	assert.Equal(t, "mmm", resp.Tags[1].Key)
	assert.Equal(t, "zzz", resp.Tags[2].Key)
}

func TestRefinement1_TagsOnDeploymentGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)

	dgArn := h.Backend.DeploymentGroupARN("my-app", "my-dg")

	// Tag the deployment group via TagResource
	rec := doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": dgArn,
		"tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Read the tags back
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": dgArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"tags"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tags, 1)
	assert.Equal(t, "env", resp.Tags[0].Key)
	assert.Equal(t, "prod", resp.Tags[0].Value)
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddApplicationInternal(&codedeploy.Application{
		ApplicationName: "seeded-app",
		ComputePlatform: "Lambda",
	})

	h.Backend.AddDeploymentGroupInternal(&codedeploy.DeploymentGroup{
		ApplicationName:     "seeded-app",
		DeploymentGroupName: "seeded-dg",
	})

	h.Backend.AddDeploymentInternal(&codedeploy.Deployment{
		ApplicationName:     "seeded-app",
		DeploymentGroupName: "seeded-dg",
		Status:              "InProgress",
	})

	h.Backend.AddOnPremisesInstanceInternal(&codedeploy.OnPremisesInstance{
		InstanceName: "my-server",
	})

	h.Backend.AddDeploymentConfigInternal(&codedeploy.DeploymentConfig{
		DeploymentConfigName: "my-config",
		ComputePlatform:      "ECS",
	})

	assert.Equal(t, 1, h.Backend.ApplicationCount())
	assert.Equal(t, 1, h.Backend.DeploymentGroupCount("seeded-app"))
	assert.Equal(t, 1, h.Backend.DeploymentCount())
	assert.Equal(t, 1, h.Backend.OnPremisesInstanceCount())
	// 9 CodeDeployDefault.* pre-seeded configs + 1 added here.
	assert.Equal(t, 10, h.Backend.DeploymentConfigCount())
}

func TestRefinement1_DeploymentConfigARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	cfgARN := h.Backend.DeploymentConfigARN("my-config")

	assert.Contains(t, cfgARN, "deploymentconfig:my-config")
}

func TestRefinement1_DeploymentGroupARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dgARN := h.Backend.DeploymentGroupARN("my-app", "my-dg")

	assert.Contains(t, dgARN, "deploymentgroup:my-app/my-dg")
}

func TestRefinement1_ComputePlatformValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		platform    string
		wantStatus  int
		wantSuccess bool
	}{
		{name: "server", platform: "Server", wantStatus: http.StatusOK, wantSuccess: true},
		{name: "lambda", platform: "Lambda", wantStatus: http.StatusOK, wantSuccess: true},
		{name: "ecs", platform: "ECS", wantStatus: http.StatusOK, wantSuccess: true},
		{name: "invalid", platform: "Docker", wantStatus: http.StatusBadRequest, wantSuccess: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDeploymentConfig", map[string]any{
				"deploymentConfigName": "cfg-" + tt.name,
				"computePlatform":      tt.platform,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement1_GetDeploymentGroupRequiresAppName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetDeploymentGroup", map[string]any{
		"deploymentGroupName": "my-dg",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_BatchGetApplicationRevisions_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)

	// Build 26 revisions (> maxBatchRevisions=25)
	revisions := make([]map[string]string, 26)
	for i := range revisions {
		revisions[i] = map[string]string{"revisionType": "S3"}
	}

	rec := doRequest(t, h, "BatchGetApplicationRevisions", map[string]any{
		"applicationName": "my-app",
		"revisions":       revisions,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_DeploymentInfoIncludesConfigName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "CodeDeployDefault.AllAtOnce", nil)
	d, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

	rec := doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": d.DeploymentID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	info := resp["deploymentInfo"]
	assert.Equal(t, "CodeDeployDefault.AllAtOnce", info["deploymentConfigName"])
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("persisted-app", "Server", nil)
	_, _ = createDG(h.Backend, "persisted-app", "persisted-dg", "", "", nil)
	d, _ := createDeploy(h.Backend, "persisted-app", "persisted-dg", "", "")
	_, _ = createCfg(h.Backend, "persisted-cfg", "Lambda")

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.ApplicationCount())
	assert.Equal(t, 1, h2.Backend.DeploymentGroupCount("persisted-app"))
	assert.Equal(t, 1, h2.Backend.DeploymentCount())
	// 9 CodeDeployDefault.* pre-seeded configs + 1 custom "persisted-cfg".
	assert.Equal(t, 10, h2.Backend.DeploymentConfigCount())

	// Verify deployment is readable
	rec := doRequest(t, h2, "GetDeployment", map[string]any{"deploymentId": d.DeploymentID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Invalid compute platform triggers ErrValidation → 400
	rec := doRequest(t, h, "CreateDeploymentConfig", map[string]any{
		"deploymentConfigName": "bad-cfg",
		"computePlatform":      "InvalidPlatform",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterValueException", resp["__type"])
}

func TestRefinement1_OnPremisesInstanceTagsAlwaysSlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Add instance with no tags
	h.Backend.AddOnPremisesInstanceInternal(&codedeploy.OnPremisesInstance{
		InstanceName: "notag-server",
	})

	rec := doRequest(t, h, "BatchGetOnPremisesInstances", map[string]any{
		"instanceNames": []string{"notag-server"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		InstanceInfos []struct {
			Tags []map[string]string `json:"tags"`
		} `json:"instanceInfos"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.InstanceInfos, 1)
	// Tags should be an empty slice, not nil/missing
	assert.NotNil(t, resp.InstanceInfos[0].Tags)
	assert.Empty(t, resp.InstanceInfos[0].Tags)
}

func TestRefinement1_BatchGetDeploymentGroups_EmptySlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)

	rec := doRequest(t, h, "BatchGetDeploymentGroups", map[string]any{
		"applicationName":      "my-app",
		"deploymentGroupNames": []string{"nonexistent"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		DeploymentGroupsInfo []any `json:"deploymentGroupsInfo"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// Should be an empty array not null
	assert.NotNil(t, resp.DeploymentGroupsInfo)
	assert.Empty(t, resp.DeploymentGroupsInfo)
}

func TestRefinement1_UntagDeploymentGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", map[string]string{"env": "test", "team": "eng"})

	dgARN := h.Backend.DeploymentGroupARN("my-app", "my-dg")

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": dgARN,
		"tagKeys":     []string{"team"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	kv, err := h.Backend.ListTagsForResource(dgARN)
	require.NoError(t, err)
	assert.NotContains(t, kv, "team")
	assert.Contains(t, kv, "env")
}
