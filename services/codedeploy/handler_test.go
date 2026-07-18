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

// createAppAndDG is a convenience helper to set up an app and deployment group,
// used by tests across multiple op families.
func createAppAndDG(t *testing.T, h *codedeploy.Handler, appName, dgName string) {
	t.Helper()
	doRequest(t, h, "CreateApplication", map[string]any{
		"applicationName": appName,
		"computePlatform": "Server",
	})
	doRequest(t, h, "CreateDeploymentGroup", map[string]any{
		"applicationName":     appName,
		"deploymentGroupName": dgName,
		"serviceRoleArn":      "arn:aws:iam::000000000000:role/role",
	})
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

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestProvider_InitNilContext(t *testing.T) {
	t.Parallel()

	p := &codedeploy.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	require.ErrorIs(t, err, codedeploy.ErrNilAppContext)
}
