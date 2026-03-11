package elasticbeanstalk_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func newTestHandler() *elasticbeanstalk.Handler {
	return elasticbeanstalk.NewHandler(elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"))
}

func postEBForm(t *testing.T, h *elasticbeanstalk.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, "Elasticbeanstalk", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "DescribeApplications")
	assert.Contains(t, ops, "UpdateApplication")
	assert.Contains(t, ops, "DeleteApplication")
	assert.Contains(t, ops, "CreateEnvironment")
	assert.Contains(t, ops, "DescribeEnvironments")
	assert.Contains(t, ops, "UpdateEnvironment")
	assert.Contains(t, ops, "TerminateEnvironment")
	assert.Contains(t, ops, "CreateApplicationVersion")
	assert.Contains(t, ops, "DescribeApplicationVersions")
	assert.Contains(t, ops, "DeleteApplicationVersion")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "UpdateTagsForResource")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   bool
	}{
		{
			name:   "valid EB request",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2010-12-01&Action=DescribeApplications",
			want:   true,
		},
		{
			name:   "wrong version",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2011-01-01&Action=DescribeApplications",
			want:   false,
		},
		{
			name:   "GET method",
			method: http.MethodGet,
			path:   "/",
			body:   "Version=2010-12-01&Action=DescribeApplications",
			want:   false,
		},
		{
			name:   "dashboard path excluded",
			method: http.MethodPost,
			path:   "/dashboard/elasticbeanstalk",
			body:   "Version=2010-12-01&Action=DescribeApplications",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       "Version=2010-12-01&Action=CreateApplication&ApplicationName=my-app&Description=My+App",
			wantStatus: http.StatusOK,
			wantXML:    "CreateApplicationResponse",
		},
		{
			name:       "missing application name",
			body:       "Version=2010-12-01&Action=CreateApplication",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_DescribeApplications(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// Create two applications.
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=app-a")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=app-b")

	tests := []struct {
		name       string
		body       string
		wantApp    string
		wantStatus int
	}{
		{
			name:       "list all",
			body:       "Version=2010-12-01&Action=DescribeApplications",
			wantStatus: http.StatusOK,
		},
		{
			name:       "filter by name",
			body:       "Version=2010-12-01&Action=DescribeApplications&ApplicationNames.member.1=app-a",
			wantStatus: http.StatusOK,
			wantApp:    "app-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantApp != "" {
				assert.Contains(t, rec.Body.String(), tt.wantApp)
			}
		})
	}
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupName  string
		deleteName string
		wantStatus int
	}{
		{
			name:       "delete existing",
			setupName:  "del-app",
			deleteName: "del-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete nonexistent",
			deleteName: "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupName != "" {
				postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName="+tt.setupName)
			}

			rec := postEBForm(t, h, "Version=2010-12-01&Action=DeleteApplication&ApplicationName="+tt.deleteName)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
			wantStatus: http.StatusOK,
			wantXML:    "CreateEnvironmentResponse",
		},
		{
			name:       "missing environment name",
			body:       "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing application name",
			body:       "Version=2010-12-01&Action=CreateEnvironment&EnvironmentName=my-env",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_DescribeEnvironments(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app-a&EnvironmentName=env-1")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app-a&EnvironmentName=env-2")

	tests := []struct {
		name       string
		body       string
		wantEnv    string
		wantStatus int
	}{
		{
			name:       "list all",
			body:       "Version=2010-12-01&Action=DescribeEnvironments",
			wantStatus: http.StatusOK,
		},
		{
			name:       "filter by app",
			body:       "Version=2010-12-01&Action=DescribeEnvironments&ApplicationName=app-a",
			wantStatus: http.StatusOK,
			wantEnv:    "env-1",
		},
		{
			name:       "filter by env name",
			body:       "Version=2010-12-01&Action=DescribeEnvironments&EnvironmentNames.member.1=env-1",
			wantStatus: http.StatusOK,
			wantEnv:    "env-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantEnv != "" {
				assert.Contains(t, rec.Body.String(), tt.wantEnv)
			}
		})
	}
}

func TestHandler_TerminateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupApp   string
		setupEnv   string
		termEnv    string
		wantStatus int
	}{
		{
			name:       "terminate existing",
			setupApp:   "my-app",
			setupEnv:   "my-env",
			termEnv:    "my-env",
			wantStatus: http.StatusOK,
		},
		{
			name:       "terminate nonexistent",
			termEnv:    "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupApp != "" {
				postEBForm(t, h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName="+tt.setupApp+
						"&EnvironmentName="+tt.setupEnv)
			}

			rec := postEBForm(t, h,
				"Version=2010-12-01&Action=TerminateEnvironment&EnvironmentName="+tt.termEnv)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ApplicationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "create success",
			body:       "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v1",
			wantStatus: http.StatusOK,
			wantXML:    "CreateApplicationVersionResponse",
		},
		{
			name:       "create missing app name",
			body:       "Version=2010-12-01&Action=CreateApplicationVersion&VersionLabel=v1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create missing version label",
			body:       "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(
		t,
		h,
		"Version=2010-12-01&Action=CreateApplication&ApplicationName=tag-app&Tags.member.1.Key=env&Tags.member.1.Value=prod",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Parse application ARN from create response.
	var resp struct {
		CreateApplicationResult struct {
			Application struct {
				ApplicationArn string `xml:"ApplicationArn"`
			} `xml:"Application"`
		} `xml:"CreateApplicationResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	appARN := resp.CreateApplicationResult.Application.ApplicationArn

	tests := []struct {
		name        string
		resourceARN string
		wantTag     string
		wantStatus  int
	}{
		{
			name:        "list tags for existing",
			resourceARN: appARN,
			wantStatus:  http.StatusOK,
			wantTag:     "env",
		},
		{
			name:        "list tags for nonexistent",
			resourceARN: "arn:aws:elasticbeanstalk:us-east-1:123:nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:       "missing resource arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := "Version=2010-12-01&Action=ListTagsForResource"
			if tt.resourceARN != "" {
				body += "&ResourceArn=" + tt.resourceARN
			}

			rec2 := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			if tt.wantTag != "" {
				assert.Contains(t, rec2.Body.String(), tt.wantTag)
			}
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=UnknownAction")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_MissingAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_XMLResponseFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplications")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/xml", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "<?xml")
	assert.Contains(t, rec.Body.String(), "DescribeApplicationsResponse")
}
