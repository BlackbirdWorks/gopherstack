package elasticbeanstalk_test

import (
	"encoding/xml"
	"fmt"
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
			name:   "non-EB action",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2010-12-01&Action=VerifyEmailIdentity",
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
		{
			name:   "wrong API version (SNS-style) rejected even for shared action name",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2010-03-31&Action=ListTagsForResource&ResourceArn=arn:aws:sns:us-east-1:123:my-topic",
			want:   false,
		},
		{
			name:   "wrong API version (CloudWatch-style) rejected even for shared action name",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2010-08-01&Action=ListTagsForResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123:alarm:my-alarm",
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
	rec1 := postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app-a&EnvironmentName=env-1")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app-a&EnvironmentName=env-2")

	// Extract env-1's EnvironmentId from the create response using XML parsing.
	var createResult struct {
		CreateEnvironmentResult struct {
			EnvironmentID string `xml:"EnvironmentId"`
		} `xml:"CreateEnvironmentResult"`
	}

	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &createResult))
	env1ID := createResult.CreateEnvironmentResult.EnvironmentID
	require.NotEmpty(t, env1ID, "env-1 EnvironmentId should not be empty")

	tests := []struct {
		name        string
		body        string
		wantEnv     string
		wantContain string
		wantStatus  int
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
		{
			name:        "filter by env id",
			body:        "Version=2010-12-01&Action=DescribeEnvironments&EnvironmentIds.member.1=" + env1ID,
			wantStatus:  http.StatusOK,
			wantEnv:     "env-1",
			wantContain: "<Tier>",
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

			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
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

func TestElasticBeanstalk_Handler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createApps int
		wantAfter  int
	}{
		{
			name:       "reset clears all applications",
			createApps: 2,
			wantAfter:  0,
		},
		{
			name:       "reset on empty backend is a no-op",
			createApps: 0,
			wantAfter:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.createApps {
				rec := postEBForm(t, h, fmt.Sprintf(
					"Action=CreateApplication&ApplicationName=app-%d&Version=2010-12-01", i,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			rec := postEBForm(t, h, "Action=DescribeApplications&Version=2010-12-01")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName                    xml.Name `xml:"DescribeApplicationsResponse"`
				DescribeApplicationsResult struct {
					Applications struct {
						Members []struct {
							ApplicationName string `xml:"ApplicationName"`
						} `xml:"member"`
					} `xml:"Applications"`
				} `xml:"DescribeApplicationsResult"`
			}

			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.DescribeApplicationsResult.Applications.Members, tt.wantAfter)
		})
	}
}

func TestHandler_AbortEnvironmentUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success no-op",
			body:       "Version=2010-12-01&Action=AbortEnvironmentUpdate&EnvironmentName=my-env",
			wantStatus: http.StatusOK,
			wantXML:    "AbortEnvironmentUpdateResponse",
		},
		{
			name:       "no env name still ok",
			body:       "Version=2010-12-01&Action=AbortEnvironmentUpdate",
			wantStatus: http.StatusOK,
			wantXML:    "AbortEnvironmentUpdateResponse",
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

func TestHandler_ApplyEnvironmentManagedAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       "Version=2010-12-01&Action=ApplyEnvironmentManagedAction&EnvironmentName=my-env&ActionId=action-1",
			wantStatus: http.StatusOK,
			wantXML:    "ApplyEnvironmentManagedActionResponse",
		},
		{
			name:       "missing action id",
			body:       "Version=2010-12-01&Action=ApplyEnvironmentManagedAction&EnvironmentName=my-env",
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

func TestHandler_AssociateEnvironmentOperationsRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		setupEnv   bool
	}{
		{
			name:     "success",
			setupEnv: true,
			body: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&EnvironmentName=my-env&OperationsRole=arn:aws:iam::123:role/ops",
			wantStatus: http.StatusOK,
		},
		{
			name: "missing environment name",
			body: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&OperationsRole=arn:aws:iam::123:role/ops",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing operations role",
			body:       "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole&EnvironmentName=my-env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "environment not found",
			body: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&EnvironmentName=nonexistent&OperationsRole=arn:aws:iam::123:role/ops",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupEnv {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
				)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CheckDNSAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cnamePrefix string
		wantStatus  int
		setupEnv    bool
		wantAvail   bool
	}{
		{
			name:        "available prefix",
			cnamePrefix: "free-prefix",
			wantStatus:  http.StatusOK,
			wantAvail:   true,
		},
		{
			name:        "taken prefix matches env name",
			cnamePrefix: "my-env",
			setupEnv:    true,
			wantStatus:  http.StatusOK,
			wantAvail:   false,
		},
		{
			name:       "missing cname prefix",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupEnv {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
				)
			}

			body := "Version=2010-12-01&Action=CheckDNSAvailability"
			if tt.cnamePrefix != "" {
				body += "&CNAMEPrefix=" + tt.cnamePrefix
			}

			rec := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					CheckDNSAvailabilityResult struct {
						FullyQualifiedCNAME string `xml:"FullyQualifiedCNAME"`
						Available           bool   `xml:"Available"`
					} `xml:"CheckDNSAvailabilityResult"`
				}

				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantAvail, out.CheckDNSAvailabilityResult.Available)
				assert.Contains(t, out.CheckDNSAvailabilityResult.FullyQualifiedCNAME, tt.cnamePrefix)
			}
		})
	}
}

func TestHandler_ComposeEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		appName      string
		wantXML      string
		wantStatus   int
		wantMinCount int
		setupEnvs    bool
	}{
		{
			name:       "success with no envs",
			appName:    "my-app",
			wantStatus: http.StatusOK,
			wantXML:    "ComposeEnvironmentsResponse",
		},
		{
			name:         "success returns existing envs",
			appName:      "my-app",
			setupEnvs:    true,
			wantStatus:   http.StatusOK,
			wantMinCount: 1,
		},
		{
			name:       "missing application name",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupEnvs {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=composed-env",
				)
			}

			body := "Version=2010-12-01&Action=ComposeEnvironments"
			if tt.appName != "" {
				body += "&ApplicationName=" + tt.appName
			}

			rec := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_CreateConfigurationTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(*elasticbeanstalk.Handler)
		name            string
		body            string
		wantXML         string
		wantStatus      int
		createDuplicate bool
	}{
		{
			name: "success",
			body: "Version=2010-12-01&Action=CreateConfigurationTemplate" +
				"&ApplicationName=my-app&TemplateName=my-tmpl&SolutionStackName=64bit+Amazon+Linux",
			wantStatus: http.StatusOK,
			wantXML:    "CreateConfigurationTemplateResponse",
		},
		{
			name:       "missing application name",
			body:       "Version=2010-12-01&Action=CreateConfigurationTemplate&TemplateName=my-tmpl",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing template name",
			body:       "Version=2010-12-01&Action=CreateConfigurationTemplate&ApplicationName=my-app",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:            "duplicate template",
			createDuplicate: true,
			body: "Version=2010-12-01&Action=CreateConfigurationTemplate" +
				"&ApplicationName=my-app&TemplateName=dup-tmpl",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.createDuplicate {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateConfigurationTemplate&ApplicationName=my-app&TemplateName=dup-tmpl",
				)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_CreatePlatformVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       "Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0.0",
			wantStatus: http.StatusOK,
			wantXML:    "CreatePlatformVersionResponse",
		},
		{
			name:       "missing platform name",
			body:       "Version=2010-12-01&Action=CreatePlatformVersion&PlatformVersion=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing platform version",
			body:       "Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform",
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

func TestHandler_CreateStorageLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success returns bucket name",
			wantStatus: http.StatusOK,
			wantXML:    "CreateStorageLocationResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, "Version=2010-12-01&Action=CreateStorageLocation")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)

				var out struct {
					CreateStorageLocationResult struct {
						S3Bucket string `xml:"S3Bucket"`
					} `xml:"CreateStorageLocationResult"`
				}

				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.CreateStorageLocationResult.S3Bucket)
				assert.Contains(t, out.CreateStorageLocationResult.S3Bucket, "elasticbeanstalk")
			}
		})
	}
}

func TestHandler_DeleteConfigurationTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		setup      bool
	}{
		{
			name:       "success",
			setup:      true,
			body:       "Version=2010-12-01&Action=DeleteConfigurationTemplate&ApplicationName=my-app&TemplateName=my-tmpl",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			body:       "Version=2010-12-01&Action=DeleteConfigurationTemplate&TemplateName=my-tmpl",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing template name",
			body:       "Version=2010-12-01&Action=DeleteConfigurationTemplate&ApplicationName=my-app",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found",
			body:       "Version=2010-12-01&Action=DeleteConfigurationTemplate&ApplicationName=my-app&TemplateName=nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setup {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateConfigurationTemplate&ApplicationName=my-app&TemplateName=my-tmpl",
				)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteEnvironmentConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success no-op",
			body:       "Version=2010-12-01&Action=DeleteEnvironmentConfiguration&ApplicationName=my-app&EnvironmentName=my-env",
			wantStatus: http.StatusOK,
			wantXML:    "DeleteEnvironmentConfigurationResponse",
		},
		{
			name:       "missing application name",
			body:       "Version=2010-12-01&Action=DeleteEnvironmentConfiguration&EnvironmentName=my-env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing environment name",
			body:       "Version=2010-12-01&Action=DeleteEnvironmentConfiguration&ApplicationName=my-app",
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

func TestHandler_NewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AbortEnvironmentUpdate",
		"ApplyEnvironmentManagedAction",
		"AssociateEnvironmentOperationsRole",
		"CheckDNSAvailability",
		"ComposeEnvironments",
		"CreateConfigurationTemplate",
		"CreatePlatformVersion",
		"CreateStorageLocation",
		"DeleteConfigurationTemplate",
		"DeleteEnvironmentConfiguration",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "expected %s in GetSupportedOperations", op)
	}
}

func TestHandler_NewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a config template and platform version.
	createTmplBody := "Version=2010-12-01&Action=CreateConfigurationTemplate" +
		"&ApplicationName=my-app&TemplateName=my-tmpl&SolutionStackName=64bit+Amazon+Linux"
	rec1 := postEBForm(t, h, createTmplBody)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postEBForm(
		t,
		h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0.0",
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Snapshot and restore.
	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := newTestHandler()
	require.NoError(t, h2.Restore(snap))

	// Verify the config template can still be deleted (meaning it was restored).
	rec3 := postEBForm(
		t,
		h2,
		"Version=2010-12-01&Action=DeleteConfigurationTemplate&ApplicationName=my-app&TemplateName=my-tmpl",
	)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
		setup      bool
	}{
		{
			name:       "success",
			setup:      true,
			body:       "Version=2010-12-01&Action=UpdateApplication&ApplicationName=my-app&Description=updated",
			wantStatus: http.StatusOK,
			wantXML:    "UpdateApplicationResponse",
		},
		{
			name:       "missing application name",
			body:       "Version=2010-12-01&Action=UpdateApplication&Description=updated",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found",
			body:       "Version=2010-12-01&Action=UpdateApplication&ApplicationName=nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setup {
				postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=my-app")
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_UpdateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
		setup      bool
	}{
		{
			name:  "success",
			setup: true,
			body: "Version=2010-12-01&Action=UpdateEnvironment" +
				"&ApplicationName=my-app&EnvironmentName=my-env&Description=updated",
			wantStatus: http.StatusOK,
			wantXML:    "UpdateEnvironmentResponse",
		},
		{
			name:       "missing environment name",
			body:       "Version=2010-12-01&Action=UpdateEnvironment&ApplicationName=my-app",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found",
			body:       "Version=2010-12-01&Action=UpdateEnvironment&EnvironmentName=nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setup {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
				)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_DescribeApplicationVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*elasticbeanstalk.Handler)
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name: "list all",
			setup: func(h *elasticbeanstalk.Handler) {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v1",
				)
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v2",
				)
			},
			body:       "Version=2010-12-01&Action=DescribeApplicationVersions",
			wantStatus: http.StatusOK,
			wantXML:    "DescribeApplicationVersionsResponse",
		},
		{
			name: "filter by app",
			setup: func(h *elasticbeanstalk.Handler) {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v1",
				)
			},
			body:       "Version=2010-12-01&Action=DescribeApplicationVersions&ApplicationName=my-app",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_DeleteApplicationVersionOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		setup      bool
	}{
		{
			name:       "success",
			setup:      true,
			body:       "Version=2010-12-01&Action=DeleteApplicationVersion&ApplicationName=my-app&VersionLabel=v1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       "Version=2010-12-01&Action=DeleteApplicationVersion&ApplicationName=my-app&VersionLabel=nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setup {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v1",
				)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateTagsForResource(t *testing.T) {
	t.Parallel()

	// createAppAndGetARN creates a tagged application and returns its ARN.
	createAppAndGetARN := func(h *elasticbeanstalk.Handler) string {
		createBody := "Version=2010-12-01&Action=CreateApplication" +
			"&ApplicationName=tag-app&Tags.member.1.Key=k1&Tags.member.1.Value=v1"
		rec := postEBForm(t, h, createBody)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			CreateApplicationResult struct {
				Application struct {
					ApplicationArn string `xml:"ApplicationArn"`
				} `xml:"Application"`
			} `xml:"CreateApplicationResult"`
		}

		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

		return resp.CreateApplicationResult.Application.ApplicationArn
	}

	tests := []struct {
		setup      func(*elasticbeanstalk.Handler) string
		name       string
		body       string
		wantStatus int
	}{
		{
			name:  "add tags",
			setup: createAppAndGetARN,
			body: "Version=2010-12-01&Action=UpdateTagsForResource" +
				"&TagsToAdd.member.1.Key=k2&TagsToAdd.member.1.Value=v2",
			wantStatus: http.StatusOK,
		},
		{
			name:       "remove tags",
			setup:      createAppAndGetARN,
			body:       "Version=2010-12-01&Action=UpdateTagsForResource&TagsToRemove.member.1=k1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing resource arn",
			body:       "Version=2010-12-01&Action=UpdateTagsForResource",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := tt.body
			if tt.setup != nil {
				arn := tt.setup(h)
				body = body + "&ResourceArn=" + arn
			}

			rec := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "returns empty events list",
			body:       "Version=2010-12-01&Action=DescribeEvents&ApplicationName=my-app",
			wantStatus: http.StatusOK,
			wantXML:    "DescribeEventsResponse",
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

func TestHandler_DescribeEnvironmentResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantXML     string
		wantContain string
		wantStatus  int
	}{
		{
			name:        "returns resources for environment",
			body:        "Version=2010-12-01&Action=DescribeEnvironmentResources&EnvironmentName=my-env",
			wantStatus:  http.StatusOK,
			wantXML:     "DescribeEnvironmentResourcesResponse",
			wantContain: "my-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(
				t,
				h,
				"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
			)
			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}

			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

func TestHandler_DescribeConfigurationSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*elasticbeanstalk.Handler)
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name: "existing environment",
			setup: func(h *elasticbeanstalk.Handler) {
				createEnvBody := "Version=2010-12-01&Action=CreateEnvironment" +
					"&ApplicationName=my-app&EnvironmentName=my-env&SolutionStackName=64bit+Amazon+Linux"
				postEBForm(t, h, createEnvBody)
			},
			body: "Version=2010-12-01&Action=DescribeConfigurationSettings" +
				"&ApplicationName=my-app&EnvironmentName=my-env",
			wantStatus: http.StatusOK,
			wantXML:    "DescribeConfigurationSettingsResponse",
		},
		{
			name: "nonexistent environment returns empty",
			body: "Version=2010-12-01&Action=DescribeConfigurationSettings" +
				"&ApplicationName=my-app&EnvironmentName=nonexistent",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

func TestHandler_RestartAppServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success no-op",
			body:       "Version=2010-12-01&Action=RestartAppServer&EnvironmentName=my-env",
			wantStatus: http.StatusOK,
			wantXML:    "RestartAppServerResponse",
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

func TestHandler_RebuildEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success no-op",
			body:       "Version=2010-12-01&Action=RebuildEnvironment&EnvironmentName=my-env",
			wantStatus: http.StatusOK,
			wantXML:    "RebuildEnvironmentResponse",
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
