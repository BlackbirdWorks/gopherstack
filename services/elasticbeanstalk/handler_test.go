package elasticbeanstalk_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// iso8601Re matches ISO 8601 UTC timestamps like 2026-06-26T09:12:26Z.
var iso8601Re = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`)

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

// indexOfFirst returns the position of the first occurrence of substr in s, or -1.
func indexOfFirst(s, substr string) int {
	return strings.Index(s, substr)
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

	wantOps := []string{
		"CreateApplication", "DescribeApplications", "UpdateApplication", "DeleteApplication",
		"CreateEnvironment", "DescribeEnvironments", "UpdateEnvironment", "TerminateEnvironment",
		"CreateApplicationVersion", "DescribeApplicationVersions", "DeleteApplicationVersion",
		"ListTagsForResource", "UpdateTagsForResource",
		"AbortEnvironmentUpdate", "ApplyEnvironmentManagedAction", "AssociateEnvironmentOperationsRole",
		"CheckDNSAvailability", "ComposeEnvironments", "CreateConfigurationTemplate",
		"CreatePlatformVersion", "CreateStorageLocation", "DeleteConfigurationTemplate",
		"DeleteEnvironmentConfiguration",
	}

	for _, op := range wantOps {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, op)
		})
	}
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

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=UnknownAction")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_CloneEnvironment_NotSupported locks that CloneEnvironment --
// a fabricated action that was never part of the real AWS Elastic
// Beanstalk API (no api_op_CloneEnvironment.go/deserializer exists in
// aws-sdk-go-v2/service/elasticbeanstalk) -- is not routed. Real SDK
// clients can never construct this request, so it must 400 like any other
// unrecognized action rather than be served by a gopherstack-invented
// handler; this guards against the fabricated op being reintroduced.
func TestHandler_CloneEnvironment_NotSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.NotContains(t, h.GetSupportedOperations(), "CloneEnvironment")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=CloneEnvironment"+
		"&SourceEnvironmentName=src&EnvironmentName=dst")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnknownOperationException")
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

func TestHandler_Reset(t *testing.T) {
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

// TestHandler_Reset_DelegatesToBackend verifies that Handler.Reset delegates to Backend.Reset.
func TestHandler_Reset_DelegatesToBackend(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=app1")
	assert.Equal(t, 1, h.Backend.ApplicationCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ApplicationCount())
}

// TestHandler_OpsTable_PreBuilt verifies that the handler dispatch table is pre-built.
func TestHandler_OpsTable_PreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	assert.Equal(t, len(ops), h.HandlerOpsLen(), "ops table should match supported operations count")
}

// TestHandler_CountHelpers_TrackResourceCreation verifies export count helpers via HTTP,
// across every resource collection.
func TestHandler_CountHelpers_TrackResourceCreation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, 0, h.Backend.ApplicationCount())

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=a1")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=a2")

	assert.Equal(t, 2, h.Backend.ApplicationCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=a1&EnvironmentName=env1")
	assert.Equal(t, 1, h.Backend.EnvironmentCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=a1&VersionLabel=v1")
	assert.Equal(t, 1, h.Backend.AppVersionCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate&ApplicationName=a1&TemplateName=tmpl1")
	// 3 = the "Default" template AWS auto-creates for each of a1 and a2
	// (CreateApplication: "creates an application that has one configuration
	// template named default") plus the explicit tmpl1 created above.
	assert.Equal(t, 3, h.Backend.ConfigTemplateCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")
	assert.Equal(t, 1, h.Backend.PlatformVersionCount())
}

// TestHandler_PersistenceRoundTrip verifies snapshot/restore preserves all state
// across every resource collection.
func TestHandler_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=persisted-app")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=persisted-app&EnvironmentName=persisted-env")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=persisted-app&VersionLabel=v1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=persisted-app&TemplateName=tmpl1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")

	snap := h.Backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := elasticbeanstalk.NewHandler(elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"))
	require.NoError(t, h2.Backend.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.ApplicationCount())
	assert.Equal(t, 1, h2.Backend.EnvironmentCount())
	assert.Equal(t, 1, h2.Backend.AppVersionCount())
	// 2 = the auto-created "Default" template plus the explicit tmpl1.
	assert.Equal(t, 2, h2.Backend.ConfigTemplateCount())
	assert.Equal(t, 1, h2.Backend.PlatformVersionCount())

	// Verify ARN indexes are rebuilt for tag lookup.
	rec := postEBForm(t, h2,
		"Version=2010-12-01&Action=DescribeApplications")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "persisted-app")
}

// TestHandler_CreateOps_DateCreatedIsRealTimestamp verifies DateCreated is a real
// ISO 8601 timestamp, not hardcoded, across every Create* operation.
func TestHandler_CreateOps_DateCreatedIsRealTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{
			name:   "CreateApplication has real DateCreated",
			action: "Version=2010-12-01&Action=CreateApplication&ApplicationName=ts-app",
		},
		{
			name:   "CreateEnvironment has real DateCreated",
			action: "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1",
		},
		{
			name: "CreateApplicationVersion has real DateCreated",
			action: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app" +
				"&VersionLabel=v1&AutoCreateApplication=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.action)
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<DateCreated>")
			assert.NotContains(t, body, "<DateCreated>2026-01-01T00:00:00Z</DateCreated>",
				"timestamp should not be hardcoded")
			assert.Regexp(t, iso8601Re, body, "response must include an ISO 8601 timestamp")
		})
	}
}

// TestHandler_CreateOps_DateUpdatedPresent verifies DateUpdated is returned for
// resources across every Create* operation.
func TestHandler_CreateOps_DateUpdatedPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  string
		action string
	}{
		{
			name:   "CreateApplication includes DateUpdated",
			action: "Version=2010-12-01&Action=CreateApplication&ApplicationName=app1",
		},
		{
			name:   "CreateEnvironment includes DateUpdated",
			action: "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1",
		},
		{
			name:   "CreateApplicationVersion includes DateUpdated",
			setup:  "Version=2010-12-01&Action=CreateApplication&ApplicationName=app",
			action: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app&VersionLabel=v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != "" {
				postEBForm(t, h, tt.setup)
			}

			rec := postEBForm(t, h, tt.action)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "<DateUpdated>", "response must include DateUpdated")
		})
	}
}
