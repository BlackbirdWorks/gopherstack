package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestHandler_CreateApplicationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name: "create success",
			body: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app" +
				"&VersionLabel=v1&AutoCreateApplication=true",
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

// TestHandler_CreateApplicationVersion_SourceBundleAndBuildInfo verifies source
// bundle, CodeCommit build info, and default processing state.
func TestHandler_CreateApplicationVersion_SourceBundleAndBuildInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		create            string
		contains          []string
		verifyApplication bool
	}{
		{
			name: "source bundle and processing",
			create: "&ApplicationName=bundle-app&VersionLabel=v1&Process=true&AutoCreateApplication=true" +
				"&SourceBundle.S3Bucket=src-bucket&SourceBundle.S3Key=releases%2Fapp.zip",
			contains: []string{
				"<Status>Processed</Status>", "<S3Bucket>src-bucket</S3Bucket>",
				"<S3Key>releases/app.zip</S3Key>",
			},
		},
		{
			name: "codecommit source and automatic application",
			create: "&ApplicationName=source-app&VersionLabel=v2&AutoCreateApplication=true&Process=true" +
				"&SourceBuildInformation.SourceType=CodeCommit" +
				"&SourceBuildInformation.SourceRepository=demo-repo" +
				"&SourceBuildInformation.SourceLocation=main",
			contains: []string{
				"<Status>Processed</Status>", "<SourceType>CodeCommit</SourceType>",
				"<SourceRepository>demo-repo</SourceRepository>", "<SourceLocation>main</SourceLocation>",
			},
			verifyApplication: true,
		},
		{
			name:     "sample application remains unprocessed by default",
			create:   "&ApplicationName=sample-app&VersionLabel=v3&AutoCreateApplication=true",
			contains: []string{"<Status>Unprocessed</Status>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, "Version=2010-12-01&Action=CreateApplicationVersion"+tt.create)
			assert.Equal(t, http.StatusOK, rec.Code)
			for _, expected := range tt.contains {
				assert.Contains(t, rec.Body.String(), expected)
			}
			if tt.verifyApplication {
				apps := postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=DescribeApplications&ApplicationNames.member.1=source-app",
				)
				assert.Contains(t, apps.Body.String(), "<ApplicationName>source-app</ApplicationName>")
			}
		})
	}
}

// TestHandler_CreateApplicationVersion_DateCreatedPresent verifies that DateCreated
// is present in both create and describe application-version responses.
func TestHandler_CreateApplicationVersion_DateCreatedPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  string
		action string
	}{
		{
			name: "create response includes DateCreated",
			action: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app" +
				"&VersionLabel=v1&AutoCreateApplication=true",
		},
		{
			name: "describe response includes DateCreated",
			setup: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app" +
				"&VersionLabel=v1&AutoCreateApplication=true",
			action: "Version=2010-12-01&Action=DescribeApplicationVersions",
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
			assert.Contains(t, rec.Body.String(), "<DateCreated>")
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

// TestHandler_DescribeApplicationVersions_SortedByLabel verifies alphabetic sort order.
func TestHandler_DescribeApplicationVersions_SortedByLabel(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	setupRec := postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=app")
	require.Equal(t, http.StatusOK, setupRec.Code)

	for _, label := range []string{"v3", "v1", "v2"} {
		rec := postEBForm(t, h,
			"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app&VersionLabel="+label)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplicationVersions")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	pos1 := indexOfFirst(body, ">v1<")
	pos2 := indexOfFirst(body, ">v2<")
	pos3 := indexOfFirst(body, ">v3<")

	assert.Less(t, pos1, pos2, "v1 should come before v2")
	assert.Less(t, pos2, pos3, "v2 should come before v3")
}

func TestHandler_DeleteApplicationVersion(t *testing.T) {
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
				postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=my-app")
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
