package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_DescribeApplications_IncludesConfigurationTemplates verifies
// DescribeApplications includes configuration template names.
func TestHandler_DescribeApplications_IncludesConfigurationTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		templatesBefore []string
		contains        []string
		absent          []string
	}{
		{
			name:     "no templates — empty list",
			contains: []string{"<ApplicationName>myapp</ApplicationName>"},
			absent:   []string{"<ConfigurationTemplates>"},
		},
		{
			name:            "one template — name included",
			templatesBefore: []string{"tmpl1"},
			contains:        []string{"<member>tmpl1</member>"},
		},
		{
			name:            "two templates — both names included",
			templatesBefore: []string{"tmpl1", "tmpl2"},
			contains:        []string{"<member>tmpl1</member>", "<member>tmpl2</member>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=myapp")

			for _, name := range tt.templatesBefore {
				postEBForm(t, h, "Version=2010-12-01&Action=CreateConfigurationTemplate"+
					"&ApplicationName=myapp&TemplateName="+name)
			}

			rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplications")
			require.Equal(t, http.StatusOK, rec.Code)

			for _, s := range tt.contains {
				assert.Contains(t, rec.Body.String(), s)
			}

			for _, s := range tt.absent {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestHandler_DescribeApplications_SortedByName verifies alphabetic sort order.
func TestHandler_DescribeApplications_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"zapp", "aapp", "mapp"} {
		rec := postEBForm(t, h,
			"Version=2010-12-01&Action=CreateApplication&ApplicationName="+name)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplications")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posA := indexOfFirst(body, "aapp")
	posM := indexOfFirst(body, "mapp")
	posZ := indexOfFirst(body, "zapp")

	assert.Less(t, posA, posM, "aapp should come before mapp")
	assert.Less(t, posM, posZ, "mapp should come before zapp")
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

// TestHandler_DeleteApplication_CascadesToRelatedResources verifies that deleting an
// application removes its environments, versions, and configuration templates.
func TestHandler_DeleteApplication_CascadesToRelatedResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=my-app")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=env1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=my-app&TemplateName=tmpl1")

	assert.Equal(t, 1, h.Backend.EnvironmentCount())
	assert.Equal(t, 1, h.Backend.AppVersionCount())
	assert.Equal(t, 1, h.Backend.ConfigTemplateCount())

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=DeleteApplication&ApplicationName=my-app")
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, h.Backend.ApplicationCount())
	assert.Equal(t, 0, h.Backend.EnvironmentCount())
	assert.Equal(t, 0, h.Backend.AppVersionCount())
	assert.Equal(t, 0, h.Backend.ConfigTemplateCount())
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

// TestHandler_UpdateApplicationResourceLifecycle_SurfacedOnDescribe verifies that a
// lifecycle service role set via UpdateApplicationResourceLifecycle is
// readable back through DescribeApplications -- the backend stores it on
// Application, and it must not be a write-only field.
func TestHandler_UpdateApplicationResourceLifecycle_SurfacedOnDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=lc-app")

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=UpdateApplicationResourceLifecycle&ApplicationName=lc-app"+
			"&ResourceLifecycleConfig.ServiceRole=arn:aws:iam::123456789012:role/eb-lifecycle")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplications&ApplicationNames.member.1=lc-app")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ResourceLifecycleConfig>")
	assert.Contains(t, body, "<ServiceRole>arn:aws:iam::123456789012:role/eb-lifecycle</ServiceRole>")
}

// TestHandler_CreateApplication_DateCreatedPresent verifies that DateCreated is
// present in both create and describe application responses.
func TestHandler_CreateApplication_DateCreatedPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  string
		action string
	}{
		{
			name:   "create response includes DateCreated",
			action: "Version=2010-12-01&Action=CreateApplication&ApplicationName=ts-app",
		},
		{
			name:   "describe response includes DateCreated",
			setup:  "Version=2010-12-01&Action=CreateApplication&ApplicationName=ts-app",
			action: "Version=2010-12-01&Action=DescribeApplications",
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
