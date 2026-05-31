package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAudit1_DateCreated_Application verifies that DateCreated is present in application responses.
func TestAudit1_DateCreated_Application(t *testing.T) {
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
			assert.Contains(t, rec.Body.String(), "<DateCreated>2026-01-01T00:00:00Z</DateCreated>")
		})
	}
}

// TestAudit1_DateCreated_Environment verifies that DateCreated is present in environment responses.
func TestAudit1_DateCreated_Environment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  string
		action string
	}{
		{
			name:   "create response includes DateCreated",
			action: "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1",
		},
		{
			name:   "describe response includes DateCreated",
			setup:  "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1",
			action: "Version=2010-12-01&Action=DescribeEnvironments",
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
			assert.Contains(t, rec.Body.String(), "<DateCreated>2026-01-01T00:00:00Z</DateCreated>")
		})
	}
}

// TestAudit1_DateCreated_AppVersion verifies that DateCreated is present in app version responses.
func TestAudit1_DateCreated_AppVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  string
		action string
	}{
		{
			name:   "create response includes DateCreated",
			action: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app&VersionLabel=v1",
		},
		{
			name:   "describe response includes DateCreated",
			setup:  "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app&VersionLabel=v1",
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
			assert.Contains(t, rec.Body.String(), "<DateCreated>2026-01-01T00:00:00Z</DateCreated>")
		})
	}
}

// TestAudit1_DescribeEvents_AfterCreate verifies that DescribeEvents returns a launch event.
func TestAudit1_DescribeEvents_AfterCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   string
		contains []string
		absent   []string
	}{
		{
			name:     "no filter returns all events",
			filter:   "",
			contains: []string{"Successfully launched environment: env1.", "<Severity>INFO</Severity>"},
		},
		{
			name:     "filter by matching EnvironmentName",
			filter:   "&EnvironmentName=env1",
			contains: []string{"Successfully launched environment: env1."},
		},
		{
			name:   "filter by non-matching EnvironmentName returns empty",
			filter: "&EnvironmentName=other",
			absent: []string{"Successfully launched environment"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

			rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents"+tt.filter)
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

// TestAudit1_DescribeEvents_AfterTerminate verifies that DescribeEvents returns a termination event.
func TestAudit1_DescribeEvents_AfterTerminate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h, "Version=2010-12-01&Action=TerminateEnvironment&ApplicationName=app&EnvironmentName=env1")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "terminateEnvironment completed successfully.")
}

// TestAudit1_DescribeEvents_AfterUpdate verifies that DescribeEvents returns an update event.
func TestAudit1_DescribeEvents_AfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=UpdateEnvironment&ApplicationName=app&EnvironmentName=env1&Description=updated")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Environment update completed successfully.")
}

// TestAudit1_DescribeEvents_RecentFirst verifies that newer events appear before older ones.
func TestAudit1_DescribeEvents_RecentFirst(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h, "Version=2010-12-01&Action=TerminateEnvironment&ApplicationName=app&EnvironmentName=env1")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posTerminate := indexOfFirst(body, "terminateEnvironment")
	posLaunch := indexOfFirst(body, "Successfully launched")

	assert.Less(t, posTerminate, posLaunch, "terminate event should appear before launch event (most recent first)")
}

// TestAudit1_ApplicationConfigurationTemplates verifies DescribeApplications includes template names.
func TestAudit1_ApplicationConfigurationTemplates(t *testing.T) {
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

// TestAudit1_DescribeConfigurationSettings_Template verifies TemplateName-based lookup.
func TestAudit1_DescribeConfigurationSettings_Template(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		contains []string
		absent   []string
	}{
		{
			name:     "existing template returns settings",
			action:   "Version=2010-12-01&Action=DescribeConfigurationSettings&ApplicationName=app&TemplateName=tmpl1",
			contains: []string{"<TemplateName>tmpl1</TemplateName>", "<ApplicationName>app</ApplicationName>"},
			absent:   []string{"<EnvironmentName>"},
		},
		{
			name:   "missing template returns empty list",
			action: "Version=2010-12-01&Action=DescribeConfigurationSettings&ApplicationName=app&TemplateName=missing",
			absent: []string{"<member>"},
		},
		{
			name: "template with SolutionStack returns stack name",
			action: "Version=2010-12-01&Action=DescribeConfigurationSettings" +
				"&ApplicationName=app&TemplateName=stack-tmpl",
			contains: []string{"<SolutionStackName>64bit Amazon Linux 2023 v4.3.0 running Go 1</SolutionStackName>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(t, h, "Version=2010-12-01&Action=CreateConfigurationTemplate"+
				"&ApplicationName=app&TemplateName=tmpl1")
			postEBForm(t, h, "Version=2010-12-01&Action=CreateConfigurationTemplate"+
				"&ApplicationName=app&TemplateName=stack-tmpl"+
				"&SolutionStackName=64bit+Amazon+Linux+2023+v4.3.0+running+Go+1")

			rec := postEBForm(t, h, tt.action)
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

// TestAudit1_PersistenceRoundTrip_Events verifies that events survive snapshot/restore.
func TestAudit1_PersistenceRoundTrip_Events(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

	snap := h.Backend.Snapshot()
	require.NotNil(t, snap)

	h2 := newTestHandler()
	require.NoError(t, h2.Backend.Restore(snap))

	rec := postEBForm(t, h2, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Successfully launched environment: env1.")
}
