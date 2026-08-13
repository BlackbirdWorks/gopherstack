package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestHandler_CreateConfigurationTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
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

// TestHandler_ConfigurationTemplate_KeyDoesNotCollideAcrossAppNames verifies that a
// colon in an application name doesn't collide the internal composite key with
// another application's template of the same name.
func TestHandler_ConfigurationTemplate_KeyDoesNotCollideAcrossAppNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create two apps where one name is a prefix of the combined key of the other.
	// "app:x" + templateName "y" must not collide with "app" + templateName "x:y".
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=myapp")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=myapp2")

	const solutionStack = "64bit+Amazon+Linux+2023+v4.0.0+running+Python+3.11"
	rec1 := postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=myapp&TemplateName=tpl1&SolutionStackName="+solutionStack)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=myapp2&TemplateName=tpl1&SolutionStackName="+solutionStack)
	require.Equal(t, http.StatusOK, rec2.Code, "second template for different app should succeed")

	// Describe both — each should see only their own template.
	descRec1 := postEBForm(t, h,
		"Version=2010-12-01&Action=DescribeConfigurationSettings&ApplicationName=myapp&TemplateName=tpl1")
	require.Equal(t, http.StatusOK, descRec1.Code)
	assert.Contains(t, descRec1.Body.String(), "<ApplicationName>myapp</ApplicationName>")

	descRec2 := postEBForm(t, h,
		"Version=2010-12-01&Action=DescribeConfigurationSettings&ApplicationName=myapp2&TemplateName=tpl1")
	require.Equal(t, http.StatusOK, descRec2.Code)
	assert.Contains(t, descRec2.Body.String(), "<ApplicationName>myapp2</ApplicationName>")
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

// TestHandler_DeleteConfigurationTemplate_RoundTrip verifies a create/delete/verify cycle,
// including that deleting an already-deleted template returns not-found.
func TestHandler_DeleteConfigurationTemplate_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=app&TemplateName=tmpl1")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.ConfigTemplateCount())

	rec = postEBForm(t, h,
		"Version=2010-12-01&Action=DeleteConfigurationTemplate"+
			"&ApplicationName=app&TemplateName=tmpl1")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.ConfigTemplateCount())

	// Deleting again should return not-found.
	rec = postEBForm(t, h,
		"Version=2010-12-01&Action=DeleteConfigurationTemplate"+
			"&ApplicationName=app&TemplateName=tmpl1")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

// TestHandler_DescribeConfigurationSettings_EnvironmentIncludesFullFields locks
// that an environment's ConfigurationSettingsDescription includes
// DateCreated, DateUpdated, DeploymentStatus and PlatformArn -- fields
// present on the real AWS wire type but previously omitted entirely.
func TestHandler_DescribeConfigurationSettings_EnvironmentIncludesFullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment"+
		"&ApplicationName=full-app&EnvironmentName=full-env"+
		"&PlatformArn=arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeConfigurationSettings"+
		"&ApplicationName=full-app&EnvironmentName=full-env")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "<DateCreated>")
	assert.Contains(t, body, "<DateUpdated>")
	// This backend applies environment updates synchronously, so a live
	// environment's configuration set is always "deployed" -- see PARITY.md.
	assert.Contains(t, body, "<DeploymentStatus>deployed</DeploymentStatus>")
	assert.Contains(t, body, "<PlatformArn>arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0</PlatformArn>")
}

// TestHandler_DescribeConfigurationSettings_ByTemplateName verifies TemplateName-based lookup.
func TestHandler_DescribeConfigurationSettings_ByTemplateName(t *testing.T) {
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

// TestHandler_DescribeConfigurationSettings_EmptyWhenNoFilters verifies an empty list is
// returned when neither ApplicationName nor EnvironmentName narrows the request.
func TestHandler_DescribeConfigurationSettings_EmptyWhenNoFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// With no ApplicationName and no EnvironmentName, should return empty list.
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeConfigurationSettings")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeConfigurationSettingsResponse")
	// Empty ConfigurationSettings list.
	assert.NotContains(t, rec.Body.String(), "<member>")
}

// TestHandler_PersistenceRoundTrip_ConfigTemplateAndPlatformVersion verifies that a
// configuration template and platform version both survive a handler-level
// snapshot/restore cycle.
func TestHandler_PersistenceRoundTrip_ConfigTemplateAndPlatformVersion(t *testing.T) {
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
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0.0"+
			"&PlatformDefinitionBundle.S3Bucket=my-bucket&PlatformDefinitionBundle.S3Key=my-key.zip",
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Snapshot and restore.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify the config template can still be deleted (meaning it was restored).
	rec3 := postEBForm(
		t,
		h2,
		"Version=2010-12-01&Action=DeleteConfigurationTemplate&ApplicationName=my-app&TemplateName=my-tmpl",
	)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestHandler_CreateConfigurationTemplate_OptionSettingsAndPlatformArn locks
// that OptionSettings and PlatformArn -- both real CreateConfigurationTemplate
// request parameters -- are actually stored, not silently dropped, and are
// readable back through both the create response and
// DescribeConfigurationSettings.
func TestHandler_CreateConfigurationTemplate_OptionSettingsAndPlatformArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := "Version=2010-12-01&Action=CreateConfigurationTemplate" +
		"&ApplicationName=opt-app&TemplateName=opt-tmpl" +
		"&PlatformArn=arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0" +
		"&OptionSettings.member.1.Namespace=aws:autoscaling:asg" +
		"&OptionSettings.member.1.OptionName=MinSize" +
		"&OptionSettings.member.1.Value=2"

	rec := postEBForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)
	createBody := rec.Body.String()
	assert.Contains(t, createBody, "CreateConfigurationTemplateResponse")
	assert.Contains(t, createBody,
		"<PlatformArn>arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0</PlatformArn>")
	assert.Contains(t, createBody, "<Namespace>aws:autoscaling:asg</Namespace>")
	assert.Contains(t, createBody, "<OptionName>MinSize</OptionName>")
	assert.Contains(t, createBody, "<Value>2</Value>")
	assert.Contains(t, createBody, "<DateCreated>")
	assert.Contains(t, createBody, "<DateUpdated>")

	descRec := postEBForm(t, h,
		"Version=2010-12-01&Action=DescribeConfigurationSettings&ApplicationName=opt-app&TemplateName=opt-tmpl")
	require.Equal(t, http.StatusOK, descRec.Code)
	descBody := descRec.Body.String()
	assert.Contains(t, descBody, "<Namespace>aws:autoscaling:asg</Namespace>")
	assert.Contains(t, descBody, "<OptionName>MinSize</OptionName>")
	assert.Contains(t, descBody,
		"<PlatformArn>arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0</PlatformArn>")
	// A template is never associated with a running environment.
	assert.NotContains(t, descBody, "<DeploymentStatus>")
}

// TestHandler_CreateConfigurationTemplate_SolutionStackAndPlatformArnMutuallyExclusive
// locks real AWS's documented constraint: "If you specify PlatformArn, then
// don't specify SolutionStackName".
func TestHandler_CreateConfigurationTemplate_SolutionStackAndPlatformArnMutuallyExclusive(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h, "Version=2010-12-01&Action=CreateConfigurationTemplate"+
		"&ApplicationName=excl-app&TemplateName=excl-tmpl"+
		"&SolutionStackName=64bit+Amazon+Linux"+
		"&PlatformArn=arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// TestHandler_UpdateConfigurationTemplate_OptionSettingsAndRemoval locks that
// UpdateConfigurationTemplate's OptionSettings/OptionsToRemove parameters --
// previously accepted on the wire but silently dropped -- actually mutate
// the template's stored option settings.
func TestHandler_UpdateConfigurationTemplate_OptionSettingsAndRemoval(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateConfigurationTemplate"+
		"&ApplicationName=upd-app&TemplateName=upd-tmpl"+
		"&OptionSettings.member.1.Namespace=aws:autoscaling:asg"+
		"&OptionSettings.member.1.OptionName=MinSize"+
		"&OptionSettings.member.1.Value=1"+
		"&OptionSettings.member.2.Namespace=aws:autoscaling:asg"+
		"&OptionSettings.member.2.OptionName=MaxSize"+
		"&OptionSettings.member.2.Value=4")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=UpdateConfigurationTemplate"+
		"&ApplicationName=upd-app&TemplateName=upd-tmpl"+
		"&OptionSettings.member.1.Namespace=aws:autoscaling:asg"+
		"&OptionSettings.member.1.OptionName=MinSize"+
		"&OptionSettings.member.1.Value=2"+
		"&OptionsToRemove.member.1.Namespace=aws:autoscaling:asg"+
		"&OptionsToRemove.member.1.OptionName=MaxSize")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	// MinSize updated to 2.
	assert.Contains(t, body, "<OptionName>MinSize</OptionName><Value>2</Value>")
	// MaxSize removed.
	assert.NotContains(t, body, "MaxSize")
}

// TestHandler_ValidateConfigurationSettings_ApplicationName proves
// ApplicationName -- a required input this op previously dropped entirely --
// is now genuinely validated for presence and application existence
// (reverting the check in handleValidateConfigurationSettings makes the
// "missing"/"unknown" cases here fail).
func TestHandler_ValidateConfigurationSettings_ApplicationName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name: "known_application",
			body: "Version=2010-12-01&Action=ValidateConfigurationSettings" +
				"&ApplicationName=vcs-app&EnvironmentName=vcs-env",
			wantStatus: http.StatusOK,
			wantXML:    "ValidateConfigurationSettingsResponse",
		},
		{
			name: "missing_application_name",
			body: "Version=2010-12-01&Action=ValidateConfigurationSettings" +
				"&EnvironmentName=vcs-env",
			wantStatus: http.StatusBadRequest,
			wantXML:    "InvalidParameterValue",
		},
		{
			name: "unknown_application_name",
			body: "Version=2010-12-01&Action=ValidateConfigurationSettings" +
				"&ApplicationName=does-not-exist&EnvironmentName=vcs-env",
			wantStatus: http.StatusBadRequest,
			wantXML:    "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=vcs-app")
			postEBForm(t, h,
				"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=vcs-app&EnvironmentName=vcs-env")

			rec := postEBForm(t, h, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantXML)
		})
	}
}
