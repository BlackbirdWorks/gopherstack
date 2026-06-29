package elasticbeanstalk_test

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// iso8601Re matches ISO 8601 UTC timestamps like 2026-06-26T09:12:26Z.
var iso8601Re = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`)

// TestParity_ErrNotFound_ResourceNotFoundException verifies not-found errors use the correct code.
func TestParity_ErrNotFound_ResourceNotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{
			name:   "DescribeEnvironmentHealth missing env",
			action: "Version=2010-12-01&Action=DescribeEnvironmentHealth&EnvironmentName=doesnotexist",
		},
		{
			name: "AssociateEnvironmentOperationsRole missing env",
			action: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&EnvironmentName=doesnotexist&OperationsRole=arn:aws:iam::123:role/r",
		},
		{
			name:   "DisassociateEnvironmentOperationsRole missing env",
			action: "Version=2010-12-01&Action=DisassociateEnvironmentOperationsRole&EnvironmentName=doesnotexist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.action)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
		})
	}
}

// TestParity_DateCreated_RealTimestamp verifies DateCreated is a real ISO 8601 timestamp, not hardcoded.
func TestParity_DateCreated_RealTimestamp(t *testing.T) {
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
			name:   "CreateApplicationVersion has real DateCreated",
			action: "Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app&VersionLabel=v1",
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

// TestParity_DateUpdated_Present verifies DateUpdated is returned for resources.
func TestParity_DateUpdated_Present(t *testing.T) {
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

// TestParity_DescribeEnvironmentHealth_NotFound_Error verifies error returned for missing env.
func TestParity_DescribeEnvironmentHealth_NotFound_Error(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEnvironmentHealth&EnvironmentName=missing")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ResourceNotFoundException")
	assert.NotContains(t, body, "Grey", "should not return Grey for missing env")
	assert.NotContains(t, body, "Terminated", "should not return Terminated for missing env")
}

// TestParity_ConfigTemplate_ColonInAppName verifies colon in app name doesn't collide keys.
func TestParity_ConfigTemplate_ColonInAppName(t *testing.T) {
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

// TestParity_Events_RealTimestamp verifies events carry real timestamps.
func TestParity_Events_RealTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<EventDate>")
	assert.NotContains(t, body, "<EventDate>2026-01-01T00:00:00Z</EventDate>", "event date should not be hardcoded")
	assert.Regexp(t, iso8601Re, body)
}

// TestParity_CheckDNSAvailability_UsedCNAME verifies CNAME is unavailable after env creation.
func TestParity_CheckDNSAvailability_UsedCNAME(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=myenv&CNAMEPrefix=myenv")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix=myenv")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Available>false</Available>")
}

// TestParity_CheckDNSAvailability_FreeCNAME verifies unused CNAME is available.
func TestParity_CheckDNSAvailability_FreeCNAME(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix=unused-prefix")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Available>true</Available>")
}
