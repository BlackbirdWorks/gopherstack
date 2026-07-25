package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DescribeConfigurationOptions_FullCatalog locks that
// DescribeConfigurationOptions returns a real, multi-namespace catalog of
// options with the documented ConfigurationOptionDescription fields
// (DefaultValue, ChangeSeverity, ValueType, ValueOptions, MinValue) --
// previously this op always returned the same 3 bare options regardless of
// the request.
func TestHandler_DescribeConfigurationOptions_FullCatalog(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeConfigurationOptions")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "DescribeConfigurationOptionsResponse")
	// Spans multiple namespaces, not just aws:autoscaling:asg.
	assert.Contains(t, body, "<Namespace>aws:autoscaling:asg</Namespace>")
	assert.Contains(t, body, "<Namespace>aws:rds:dbinstance</Namespace>")
	assert.Contains(t, body, "<Namespace>aws:elb:loadbalancer</Namespace>")
	// Documented ConfigurationOptionDescription fields beyond Namespace/Name/ValueType.
	assert.Contains(t, body, "<DefaultValue>")
	assert.Contains(t, body, "<ChangeSeverity>")
	assert.Contains(t, body, "<UserDefined>false</UserDefined>")
	// A List-typed option and a Boolean-typed option are both represented.
	assert.Contains(t, body, "<ValueType>List</ValueType>")
	assert.Contains(t, body, "<ValueType>Boolean</ValueType>")
	// A constrained option renders its ValueOptions enumeration.
	assert.Contains(t, body, "<ValueOptions>")
	// A numerically bounded option renders MinValue.
	assert.Contains(t, body, "<MinValue>")

	// The full, unfiltered catalog has well over the original 3 entries.
	assert.Greater(t, len(body), 3000)
}

// TestHandler_DescribeConfigurationOptions_FilteredByOptions verifies the
// Options request parameter -- "If specified, restricts the descriptions to
// only the specified options" -- actually filters the response, rather than
// being ignored.
func TestHandler_DescribeConfigurationOptions_FilteredByOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeConfigurationOptions"+
		"&Options.member.1.Namespace=aws:autoscaling:asg"+
		"&Options.member.1.OptionName=MinSize")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "<Name>MinSize</Name>")
	assert.NotContains(t, body, "<Name>MaxSize</Name>")
	assert.NotContains(t, body, "aws:rds:dbinstance")
}

// TestHandler_DescribeConfigurationOptions_EchoesResolvedPlatform verifies
// that SolutionStackName/PlatformArn on the response are resolved from the
// request's SolutionStackName, PlatformArn, or referenced
// environment/template -- not left unset (the response previously had no
// SolutionStackName/PlatformArn fields at all).
func TestHandler_DescribeConfigurationOptions_EchoesResolvedPlatform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{
			name:     "explicit SolutionStackName echoed",
			body:     "Version=2010-12-01&Action=DescribeConfigurationOptions&SolutionStackName=64bit+Amazon+Linux",
			contains: "<SolutionStackName>64bit Amazon Linux</SolutionStackName>",
		},
		{
			name: "explicit PlatformArn echoed",
			body: "Version=2010-12-01&Action=DescribeConfigurationOptions" +
				"&PlatformArn=arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0",
			contains: "<PlatformArn>arn:aws:elasticbeanstalk:us-east-1::platform/MyPlatform/1.0.0</PlatformArn>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.contains)
		})
	}
}

// TestHandler_DescribeConfigurationOptions_ResolvesFromEnvironment verifies
// that when neither SolutionStackName nor PlatformArn is given directly, the
// response resolves them from the referenced EnvironmentName.
func TestHandler_DescribeConfigurationOptions_ResolvesFromEnvironment(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment"+
		"&ApplicationName=opts-app&EnvironmentName=opts-env&SolutionStackName=64bit+Amazon+Linux+2023")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeConfigurationOptions"+
		"&ApplicationName=opts-app&EnvironmentName=opts-env")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<SolutionStackName>64bit Amazon Linux 2023</SolutionStackName>")
}
