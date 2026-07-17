package elasticbeanstalk_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_CreateEnvironment_DateCreatedPresent verifies that DateCreated is
// present in both create and describe environment responses.
func TestHandler_CreateEnvironment_DateCreatedPresent(t *testing.T) {
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
			assert.Contains(t, rec.Body.String(), "<DateCreated>")
		})
	}
}

// TestHandler_Environment_TierAndOptionSettingsState verifies tier, option-settings, and
// topology-derived state (CNAME, option settings round-trip, worker queue resources).
func TestHandler_Environment_TierAndOptionSettingsState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		create   string
		update   string
		action   string
		contains []string
	}{
		{
			name: "create selection fields and tier",
			create: "&CNAMEPrefix=customer-url&PlatformArn=arn%3Aaws%3Aelasticbeanstalk%3Aus-east-1%3Aplatform%2Fgo" +
				"&VersionLabel=v1&Tier.Name=Worker&Tier.Type=SQS%2FHTTP&Tier.Version=1.1",
			action: "DescribeEnvironments&ApplicationName=app&EnvironmentNames.member.1=env1",
			contains: []string{
				"customer-url.us-east-1.elasticbeanstalk.com", "<PlatformArn>arn:aws:elasticbeanstalk:",
				"<VersionLabel>v1</VersionLabel>", "<Name>Worker</Name>", "<Version>1.1</Version>",
			},
		},
		{
			name: "update options and remove original",
			create: "&SolutionStackName=stack-a&OptionSettings.member.1.Namespace=aws%3Aec2%3Avpc" +
				"&OptionSettings.member.1.OptionName=VPCId&OptionSettings.member.1.Value=vpc-old" +
				"&OptionSettings.member.2.Namespace=aws%3Aelasticbeanstalk%3Aenvironment" +
				"&OptionSettings.member.2.OptionName=EnvironmentType&OptionSettings.member.2.Value=SingleInstance",
			update: "&SolutionStackName=stack-b&OptionSettings.member.1.Namespace=aws%3Aec2%3Avpc" +
				"&OptionSettings.member.1.OptionName=Subnets&OptionSettings.member.1.Value=subnet-new" +
				"&OptionsToRemove.member.1.Namespace=aws%3Aec2%3Avpc&OptionsToRemove.member.1.OptionName=VPCId",
			action: "DescribeConfigurationSettings&ApplicationName=app&EnvironmentName=env1",
			contains: []string{
				"<SolutionStackName>stack-b</SolutionStackName>", "<OptionName>Subnets</OptionName>",
				"<Value>subnet-new</Value>", "<OptionName>EnvironmentType</OptionName>",
				"<Value>SingleInstance</Value>",
			},
		},
		{
			name:   "worker topology includes queue",
			create: "&Tier.Name=Worker",
			action: "DescribeEnvironmentResources&EnvironmentName=env1",
			contains: []string{
				"<AutoScalingGroups><member><Name>env1-asg</Name>", "<Queues><member><URL>https://sqs.",
				"env1</URL>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			create := "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1" + tt.create
			assert.Equal(t, http.StatusOK, postEBForm(t, h, create).Code)
			if tt.update != "" {
				update := "Version=2010-12-01&Action=UpdateEnvironment&ApplicationName=app&EnvironmentName=env1" + tt.update
				assert.Equal(t, http.StatusOK, postEBForm(t, h, update).Code)
			}

			rec := postEBForm(t, h, "Version=2010-12-01&Action="+tt.action)
			assert.Equal(t, http.StatusOK, rec.Code)
			for _, expected := range tt.contains {
				assert.Contains(t, rec.Body.String(), expected)
			}
			if tt.name == "update options and remove original" {
				assert.NotContains(t, rec.Body.String(), "vpc-old")
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

// TestHandler_DescribeEnvironments_SortedByName verifies alphabetic sort order.
func TestHandler_DescribeEnvironments_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"zoo-env", "alpha-env", "mid-env"} {
		rec := postEBForm(t, h,
			"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName="+name)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEnvironments")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posA := indexOfFirst(body, "alpha-env")
	posM := indexOfFirst(body, "mid-env")
	posZ := indexOfFirst(body, "zoo-env")

	assert.Less(t, posA, posM, "alpha-env should come before mid-env")
	assert.Less(t, posM, posZ, "mid-env should come before zoo-env")
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

// TestHandler_UpdateEnvironment_ResolvesApplicationByEnvironmentName verifies
// UpdateEnvironment resolves the owning application when ApplicationName is omitted.
func TestHandler_UpdateEnvironment_ResolvesApplicationByEnvironmentName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env")

	// Update without specifying ApplicationName.
	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=UpdateEnvironment&EnvironmentName=my-env&Description=updated-desc")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdateEnvironmentResponse")
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

// TestHandler_Environment_NotFoundUsesResourceNotFoundException verifies that
// environment lookups by name surface the specific ResourceNotFoundException
// wire code for these operations.
func TestHandler_Environment_NotFoundUsesResourceNotFoundException(t *testing.T) {
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

// TestHandler_DescribeEnvironmentHealth_NotFoundError verifies the error body for a
// missing environment and that it does not fabricate placeholder health/status values.
func TestHandler_DescribeEnvironmentHealth_NotFoundError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEnvironmentHealth&EnvironmentName=missing")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ResourceNotFoundException")
	assert.NotContains(t, body, "Grey", "should not return Grey for missing env")
	assert.NotContains(t, body, "Terminated", "should not return Terminated for missing env")
}
