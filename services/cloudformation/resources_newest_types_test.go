package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	accessanalyzerbackend "github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	datasyncbackend "github.com/blackbirdworks/gopherstack/services/datasync"
	guarddutybackend "github.com/blackbirdworks/gopherstack/services/guardduty"
	macie2backend "github.com/blackbirdworks/gopherstack/services/macie2"
)

// newSweepTestClient wires a real aws-sdk-go-v2 CloudFormation client against
// a backend with DataSync, AppConfig, Macie2, GuardDuty, AccessAnalyzer, and
// Amplify (among the phase-3/4 backends newMoreTypesServiceBackends already
// wires: Glue, Transfer, Batch, EFS, Redshift) all wired to real in-memory
// service backends.
func newSweepTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	backends.DataSync = datasyncbackend.NewHandler(datasyncbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.AppConfig = appconfigbackend.NewHandler(appconfigbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.Macie2 = macie2backend.NewHandler(macie2backend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.GuardDuty = guarddutybackend.NewHandler(guarddutybackend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.AccessAnalyzer = accessanalyzerbackend.NewHandler(
		accessanalyzerbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	backends.Amplify = amplifybackend.NewHandler(amplifybackend.NewInMemoryBackend("000000000000", "us-east-1"))

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_GlueNewerTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testGlueBlueprint, "blueprint"},
		{testGlueCustomEntityType, "custom_entity_type"},
		{testGlueNewerWorkflow, "workflow"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testGlueBlueprint(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"BP": {"Type": "AWS::Glue::Blueprint", "Properties": {
  "Name": "bp-1",
  "BlueprintLocation": "s3://my-bucket/bp.json",
  "Description": "test blueprint"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "BP"}},
  "Status": {"Value": {"Fn::GetAtt": ["BP", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-bp-stack", tmpl)
	assert.Equal(t, "bp-1", outputs["Ref"])
	assert.Equal(t, "ACTIVE", outputs["Status"])

	found, _ := backends.Glue.Backend.BatchGetBlueprints([]string{"bp-1"})
	require.Len(t, found, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-bp-stack")})
	require.NoError(t, err)

	found, _ = backends.Glue.Backend.BatchGetBlueprints([]string{"bp-1"})
	require.Empty(t, found)
}

func testGlueCustomEntityType(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"CET": {"Type": "AWS::Glue::CustomEntityType", "Properties": {
  "Name": "cet-1",
  "RegexString": "[0-9]{9}"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "CET"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-cet-stack", tmpl)
	assert.Equal(t, "cet-1", outputs["Ref"])

	_, err := backends.Glue.Backend.GetCustomEntityType("cet-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-cet-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.GetCustomEntityType("cet-1")
	require.Error(t, err)
}

func testGlueNewerWorkflow(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"WF": {"Type": "AWS::Glue::Workflow", "Properties": {
  "Name": "wf-1",
  "Description": "test workflow"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "WF"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-wf-stack", tmpl)
	assert.Equal(t, "wf-1", outputs["Ref"])

	_, err := backends.Glue.Backend.GetWorkflow("wf-1", false)
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-wf-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.GetWorkflow("wf-1", false)
	require.Error(t, err)
}

func TestCreateStack_DataSyncTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testDataSyncAgent, "agent"},
		{testDataSyncLocationS3, "location_s3"},
		{testDataSyncTask, "task"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testDataSyncAgent(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"Agent": {"Type": "AWS::DataSync::Agent", "Properties": {
  "ActivationKey": "AAAAA-BBBBB-CCCCC-DDDDD-EEEEE",
  "AgentName": "my-agent"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Agent"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Agent", "AgentArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ds-agent-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Ref"], "agent/")

	_, err := backends.DataSync.Backend.DescribeAgent(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ds-agent-stack")})
	require.NoError(t, err)

	_, err = backends.DataSync.Backend.DescribeAgent(outputs["Ref"])
	require.Error(t, err)
}

func testDataSyncLocationS3(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"Loc": {"Type": "AWS::DataSync::LocationS3", "Properties": {
  "S3BucketArn": "arn:aws:s3:::my-bucket",
  "S3Config": {"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/ds-role"},
  "Subdirectory": "/data"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Loc"}},
  "Uri": {"Value": {"Fn::GetAtt": ["Loc", "LocationUri"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ds-loc-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "location/")
	assert.Contains(t, outputs["Uri"], "s3://my-bucket/data")

	_, err := backends.DataSync.Backend.DescribeLocationS3(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ds-loc-stack")})
	require.NoError(t, err)

	_, err = backends.DataSync.Backend.DescribeLocationS3(outputs["Ref"])
	require.Error(t, err)
}

// testDataSyncTask exercises a real Fn::GetAtt cross-reference: the task's
// SourceLocationArn/DestinationLocationArn are pulled from two LocationS3
// resources' LocationArn attribute, not their Ref (which happens to be the
// same value here, but the template deliberately uses Fn::GetAtt).
func testDataSyncTask(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "Src": {"Type": "AWS::DataSync::LocationS3", "Properties": {
    "S3BucketArn": "arn:aws:s3:::src-bucket",
    "S3Config": {"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/ds-role"}
  }},
  "Dst": {"Type": "AWS::DataSync::LocationS3", "Properties": {
    "S3BucketArn": "arn:aws:s3:::dst-bucket",
    "S3Config": {"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/ds-role"}
  }},
  "Task": {"Type": "AWS::DataSync::Task", "Properties": {
    "SourceLocationArn": {"Fn::GetAtt": ["Src", "LocationArn"]},
    "DestinationLocationArn": {"Fn::GetAtt": ["Dst", "LocationArn"]},
    "Name": "my-task"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Task"}},
  "Status": {"Value": {"Fn::GetAtt": ["Task", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ds-task-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "task/")
	assert.NotEmpty(t, outputs["Status"])

	task, err := backends.DataSync.Backend.DescribeTask(outputs["Ref"])
	require.NoError(t, err)
	assert.Contains(t, task.SourceLocationArn, "location/")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ds-task-stack")})
	require.NoError(t, err)

	_, err = backends.DataSync.Backend.DescribeTask(outputs["Ref"])
	require.Error(t, err)
}

func TestCreateStack_TransferMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testTransferProfile, "profile"},
		{testTransferWorkflow, "workflow"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testTransferProfile(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"Profile": {"Type": "AWS::Transfer::Profile", "Properties": {
  "ProfileType": "LOCAL",
  "As2Id": "MYAS2ID"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Profile"}},
  "Id": {"Value": {"Fn::GetAtt": ["Profile", "ProfileId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "transfer-profile-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "profile/")
	assert.Contains(t, outputs["Ref"], outputs["Id"])

	_, err := backends.Transfer.Backend.DescribeProfile(outputs["Id"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("transfer-profile-stack")})
	require.NoError(t, err)

	_, err = backends.Transfer.Backend.DescribeProfile(outputs["Id"])
	require.Error(t, err)
}

func testTransferWorkflow(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"Workflow": {"Type": "AWS::Transfer::Workflow", "Properties": {
  "Description": "test workflow",
  "Steps": [{"Type": "DELETE"}]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Workflow"}},
  "Id": {"Value": {"Fn::GetAtt": ["Workflow", "WorkflowId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "transfer-wf-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.Transfer.Backend.DescribeWorkflow(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("transfer-wf-stack")})
	require.NoError(t, err)

	_, err = backends.Transfer.Backend.DescribeWorkflow(outputs["Ref"])
	require.Error(t, err, "workflow should already be gone after stack deletion")
}

func TestCreateStack_AppConfigTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testAppConfigApplication, "application"},
		{testAppConfigEnvironment, "environment"},
		{testAppConfigConfigurationProfile, "configuration_profile"},
		{testAppConfigDeploymentStrategy, "deployment_strategy"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testAppConfigApplication(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"App": {"Type": "AWS::AppConfig::Application", "Properties": {
  "Name": "my-app"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "App"}},
  "Id": {"Value": {"Fn::GetAtt": ["App", "ApplicationId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "appconfig-app-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.AppConfig.Backend.GetApplication(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("appconfig-app-stack")})
	require.NoError(t, err)

	_, err = backends.AppConfig.Backend.GetApplication(outputs["Ref"])
	require.Error(t, err)
}

// testAppConfigEnvironment cross-references the owning Application via
// Fn::GetAtt ApplicationId (rather than Ref, which happens to return the
// same value here).
func testAppConfigEnvironment(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "App": {"Type": "AWS::AppConfig::Application", "Properties": {"Name": "env-app"}},
  "Env": {"Type": "AWS::AppConfig::Environment", "Properties": {
    "ApplicationId": {"Fn::GetAtt": ["App", "ApplicationId"]},
    "Name": "my-env"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Env"}},
  "Id": {"Value": {"Fn::GetAtt": ["Env", "EnvironmentId"]}},
  "AppId": {"Value": {"Ref": "App"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "appconfig-env-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.AppConfig.Backend.GetEnvironment(outputs["AppId"], outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("appconfig-env-stack")})
	require.NoError(t, err)

	_, err = backends.AppConfig.Backend.GetEnvironment(outputs["AppId"], outputs["Ref"])
	require.Error(t, err)
}

func testAppConfigConfigurationProfile(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "App": {"Type": "AWS::AppConfig::Application", "Properties": {"Name": "cp-app"}},
  "Profile": {"Type": "AWS::AppConfig::ConfigurationProfile", "Properties": {
    "ApplicationId": {"Fn::GetAtt": ["App", "ApplicationId"]},
    "Name": "my-profile",
    "LocationUri": "hosted",
    "Type": "AWS.Freeform"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Profile"}},
  "Id": {"Value": {"Fn::GetAtt": ["Profile", "ConfigurationProfileId"]}},
  "AppId": {"Value": {"Ref": "App"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "appconfig-cp-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.AppConfig.Backend.GetConfigurationProfile(outputs["AppId"], outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("appconfig-cp-stack")})
	require.NoError(t, err)

	_, err = backends.AppConfig.Backend.GetConfigurationProfile(outputs["AppId"], outputs["Ref"])
	require.Error(t, err)
}

func testAppConfigDeploymentStrategy(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"Strategy": {"Type": "AWS::AppConfig::DeploymentStrategy", "Properties": {
  "Name": "my-strategy",
  "DeploymentDurationInMinutes": 3,
  "FinalBakeTimeInMinutes": 1,
  "GrowthFactor": 10,
  "GrowthType": "LINEAR",
  "ReplicateTo": "NONE"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Strategy"}},
  "Id": {"Value": {"Fn::GetAtt": ["Strategy", "Id"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "appconfig-ds-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.AppConfig.Backend.GetDeploymentStrategy(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("appconfig-ds-stack")})
	require.NoError(t, err)

	_, err = backends.AppConfig.Backend.GetDeploymentStrategy(outputs["Ref"])
	require.Error(t, err)
}

func TestCreateStack_MacieTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testMacieAllowList, "allow_list"},
		{testMacieFindingsFilter, "findings_filter"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testMacieAllowList(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"AL": {"Type": "AWS::Macie::AllowList", "Properties": {
  "Name": "my-allow-list",
  "Criteria": {"Regex": "^.*@example\\.com$"}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "AL"}},
  "Id": {"Value": {"Fn::GetAtt": ["AL", "Id"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "macie-al-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.Macie2.Backend.GetAllowList(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("macie-al-stack")})
	require.NoError(t, err)

	_, err = backends.Macie2.Backend.GetAllowList(outputs["Ref"])
	require.Error(t, err)
}

func testMacieFindingsFilter(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"FF": {"Type": "AWS::Macie::FindingsFilter", "Properties": {
  "Name": "my-filter",
  "Action": "ARCHIVE",
  "FindingCriteria": {"Criterion": {"accountId": {"eq": ["000000000000"]}}}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "FF"}},
  "Arn": {"Value": {"Fn::GetAtt": ["FF", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "macie-ff-stack", tmpl)
	assert.Contains(t, outputs["Arn"], outputs["Ref"])

	_, err := backends.Macie2.Backend.GetFindingsFilter(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("macie-ff-stack")})
	require.NoError(t, err)

	_, err = backends.Macie2.Backend.GetFindingsFilter(outputs["Ref"])
	require.Error(t, err)
}

func TestCreateStack_GuardDutyTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testGuardDutyDetector, "detector"},
		{testGuardDutyIPSet, "ip_set"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testGuardDutyDetector(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"Det": {"Type": "AWS::GuardDuty::Detector", "Properties": {
  "Enable": true,
  "FindingPublishingFrequency": "FIFTEEN_MINUTES"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Det"}},
  "Id": {"Value": {"Fn::GetAtt": ["Det", "Id"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "gd-det-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	_, err := backends.GuardDuty.Backend.GetDetector(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("gd-det-stack")})
	require.NoError(t, err)

	_, err = backends.GuardDuty.Backend.GetDetector(outputs["Ref"])
	require.Error(t, err)
}

// testGuardDutyIPSet cross-references the owning Detector via Fn::GetAtt Id.
func testGuardDutyIPSet(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "Det": {"Type": "AWS::GuardDuty::Detector", "Properties": {"Enable": true}},
  "Set": {"Type": "AWS::GuardDuty::IPSet", "Properties": {
    "DetectorId": {"Fn::GetAtt": ["Det", "Id"]},
    "Name": "my-ipset",
    "Format": "TXT",
    "Location": "https://s3.amazonaws.com/bucket/ipset.txt",
    "Activate": true
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Set"}},
  "DetectorId": {"Value": {"Ref": "Det"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "gd-ipset-stack", tmpl)
	assert.NotEmpty(t, outputs["Ref"])

	_, err := backends.GuardDuty.Backend.GetIPSet(outputs["DetectorId"], outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("gd-ipset-stack")})
	require.NoError(t, err)

	_, err = backends.GuardDuty.Backend.GetIPSet(outputs["DetectorId"], outputs["Ref"])
	require.Error(t, err)
}

func TestCreateStack_AccessAnalyzerTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testAccessAnalyzerAnalyzer, "analyzer"},
		{testAccessAnalyzerArchiveRule, "archive_rule"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testAccessAnalyzerAnalyzer(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"A": {"Type": "AWS::AccessAnalyzer::Analyzer", "Properties": {
  "AnalyzerName": "my-analyzer",
  "Type": "ACCOUNT"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "A"}},
  "Arn": {"Value": {"Fn::GetAtt": ["A", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "aa-analyzer-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Ref"], "analyzer/my-analyzer")

	_, err := backends.AccessAnalyzer.Backend.GetAnalyzer("my-analyzer")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("aa-analyzer-stack")})
	require.NoError(t, err)

	_, err = backends.AccessAnalyzer.Backend.GetAnalyzer("my-analyzer")
	require.Error(t, err)
}

// testAccessAnalyzerArchiveRule cross-references the owning Analyzer via
// Fn::GetAtt Arn: AnalyzerName expects a bare name, but Analyzer's own Ref
// (and Arn attribute) is an ARN, so the resource creator must convert it
// back to a name -- exactly the same real-world template shape AWS's own
// docs show for sibling ARN-Ref'd resources.
func testAccessAnalyzerArchiveRule(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "A": {"Type": "AWS::AccessAnalyzer::Analyzer", "Properties": {
    "AnalyzerName": "rule-analyzer", "Type": "ACCOUNT"
  }},
  "Rule": {"Type": "AWS::AccessAnalyzer::ArchiveRule", "Properties": {
    "AnalyzerName": {"Fn::GetAtt": ["A", "Arn"]},
    "RuleName": "my-rule",
    "Filter": [{"Property": "principal.AWS", "Eq": ["123456789012"]}]
  }}
},
"Outputs": {"Ref": {"Value": {"Ref": "Rule"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "aa-rule-stack", tmpl)
	assert.Equal(t, "my-rule", outputs["Ref"])

	_, err := backends.AccessAnalyzer.Backend.GetArchiveRule("rule-analyzer", "my-rule")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("aa-rule-stack")})
	require.NoError(t, err)

	_, err = backends.AccessAnalyzer.Backend.GetArchiveRule("rule-analyzer", "my-rule")
	require.Error(t, err)
}

func TestCreateStack_AmplifyTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testAmplifyApp, "app"},
		{testAmplifyBranch, "branch"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testAmplifyApp(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"App": {"Type": "AWS::Amplify::App", "Properties": {
  "Name": "my-amplify-app"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "App"}},
  "AppId": {"Value": {"Fn::GetAtt": ["App", "AppId"]}},
  "AppName": {"Value": {"Fn::GetAtt": ["App", "AppName"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "amplify-app-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["AppId"])
	assert.Equal(t, "my-amplify-app", outputs["AppName"])

	_, err := backends.Amplify.Backend.GetApp(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("amplify-app-stack")})
	require.NoError(t, err)

	_, err = backends.Amplify.Backend.GetApp(outputs["Ref"])
	require.Error(t, err)
}

// testAmplifyBranch cross-references the owning App via Fn::GetAtt AppId.
func testAmplifyBranch(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "App": {"Type": "AWS::Amplify::App", "Properties": {"Name": "branch-app"}},
  "Branch": {"Type": "AWS::Amplify::Branch", "Properties": {
    "AppId": {"Fn::GetAtt": ["App", "AppId"]},
    "BranchName": "main"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Branch"}},
  "AppId": {"Value": {"Ref": "App"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "amplify-branch-stack", tmpl)
	assert.Equal(t, "main", outputs["Ref"])

	_, err := backends.Amplify.Backend.GetBranch(outputs["AppId"], outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("amplify-branch-stack")})
	require.NoError(t, err)

	_, err = backends.Amplify.Backend.GetBranch(outputs["AppId"], outputs["Ref"])
	require.Error(t, err)
}

func TestCreateStack_BatchMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testBatchSchedulingPolicy, "scheduling_policy"},
		{testBatchServiceEnvironment, "service_environment"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testBatchSchedulingPolicy(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"SP": {"Type": "AWS::Batch::SchedulingPolicy", "Properties": {
  "Name": "my-scheduling-policy"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "SP"}},
  "Arn": {"Value": {"Fn::GetAtt": ["SP", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "batch-sp-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Ref"], "scheduling-policy/")

	got := backends.Batch.Backend.DescribeSchedulingPolicies(t.Context(), []string{outputs["Ref"]})
	require.Len(t, got, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("batch-sp-stack")})
	require.NoError(t, err)

	got = backends.Batch.Backend.DescribeSchedulingPolicies(t.Context(), []string{outputs["Ref"]})
	assert.Empty(t, got)
}

func testBatchServiceEnvironment(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"SE": {"Type": "AWS::Batch::ServiceEnvironment", "Properties": {
  "ServiceEnvironmentName": "my-service-env",
  "ServiceEnvironmentType": "SAGEMAKER_TRAINING",
  "State": "DISABLED",
  "CapacityLimits": [{"MaxCapacity": 10, "CapacityUnit": "NUM_INSTANCES"}]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "SE"}},
  "Arn": {"Value": {"Fn::GetAtt": ["SE", "ServiceEnvironmentArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "batch-se-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])

	got, _ := backends.Batch.Backend.DescribeServiceEnvironments(t.Context(), []string{outputs["Ref"]}, 0, "")
	require.Len(t, got, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("batch-se-stack")})
	require.NoError(t, err)

	got, _ = backends.Batch.Backend.DescribeServiceEnvironments(t.Context(), []string{outputs["Ref"]}, 0, "")
	assert.Empty(t, got)
}

// TestCreateStack_EFSAccessPoint exercises AWS::EFS::AccessPoint,
// cross-referencing its owning file system via a plain Ref (FileSystemId).
func TestCreateStack_EFSAccessPoint(t *testing.T) {
	t.Parallel()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {
  "FS": {"Type": "AWS::EFS::FileSystem", "Properties": {}},
  "AP": {"Type": "AWS::EFS::AccessPoint", "Properties": {
    "FileSystemId": {"Ref": "FS"},
    "PosixUser": {"Uid": "1000", "Gid": "1000"},
    "RootDirectory": {"Path": "/data", "CreationInfo": {
      "OwnerUid": "1000", "OwnerGid": "1000", "Permissions": "0755"
    }}
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "AP"}},
  "Arn": {"Value": {"Fn::GetAtt": ["AP", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "efs-ap-stack", tmpl)
	assert.Contains(t, outputs["Arn"], outputs["Ref"])

	found, _, err := backends.EFS.Backend.DescribeAccessPoints(t.Context(), "", outputs["Ref"], "", 0)
	require.NoError(t, err)
	require.Len(t, found, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("efs-ap-stack")})
	require.NoError(t, err)

	_, _, err = backends.EFS.Backend.DescribeAccessPoints(t.Context(), "", outputs["Ref"], "", 0)
	require.Error(t, err)
}

func TestCreateStack_RedshiftMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testRedshiftClusterParameterGroup, "cluster_parameter_group"},
		{testRedshiftClusterSubnetGroup, "cluster_subnet_group"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testRedshiftClusterParameterGroup(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"PG": {"Type": "AWS::Redshift::ClusterParameterGroup", "Properties": {
  "ParameterGroupName": "my-param-group",
  "ParameterGroupFamily": "redshift-1.0",
  "Description": "test parameter group"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "PG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "rs-pg-stack", tmpl)
	assert.Equal(t, "my-param-group", outputs["Ref"])

	_, err := backends.Redshift.Backend.DescribeClusterParameterGroups(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rs-pg-stack")})
	require.NoError(t, err)

	_, err = backends.Redshift.Backend.DescribeClusterParameterGroups(outputs["Ref"])
	require.Error(t, err)
}

func testRedshiftClusterSubnetGroup(t *testing.T) {
	t.Helper()

	backends, client := newSweepTestClient(t)

	tmpl := `{
"Resources": {"SG": {"Type": "AWS::Redshift::ClusterSubnetGroup", "Properties": {
  "ClusterSubnetGroupName": "my-subnet-group",
  "Description": "test subnet group",
  "SubnetIds": ["subnet-aaaa", "subnet-bbbb"]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "SG"}},
  "Name": {"Value": {"Fn::GetAtt": ["SG", "ClusterSubnetGroupName"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "rs-sg-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Name"])

	_, _, err := backends.Redshift.Backend.DescribeClusterSubnetGroups(outputs["Ref"], "", 0, nil, nil)
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rs-sg-stack")})
	require.NoError(t, err)

	_, _, err = backends.Redshift.Backend.DescribeClusterSubnetGroups(outputs["Ref"], "", 0, nil, nil)
	require.Error(t, err)
}
