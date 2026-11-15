package cloudformation_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdawebbackend "github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const lambdaWebTemplate = `{
"Resources": {
  "Fn": {"Type": "AWS::Lambda::WebFunction", "Properties": {
    "FunctionName": "cfn-web", "Tags": [{"Key": "env", "Value": "dev"}]}},
  "Rev": {"Type": "AWS::Lambda::WebFunctionRevision", "Properties": {
    "FunctionName": {"Ref": "Fn"}, "Description": "first",
    "BuildConfig": {
      "CodeConfig": {"S3Object": {"Bucket": "code", "Key": "app.zip"}},
      "RuntimeConfig": {"Runtime": "nodejs24.x"}},
    "ServiceConfig": {
      "ExecutionRoleArn": "arn:aws:iam::000000000000:role/web-role",
      "TimeoutSeconds": 20,
      "EnvironmentVariables": {"MODE": "test"},
      "TelemetryConfig": {"LoggingConfig": {"ApplicationLogLevel": "DEBUG"}}}}},
  "Ep": {"Type": "AWS::Lambda::WebFunctionEndpoint", "Properties": {
    "FunctionName": {"Ref": "Fn"}, "EndpointName": "main", "EndpointType": "HomeRegion",
    "AuthType": "IamAuth", "Description": "DESC",
    "RevisionWeights": [{"RevisionId": {"Fn::GetAtt": ["Rev", "RevisionId"]}, "Weight": 100}]}}
},
"Outputs": {
  "FnRef": {"Value": {"Ref": "Fn"}},
  "FnArn": {"Value": {"Fn::GetAtt": ["Fn", "FunctionArn"]}},
  "FnState": {"Value": {"Fn::GetAtt": ["Fn", "State"]}},
  "RevRef": {"Value": {"Ref": "Rev"}},
  "RevId": {"Value": {"Fn::GetAtt": ["Rev", "RevisionId"]}},
  "RevArn": {"Value": {"Fn::GetAtt": ["Rev", "RevisionArn"]}},
  "EpRef": {"Value": {"Ref": "Ep"}},
  "EpDomain": {"Value": {"Fn::GetAtt": ["Ep", "DomainName"]}},
  "EpState": {"Value": {"Fn::GetAtt": ["Ep", "State"]}}
}
}`

func newLambdaWebTestClient(t *testing.T) (*lambdawebbackend.InMemoryBackend, *cfnsdk.Client) {
	t.Helper()

	lw := lambdawebbackend.NewInMemoryBackend("000000000000")
	lw.SetLifecycleDelay(time.Millisecond)

	backends := newMoreTypesServiceBackends(t)
	backends.Region = "us-east-1"
	backends.LambdaWeb = lambdawebbackend.NewHandler(lw)

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	return lw, newTestClientForBackend(t, backend)
}

func TestCreateStack_LambdaWebFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, lw *lambdawebbackend.InMemoryBackend, client *cfnsdk.Client)
		name string
	}{
		{name: "create_attributes_and_delete", run: lambdaWebCreateDelete},
		{name: "update_in_place", run: lambdaWebUpdate},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lw, client := newLambdaWebTestClient(t)
			tc.run(t, lw, client)
		})
	}
}

func lambdaWebCreateDelete(t *testing.T, lw *lambdawebbackend.InMemoryBackend, client *cfnsdk.Client) {
	t.Helper()

	out := createStackAndGetOutputs(t, client, "lw-stack", lambdaWebTemplate)

	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:web-function/cfn-web", out["FnRef"])
	assert.Equal(t, out["FnRef"], out["FnArn"])
	assert.Equal(t, "Active", out["FnState"])
	assert.Equal(t, out["RevArn"], out["RevRef"])
	assert.NotEmpty(t, out["RevId"])
	assert.Contains(t, out["EpRef"], "web-function/cfn-web/endpoint/main")
	assert.NotEmpty(t, out["EpDomain"])
	assert.Equal(t, "Active", out["EpState"])

	rev, err := lw.GetWebFunctionRevision("us-east-1", "cfn-web", out["RevId"])
	require.NoError(t, err)
	assert.Equal(t, int32(20), rev.ServiceConfig.TimeoutSeconds)
	assert.Equal(t, "test", rev.ServiceConfig.EnvironmentVariables["MODE"])
	assert.Equal(t, "DEBUG", rev.ServiceConfig.Logging.ApplicationLogLevel)

	ep, err := lw.GetWebFunctionEndpoint("us-east-1", "cfn-web", "main")
	require.NoError(t, err)
	assert.Equal(t, "IamAuth", ep.AuthType)
	assert.Equal(t, []lambdawebbackend.RevisionWeight{{RevisionID: out["RevId"], Weight: 100}}, ep.Weights)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("lw-stack")})
	require.NoError(t, err)

	_, err = lw.GetWebFunction("us-east-1", "cfn-web")
	require.Error(t, err)
}

func lambdaWebUpdate(t *testing.T, lw *lambdawebbackend.InMemoryBackend, client *cfnsdk.Client) {
	t.Helper()

	createStackAndGetOutputs(t, client, "lw-update", lambdaWebTemplate)

	updated := strings.ReplaceAll(lambdaWebTemplate, `"DESC"`, `"changed"`)
	updated = strings.ReplaceAll(updated, `{"Key": "env", "Value": "dev"}`, `{"Key": "env", "Value": "prod"}`)

	_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("lw-update"), TemplateBody: aws.String(updated),
	})
	require.NoError(t, err)

	ep, err := lw.GetWebFunctionEndpoint("us-east-1", "cfn-web", "main")
	require.NoError(t, err)
	assert.Equal(t, "changed", ep.Description)

	tags, err := lw.ListTags("arn:aws:lambda:us-east-1:000000000000:web-function/cfn-web")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tags)
}
