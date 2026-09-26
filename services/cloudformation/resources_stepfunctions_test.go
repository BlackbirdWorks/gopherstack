package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func newSFNMoreTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newExtendedServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_StepFunctionsVersionAndAlias(t *testing.T) {
	t.Parallel()

	backends, client := newSFNMoreTestClient(t)

	tmpl := `{
"Resources": {
  "SM": {
    "Type": "AWS::StepFunctions::StateMachine",
    "Properties": {
      "StateMachineName": "sfn-more-sm",
      "DefinitionString": "{\"StartAt\":\"Done\",\"States\":{\"Done\":{\"Type\":\"Succeed\"}}}",
      "RoleArn": "arn:aws:iam::000000000000:role/sfn-role"
    }
  },
  "VersionA": {
    "Type": "AWS::StepFunctions::StateMachineVersion",
    "Properties": {"StateMachineArn": {"Ref": "SM"}, "Description": "v1"}
  },
  "Alias": {
    "Type": "AWS::StepFunctions::StateMachineAlias",
    "Properties": {
      "Name": "PROD",
      "RoutingConfiguration": [
        {"StateMachineVersionArn": {"Ref": "VersionA"}, "Weight": 100}
      ]
    }
  }
},
"Outputs": {
  "SMArn": {"Value": {"Ref": "SM"}},
  "VersionArn": {"Value": {"Ref": "VersionA"}},
  "VersionArnAttr": {"Value": {"Fn::GetAtt": ["VersionA", "Arn"]}},
  "AliasArn": {"Value": {"Ref": "Alias"}},
  "AliasArnAttr": {"Value": {"Fn::GetAtt": ["Alias", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sfn-version-alias-stack", tmpl)

	assert.Equal(
		t, outputs["VersionArn"], outputs["VersionArnAttr"], "Ref and GetAtt Arn must match for StateMachineVersion",
	)
	assert.Contains(t, outputs["VersionArn"], "sfn-more-sm:1")
	assert.Equal(t, outputs["AliasArn"], outputs["AliasArnAttr"], "Ref and GetAtt Arn must match for StateMachineAlias")
	assert.Contains(t, outputs["AliasArn"], "sfn-more-sm:PROD")

	versions, _, err := backends.StepFunctions.Backend.ListStateMachineVersions(outputs["SMArn"], "", 10)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, "v1", versions[0].Description)

	alias, err := backends.StepFunctions.Backend.DescribeStateMachineAlias(outputs["AliasArn"])
	require.NoError(t, err)
	require.Len(t, alias.RoutingConfiguration, 1)
	assert.Equal(t, 100, alias.RoutingConfiguration[0].Weight)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sfn-version-alias-stack")})
	require.NoError(t, err)

	_, err = backends.StepFunctions.Backend.DescribeStateMachineAlias(outputs["AliasArn"])
	require.Error(t, err)

	versionsAfter, _, err := backends.StepFunctions.Backend.ListStateMachineVersions(outputs["SMArn"], "", 10)
	require.NoError(t, err)
	assert.Empty(t, versionsAfter)
}

func TestCreateStack_StepFunctionsVersionAlias_NilBackend(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	tmpl := `{
"Resources": {
  "VersionA": {
    "Type": "AWS::StepFunctions::StateMachineVersion",
    "Properties": {"StateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:missing"}
  },
  "Alias": {
    "Type": "AWS::StepFunctions::StateMachineAlias",
    "Properties": {"Name": "PROD"}
  }
},
"Outputs": {"V": {"Value": {"Ref": "VersionA"}}, "A": {"Value": {"Ref": "Alias"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sfn-version-alias-nil-stack", tmpl)
	assert.Empty(t, outputs["V"])
	assert.Empty(t, outputs["A"])
}
