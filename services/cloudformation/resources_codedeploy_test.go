package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// newCodeDeployTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend with CodeDeploy wired to a real in-memory backend, for
// tests that provision through the actual CreateStack/DeleteStack path.
func newCodeDeployTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newDependentServiceBackends(t)
	backends.CodeDeploy = codedeploybackend.NewHandler(
		codedeploybackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_CodeDeployApplicationAndDeploymentGroup(t *testing.T) {
	t.Parallel()

	backends, client := newCodeDeployTestClient(t)

	tmpl := `{
"Resources": {
  "App": {
    "Type": "AWS::CodeDeploy::Application",
    "Properties": {"ApplicationName": "my-app", "ComputePlatform": "Server"}
  },
  "Config": {
    "Type": "AWS::CodeDeploy::DeploymentConfig",
    "Properties": {
      "DeploymentConfigName": "my-config",
      "MinimumHealthyHosts": {"Type": "FLEET_PERCENT", "Value": 75}
    }
  },
  "Group": {
    "Type": "AWS::CodeDeploy::DeploymentGroup",
    "Properties": {
      "ApplicationName": {"Ref": "App"},
      "DeploymentGroupName": "my-group",
      "ServiceRoleArn": "arn:aws:iam::000000000000:role/CodeDeployRole",
      "DeploymentConfigName": {"Ref": "Config"}
    }
  }
},
"Outputs": {
  "AppId": {"Value": {"Ref": "App"}},
  "ConfigId": {"Value": {"Ref": "Config"}},
  "GroupId": {"Value": {"Ref": "Group"}}
}
}`

	stackName := "codedeploy-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

	assert.Equal(t, "my-app", outputs["AppId"])
	assert.Equal(t, "my-config", outputs["ConfigId"])
	assert.Equal(t, "my-group", outputs["GroupId"], "Ref must be the bare DeploymentGroupName per CFN docs")

	mem := backends.CodeDeploy.Backend

	app, err := mem.GetApplication("my-app")
	require.NoError(t, err)
	assert.Equal(t, "Server", app.ComputePlatform)

	cfg, err := mem.GetDeploymentConfig("my-config")
	require.NoError(t, err)
	require.NotNil(t, cfg.MinimumHealthyHosts)
	assert.Equal(t, "FLEET_PERCENT", cfg.MinimumHealthyHosts.Type)
	assert.Equal(t, 75, cfg.MinimumHealthyHosts.Value)

	dg, err := mem.GetDeploymentGroup("my-app", "my-group")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/CodeDeployRole", dg.ServiceRoleArn)
	assert.Equal(t, "my-config", dg.DeploymentConfigName)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	_, err = mem.GetDeploymentGroup("my-app", "my-group")
	require.Error(t, err)
	_, err = mem.GetDeploymentConfig("my-config")
	require.Error(t, err)
	_, err = mem.GetApplication("my-app")
	require.Error(t, err)
}

func TestCreateStack_CodeDeployNilBackend(t *testing.T) {
	t.Parallel()

	backends := newDependentServiceBackends(t)
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	tests := []struct {
		name string
		tmpl string
	}{
		{
			name: "application",
			tmpl: `{"Resources":{"App":{"Type":"AWS::CodeDeploy::Application","Properties":{}}},
"Outputs":{"Id":{"Value":{"Ref":"App"}}}}`,
		},
		{
			name: "deployment_config",
			tmpl: `{"Resources":{"Cfg":{"Type":"AWS::CodeDeploy::DeploymentConfig","Properties":{}}},
"Outputs":{"Id":{"Value":{"Ref":"Cfg"}}}}`,
		},
		{
			name: "deployment_group",
			tmpl: `{"Resources":{"Grp":{"Type":"AWS::CodeDeploy::DeploymentGroup",
"Properties":{"ApplicationName":"app","DeploymentGroupName":"grp"}}},
"Outputs":{"Id":{"Value":{"Ref":"Grp"}}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			outputs := createStackAndGetOutputs(t, client, "codedeploy-nil-"+tt.name, tt.tmpl)
			assert.NotEmpty(t, outputs["Id"])
		})
	}
}
