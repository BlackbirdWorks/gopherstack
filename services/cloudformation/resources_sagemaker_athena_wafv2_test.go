package cloudformation_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// newWave8TestClient wires a real aws-sdk-go-v2 CloudFormation client against
// a backend with SageMaker, Athena, and WAFv2 (among others) wired to real
// in-memory service backends.
func newWave8TestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_Wave8Types(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testSageMakerModel, "sagemaker_model"},
		{testSageMakerEndpointConfigAndEndpoint, "sagemaker_endpoint_config_and_endpoint"},
		{testSageMakerNotebookInstance, "sagemaker_notebook_instance"},
		{testSageMakerNotebookInstanceLifecycleConfig, "sagemaker_notebook_lifecycle_config"},
		{testSageMakerCodeRepository, "sagemaker_code_repository"},
		{testSageMakerDomain, "sagemaker_domain"},
		{testSageMakerPipeline, "sagemaker_pipeline"},
		{testSageMakerModelPackageGroup, "sagemaker_model_package_group"},
		{testSageMakerFeatureGroup, "sagemaker_feature_group"},
		{testSageMakerProject, "sagemaker_project"},
		{testSageMakerWorkteam, "sagemaker_workteam"},
		{testSageMakerImageAndImageVersion, "sagemaker_image_and_image_version"},
		{testAthenaWorkGroup, "athena_workgroup"},
		{testAthenaDataCatalog, "athena_data_catalog"},
		{testAthenaNamedQuery, "athena_named_query"},
		{testAthenaPreparedStatement, "athena_prepared_statement"},
		{testAthenaCapacityReservation, "athena_capacity_reservation"},
		{testWAFv2WebACLAssociation, "wafv2_webacl_association"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testSageMakerModel(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"M": {"Type": "AWS::SageMaker::Model", "Properties": {
  "ModelName": "model-1",
  "ExecutionRoleArn": "arn:aws:iam::000000000000:role/sm-role",
  "PrimaryContainer": {"Image": "123.dkr.ecr.us-east-1.amazonaws.com/img:latest"}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "M"}},
  "Name": {"Value": {"Fn::GetAtt": ["M", "ModelName"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-model-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "model/model-1")
	assert.Equal(t, "model-1", outputs["Name"])

	_, err := backends.SageMaker.Backend.DescribeModel(t.Context(), "model-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-model-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeModel(t.Context(), "model-1")
	require.Error(t, err)
}

func testSageMakerEndpointConfigAndEndpoint(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {
  "EC": {"Type": "AWS::SageMaker::EndpointConfig", "Properties": {
    "EndpointConfigName": "ec-1",
    "ProductionVariants": [{"VariantName": "v1", "ModelName": "any-model",
      "InstanceType": "ml.m5.large", "InitialInstanceCount": 1, "InitialVariantWeight": 1}]
  }},
  "EP": {"Type": "AWS::SageMaker::Endpoint", "DependsOn": "EC", "Properties": {
    "EndpointName": "ep-1", "EndpointConfigName": "ec-1"
  }}
},
"Outputs": {
  "ECRef": {"Value": {"Ref": "EC"}},
  "EPRef": {"Value": {"Ref": "EP"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-endpoint-stack", tmpl)
	assert.Contains(t, outputs["ECRef"], "endpoint-config/ec-1")
	assert.Contains(t, outputs["EPRef"], "endpoint/ep-1")

	_, err := backends.SageMaker.Backend.DescribeEndpoint(t.Context(), "ep-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-endpoint-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeEndpoint(t.Context(), "ep-1")
	require.Error(t, err)
	_, err = backends.SageMaker.Backend.DescribeEndpointConfig(t.Context(), "ec-1")
	require.Error(t, err)
}

func testSageMakerNotebookInstance(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"NB": {"Type": "AWS::SageMaker::NotebookInstance", "Properties": {
  "NotebookInstanceName": "nb-1", "InstanceType": "ml.t3.medium",
  "RoleArn": "arn:aws:iam::000000000000:role/sm-role"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "NB"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-nb-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "notebook-instance/nb-1")

	_, err := backends.SageMaker.Backend.DescribeNotebookInstance(t.Context(), "nb-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-nb-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeNotebookInstance(t.Context(), "nb-1")
	require.Error(t, err)
}

func testSageMakerNotebookInstanceLifecycleConfig(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"LC": {"Type": "AWS::SageMaker::NotebookInstanceLifecycleConfig", "Properties": {
  "NotebookInstanceLifecycleConfigName": "lc-1",
  "OnCreate": [{"Content": "ZWNobyBoaQ=="}]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "LC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-lc-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "notebook-instance-lifecycle-config/lc-1")

	_, err := backends.SageMaker.Backend.DescribeNotebookInstanceLifecycleConfig(t.Context(), "lc-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-lc-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeNotebookInstanceLifecycleConfig(t.Context(), "lc-1")
	require.Error(t, err)
}

func testSageMakerCodeRepository(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"CR": {"Type": "AWS::SageMaker::CodeRepository", "Properties": {
  "CodeRepositoryName": "cr-1",
  "GitConfig": {"RepositoryUrl": "https://github.com/example/repo.git"}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "CR"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-cr-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "code-repository/cr-1")

	_, err := backends.SageMaker.Backend.DescribeCodeRepository(t.Context(), "cr-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-cr-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeCodeRepository(t.Context(), "cr-1")
	require.Error(t, err)
}

func testSageMakerDomain(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"D": {"Type": "AWS::SageMaker::Domain", "Properties": {
  "DomainName": "domain-1", "AuthMode": "IAM", "VpcId": "vpc-test",
  "SubnetIds": ["subnet-1", "subnet-2"]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "D"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-domain-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "d-")

	_, err := backends.SageMaker.Backend.DescribeDomain(t.Context(), outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-domain-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeDomain(t.Context(), outputs["Ref"])
	require.Error(t, err)
}

func testSageMakerPipeline(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"P": {"Type": "AWS::SageMaker::Pipeline", "Properties": {
  "PipelineName": "pipeline-1",
  "RoleArn": "arn:aws:iam::000000000000:role/sm-role",
  "PipelineDefinition": {"PipelineDefinitionBody": "{\"Steps\": []}"}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "P"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-pipeline-stack", tmpl)
	assert.Equal(t, "pipeline-1", outputs["Ref"])

	_, _, _, err := backends.SageMaker.Backend.DescribePipeline(t.Context(), "pipeline-1", 0)
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-pipeline-stack")})
	require.NoError(t, err)

	_, _, _, err = backends.SageMaker.Backend.DescribePipeline(t.Context(), "pipeline-1", 0)
	require.Error(t, err)
}

func testSageMakerModelPackageGroup(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"G": {"Type": "AWS::SageMaker::ModelPackageGroup", "Properties": {
  "ModelPackageGroupName": "mpg-1", "ModelPackageGroupDescription": "test group"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "G"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-mpg-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "model-package-group/mpg-1")

	_, err := backends.SageMaker.Backend.DescribeModelPackageGroup(t.Context(), "mpg-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-mpg-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeModelPackageGroup(t.Context(), "mpg-1")
	require.Error(t, err)
}

func testSageMakerFeatureGroup(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"FG": {"Type": "AWS::SageMaker::FeatureGroup", "Properties": {
  "FeatureGroupName": "fg-1", "RecordIdentifierFeatureName": "id",
  "EventTimeFeatureName": "event_time",
  "RoleArn": "arn:aws:iam::000000000000:role/sm-role",
  "FeatureDefinitions": [{"FeatureName": "id", "FeatureType": "String"},
    {"FeatureName": "event_time", "FeatureType": "String"}]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "FG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-fg-stack", tmpl)
	assert.Equal(t, "fg-1", outputs["Ref"])

	_, err := backends.SageMaker.Backend.DescribeFeatureGroup(t.Context(), "fg-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-fg-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeFeatureGroup(t.Context(), "fg-1")
	require.Error(t, err)
}

func testSageMakerProject(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"PR": {"Type": "AWS::SageMaker::Project", "Properties": {
  "ProjectName": "project-1", "ProjectDescription": "test project"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "PR"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-project-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "project/project-1")

	_, err := backends.SageMaker.Backend.DescribeProject(t.Context(), "project-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-project-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeProject(t.Context(), "project-1")
	require.Error(t, err)
}

func testSageMakerWorkteam(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"W": {"Type": "AWS::SageMaker::Workteam", "Properties": {
  "WorkteamName": "workteam-1",
  "MemberDefinitions": [{"CognitoMemberDefinition": {
    "CognitoUserPool": "pool-1", "CognitoUserGroup": "group-1", "CognitoClientId": "client-1"
  }}]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "W"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-workteam-stack", tmpl)
	assert.Equal(t, "workteam-1", outputs["Ref"])

	wt, err := backends.SageMaker.Backend.DescribeWorkteam(t.Context(), "workteam-1")
	require.NoError(t, err)
	require.Len(t, wt.MemberDefinitions, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-workteam-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeWorkteam(t.Context(), "workteam-1")
	require.Error(t, err)
}

func testSageMakerImageAndImageVersion(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {
  "Img": {"Type": "AWS::SageMaker::Image", "Properties": {
    "ImageName": "image-1", "ImageRoleArn": "arn:aws:iam::000000000000:role/sm-role"
  }},
  "ImgV": {"Type": "AWS::SageMaker::ImageVersion", "DependsOn": "Img", "Properties": {
    "ImageName": "image-1",
    "BaseImage": "123.dkr.ecr.us-east-1.amazonaws.com/base:latest"
  }}
},
"Outputs": {
  "ImgRef": {"Value": {"Ref": "Img"}},
  "ImgVRef": {"Value": {"Ref": "ImgV"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sm-image-stack", tmpl)
	assert.Contains(t, outputs["ImgRef"], "image/image-1")
	assert.Contains(t, outputs["ImgVRef"], "image-version/image-1/1")

	_, err := backends.SageMaker.Backend.DescribeImageVersion(t.Context(), "image-1", "", 1)
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sm-image-stack")})
	require.NoError(t, err)

	_, err = backends.SageMaker.Backend.DescribeImage(t.Context(), "image-1")
	require.Error(t, err)
}

func testAthenaWorkGroup(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"WG": {"Type": "AWS::Athena::WorkGroup", "Properties": {
  "Name": "workgroup-1", "Description": "test workgroup",
  "WorkGroupConfiguration": {"ResultConfiguration": {"OutputLocation": "s3://bucket/prefix/"}}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "WG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "athena-wg-stack", tmpl)
	assert.Equal(t, "workgroup-1", outputs["Ref"])

	wg, err := backends.Athena.Backend.GetWorkGroup("workgroup-1")
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/prefix/", wg.Configuration.ResultConfiguration.OutputLocation)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("athena-wg-stack")})
	require.NoError(t, err)

	_, err = backends.Athena.Backend.GetWorkGroup("workgroup-1")
	require.Error(t, err)
}

func testAthenaDataCatalog(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"DC": {"Type": "AWS::Athena::DataCatalog", "Properties": {
  "Name": "catalog-1", "Type": "GLUE", "Description": "test catalog",
  "Parameters": {"catalog-id": "000000000000"}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "DC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "athena-dc-stack", tmpl)
	assert.Equal(t, "catalog-1", outputs["Ref"])

	_, err := backends.Athena.Backend.GetDataCatalog("catalog-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("athena-dc-stack")})
	require.NoError(t, err)

	_, err = backends.Athena.Backend.GetDataCatalog("catalog-1")
	require.Error(t, err)
}

func testAthenaNamedQuery(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"NQ": {"Type": "AWS::Athena::NamedQuery", "Properties": {
  "Name": "query-1", "Database": "default", "QueryString": "SELECT 1"
}}},
"Outputs": {
  "Id": {"Value": {"Ref": "NQ"}},
  "NamedQueryId": {"Value": {"Fn::GetAtt": ["NQ", "NamedQueryId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "athena-nq-stack", tmpl)
	assert.NotEmpty(t, outputs["Id"])
	assert.Equal(t, outputs["Id"], outputs["NamedQueryId"])

	_, err := backends.Athena.Backend.GetNamedQuery(outputs["Id"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("athena-nq-stack")})
	require.NoError(t, err)

	_, err = backends.Athena.Backend.GetNamedQuery(outputs["Id"])
	require.Error(t, err)
}

func testAthenaPreparedStatement(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {
  "WG": {"Type": "AWS::Athena::WorkGroup", "Properties": {"Name": "ps-wg"}},
  "PS": {"Type": "AWS::Athena::PreparedStatement", "Properties": {
    "StatementName": "stmt-1", "WorkGroup": {"Ref": "WG"}, "QueryStatement": "SELECT ?"
  }}
},
"Outputs": {"Ref": {"Value": {"Ref": "PS"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "athena-ps-stack", tmpl)
	assert.Equal(t, "stmt-1", outputs["Ref"])

	_, err := backends.Athena.Backend.GetPreparedStatement("stmt-1", "ps-wg")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("athena-ps-stack")})
	require.NoError(t, err)

	_, err = backends.Athena.Backend.GetPreparedStatement("stmt-1", "ps-wg")
	require.Error(t, err)
}

func testAthenaCapacityReservation(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	tmpl := `{
"Resources": {"CRes": {"Type": "AWS::Athena::CapacityReservation", "Properties": {
  "Name": "capres-1", "TargetDpus": 24
}}},
"Outputs": {"Ref": {"Value": {"Ref": "CRes"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "athena-capres-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "capacity-reservation/capres-1")

	_, err := backends.Athena.Backend.GetCapacityReservation("capres-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("athena-capres-stack")})
	require.NoError(t, err)

	_, err = backends.Athena.Backend.GetCapacityReservation("capres-1")
	require.Error(t, err)
}

func testWAFv2WebACLAssociation(t *testing.T) {
	t.Helper()

	backends, client := newWave8TestClient(t)

	// AWS::WAFv2::WebACL's existing Ref (resources_wafv2.go, out of this
	// pass's scope) returns the WebACL's bare ID, not its ARN -- but
	// AssociateWebACL needs a real ARN. The WebACL is created directly
	// against the backend here (mirroring a cross-stack import) so the
	// association template below can reference its real ARN.
	acl, err := backends.WAFv2.Backend.CreateWebACL(
		t.Context(), "acl-1", "REGIONAL", "",
		json.RawMessage(`{"Allow":{}}`), nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil,
	)
	require.NoError(t, err)

	tmpl := fmt.Sprintf(`{
"Resources": {
  "Assoc": {"Type": "AWS::WAFv2::WebACLAssociation", "Properties": {
    "ResourceArn": "arn:aws:apigateway:us-east-1::/restapis/abc123/stages/prod",
    "WebACLArn": %q
  }}
},
"Outputs": {"Ref": {"Value": {"Ref": "Assoc"}}}
}`, acl.ARN)

	outputs := createStackAndGetOutputs(t, client, "wafv2-assoc-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "|")

	webACL, err := backends.WAFv2.Backend.GetWebACLForResource(
		t.Context(), "arn:aws:apigateway:us-east-1::/restapis/abc123/stages/prod",
	)
	require.NoError(t, err)
	require.NotNil(t, webACL)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("wafv2-assoc-stack")})
	require.NoError(t, err)

	_, err = backends.WAFv2.Backend.GetWebACLForResource(
		t.Context(), "arn:aws:apigateway:us-east-1::/restapis/abc123/stages/prod",
	)
	require.Error(t, err)
}
