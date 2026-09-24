package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_APIGatewayMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testAPIGatewayVpcLink, "vpc_link"},
		{testAPIGatewayClientCertificate, "client_certificate"},
		{testAPIGatewayDocumentationPart, "documentation_part"},
		{testAPIGatewayDocumentationVersion, "documentation_version"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testAPIGatewayVpcLink(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VL": {
    "Type": "AWS::ApiGateway::VpcLink",
    "Properties": {
      "Name": "my-vpc-link",
      "TargetArns": ["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/my-nlb/abc"]
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "VL"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "vl-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])

	link, err := backends.APIGateway.Backend.GetVpcLink(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "my-vpc-link", link.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("vl-stack")})
	require.NoError(t, err)

	_, err = backends.APIGateway.Backend.GetVpcLink(outputs["Ref"])
	require.Error(t, err)
}

func testAPIGatewayClientCertificate(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Cert": {"Type": "AWS::ApiGateway::ClientCertificate", "Properties": {"Description": "test cert"}}
},
"Outputs": {"Ref": {"Value": {"Ref": "Cert"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "cert-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])

	cert, err := backends.APIGateway.Backend.GetClientCertificate(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "test cert", cert.Description)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("cert-stack")})
	require.NoError(t, err)

	_, err = backends.APIGateway.Backend.GetClientCertificate(outputs["Ref"])
	require.Error(t, err)
}

func testAPIGatewayDocumentationPart(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Api": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": "doc-api"}},
  "Part": {
    "Type": "AWS::ApiGateway::DocumentationPart",
    "Properties": {
      "RestApiId": {"Ref": "Api"},
      "Location": {"Type": "API"},
      "Properties": "{\"description\": \"my api\"}"
    }
  }
},
"Outputs": {"ApiRef": {"Value": {"Ref": "Api"}}, "PartRef": {"Value": {"Ref": "Part"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "docpart-stack", tmpl)
	require.NotEmpty(t, outputs["PartRef"])

	part, err := backends.APIGateway.Backend.GetDocumentationPart(outputs["ApiRef"], outputs["PartRef"])
	require.NoError(t, err)
	assert.Equal(t, "API", part.Location.Type)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("docpart-stack")})
	require.NoError(t, err)

	_, err = backends.APIGateway.Backend.GetDocumentationPart(outputs["ApiRef"], outputs["PartRef"])
	require.Error(t, err)
}

func testAPIGatewayDocumentationVersion(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Api": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": "docver-api"}},
  "Ver": {
    "Type": "AWS::ApiGateway::DocumentationVersion",
    "Properties": {
      "RestApiId": {"Ref": "Api"},
      "DocumentationVersion": "1.0.0",
      "Description": "first release"
    }
  }
},
"Outputs": {"ApiRef": {"Value": {"Ref": "Api"}}, "VerRef": {"Value": {"Ref": "Ver"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "docver-stack", tmpl)
	assert.Equal(t, "1.0.0", outputs["VerRef"])

	ver, err := backends.APIGateway.Backend.GetDocumentationVersion(outputs["ApiRef"], "1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "first release", ver.Description)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("docver-stack")})
	require.NoError(t, err)

	_, err = backends.APIGateway.Backend.GetDocumentationVersion(outputs["ApiRef"], "1.0.0")
	require.Error(t, err)
}
