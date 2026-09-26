package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_APIGatewayV2VpcLink(t *testing.T) {
	t.Parallel()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "Link": {
    "Type": "AWS::ApiGatewayV2::VpcLink",
    "Properties": {"Name": "unit-vpc-link", "SubnetIds": [{"Ref": "Subnet"}]}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Link"}},
  "Id": {"Value": {"Fn::GetAtt": ["Link", "VpcLinkId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "apigwv2-vpclink-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])

	link, err := backends.APIGatewayV2.Backend.GetVpcLink(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "unit-vpc-link", link.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("apigwv2-vpclink-stack")})
	require.NoError(t, err)

	_, err = backends.APIGatewayV2.Backend.GetVpcLink(outputs["Ref"])
	require.Error(t, err)
}
