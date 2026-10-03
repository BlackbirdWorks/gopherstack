package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2NetworkInsightsPath(t *testing.T) {
	t.Parallel()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "Source": {"Type": "AWS::EC2::NetworkInterface", "Properties": {"SubnetId": {"Ref": "Subnet"}}},
  "Dest": {"Type": "AWS::EC2::NetworkInterface", "Properties": {"SubnetId": {"Ref": "Subnet"}}},
  "Path": {
    "Type": "AWS::EC2::NetworkInsightsPath",
    "Properties": {
      "Source": {"Ref": "Source"},
      "Destination": {"Ref": "Dest"},
      "Protocol": "tcp",
      "DestinationPort": 443
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Path"}},
  "Id": {"Value": {"Fn::GetAtt": ["Path", "NetworkInsightsPathId"]}},
  "Arn": {"Value": {"Fn::GetAtt": ["Path", "NetworkInsightsPathArn"]}},
  "SourceArn": {"Value": {"Fn::GetAtt": ["Path", "SourceArn"]}},
  "DestinationArn": {"Value": {"Fn::GetAtt": ["Path", "DestinationArn"]}},
  "SourceRef": {"Value": {"Ref": "Source"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-nip-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])
	assert.Contains(t, outputs["Arn"], "network-insights-path/"+outputs["Ref"])
	assert.Equal(t, outputs["SourceRef"], outputs["SourceArn"])
	require.Len(t, backends.EC2.Backend.DescribeNetworkInsightsPaths([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-nip-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribeNetworkInsightsPaths([]string{outputs["Ref"]}))
}
