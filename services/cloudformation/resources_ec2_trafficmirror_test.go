package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2TrafficMirrorTypes(t *testing.T) {
	t.Parallel()
	t.Run("filter_rule_target_session", testEC2TrafficMirrorChain)
}

func testEC2TrafficMirrorChain(t *testing.T) {
	t.Parallel()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "ENI": {"Type": "AWS::EC2::NetworkInterface", "Properties": {"SubnetId": {"Ref": "Subnet"}}},
  "Filter": {"Type": "AWS::EC2::TrafficMirrorFilter", "Properties": {"Description": "unit filter"}},
  "Rule": {
    "Type": "AWS::EC2::TrafficMirrorFilterRule",
    "Properties": {
      "TrafficMirrorFilterId": {"Ref": "Filter"},
      "TrafficDirection": "ingress",
      "RuleAction": "accept",
      "SourceCidrBlock": "0.0.0.0/0",
      "DestinationCidrBlock": "0.0.0.0/0",
      "RuleNumber": 1,
      "Protocol": 6
    }
  },
  "Target": {
    "Type": "AWS::EC2::TrafficMirrorTarget",
    "Properties": {"NetworkInterfaceId": {"Ref": "ENI"}, "Description": "unit target"}
  },
  "Session": {
    "Type": "AWS::EC2::TrafficMirrorSession",
    "Properties": {
      "NetworkInterfaceId": {"Ref": "ENI"},
      "TrafficMirrorTargetId": {"Ref": "Target"},
      "TrafficMirrorFilterId": {"Ref": "Filter"},
      "SessionNumber": 1
    }
  }
},
"Outputs": {
  "FilterRef": {"Value": {"Ref": "Filter"}},
  "RuleRef": {"Value": {"Ref": "Rule"}},
  "RuleId": {"Value": {"Fn::GetAtt": ["Rule", "TrafficMirrorFilterRuleId"]}},
  "TargetRef": {"Value": {"Ref": "Target"}},
  "SessionRef": {"Value": {"Ref": "Session"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-tm-stack", tmpl)
	assert.Contains(t, outputs["FilterRef"], "tmf-")
	assert.Equal(t, outputs["RuleRef"], outputs["RuleId"])
	assert.Contains(t, outputs["TargetRef"], "tmt-")
	assert.Contains(t, outputs["SessionRef"], "tms-")

	require.Len(t, backends.EC2.Backend.DescribeTrafficMirrorFilters([]string{outputs["FilterRef"]}), 1)

	rules, err := backends.EC2.Backend.DescribeTrafficMirrorFilterRules(outputs["FilterRef"])
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, outputs["RuleRef"], rules[0].TrafficMirrorFilterRuleID)

	require.Len(t, backends.EC2.Backend.DescribeTrafficMirrorTargets([]string{outputs["TargetRef"]}), 1)
	require.Len(t, backends.EC2.Backend.DescribeTrafficMirrorSessions([]string{outputs["SessionRef"]}), 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-tm-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeTrafficMirrorSessions([]string{outputs["SessionRef"]}))
	assert.Empty(t, backends.EC2.Backend.DescribeTrafficMirrorTargets([]string{outputs["TargetRef"]}))
	assert.Empty(t, backends.EC2.Backend.DescribeTrafficMirrorFilters([]string{outputs["FilterRef"]}))
}
