package cloudformation_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// newManagedTypesTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend whose ResourceCreator has EC2, IAM, RDS, and AutoScaling
// (among others) wired to real in-memory service backends, for tests that
// provision through the actual CreateStack/DescribeStackResources/DeleteStack
// path rather than calling ResourceCreator directly.
func newManagedTypesTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newDependentServiceBackends(t)
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

// createStackAndGetOutputs creates a stack from template and returns its
// Outputs as key->value pairs, asserting CREATE_COMPLETE.
func createStackAndGetOutputs(
	t *testing.T, client *cfnsdk.Client, stackName, template string, capabilities ...cfntypes.Capability,
) map[string]string {
	t.Helper()

	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
		Capabilities: capabilities,
	})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, "CREATE_COMPLETE", string(desc.Stacks[0].StackStatus))

	out := make(map[string]string, len(desc.Stacks[0].Outputs))
	for _, o := range desc.Stacks[0].Outputs {
		out[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	return out
}

func TestCreateStack_EC2LaunchTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		instanceType string
	}{
		{name: "t3_micro", instanceType: "t3.micro"},
		{name: "m5_large", instanceType: "m5.large"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends, client := newManagedTypesTestClient(t)

			tmpl := fmt.Sprintf(`{
"Resources": {
  "LT": {
    "Type": "AWS::EC2::LaunchTemplate",
    "Properties": {
      "LaunchTemplateName": "lt-%[1]s",
      "LaunchTemplateData": {"ImageId": "ami-12345678", "InstanceType": "%[1]s"},
      "TagSpecifications": [{"ResourceType": "launch-template",
        "Tags": [{"Key": "Name", "Value": "lt-tag-%[1]s"}]}]
    }
  }
},
"Outputs": {
  "Id": {"Value": {"Ref": "LT"}},
  "DefaultVer": {"Value": {"Fn::GetAtt": ["LT", "DefaultVersionNumber"]}},
  "LatestVer": {"Value": {"Fn::GetAtt": ["LT", "LatestVersionNumber"]}}
}
}`, tt.instanceType)

			stackName := "lt-stack-" + tt.name
			outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

			ltID := outputs["Id"]
			require.NotEmpty(t, ltID)
			assert.Equal(t, "1", outputs["DefaultVer"])
			assert.Equal(t, "1", outputs["LatestVer"])

			lts := backends.EC2.Backend.DescribeLaunchTemplates(nil)
			require.Len(t, lts, 1)
			assert.Equal(t, ltID, lts[0].ID)
			assert.Equal(t, tt.instanceType, lts[0].InstanceType)
			assert.Equal(t, "ami-12345678", lts[0].ImageID)
			assert.Equal(t, "lt-tag-"+tt.instanceType, backends.EC2.Backend.TagsForResource(ltID)["Name"])

			_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
			require.NoError(t, err)
			assert.Empty(t, backends.EC2.Backend.DescribeLaunchTemplates(nil))
		})
	}
}

func TestCreateStack_EC2VPCEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointType string
	}{
		{name: "gateway", endpointType: "Gateway"},
		{name: "interface", endpointType: "Interface"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends, client := newManagedTypesTestClient(t)

			tmpl := fmt.Sprintf(`{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Endpoint": {
    "Type": "AWS::EC2::VPCEndpoint",
    "DependsOn": "VPC",
    "Properties": {
      "VpcId": {"Ref": "VPC"},
      "ServiceName": "com.amazonaws.us-east-1.s3",
      "VpcEndpointType": "%s",
      "Tags": [{"Key": "Name", "Value": "vpce-tag"}]
    }
  }
},
"Outputs": {"Id": {"Value": {"Ref": "Endpoint"}}}
}`, tt.endpointType)

			stackName := "vpce-stack-" + tt.name
			outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

			epID := outputs["Id"]
			require.NotEmpty(t, epID)

			eps := backends.EC2.Backend.DescribeVpcEndpoints(nil)
			require.Len(t, eps, 1)
			assert.Equal(t, epID, eps[0].ID)
			assert.Equal(t, "com.amazonaws.us-east-1.s3", eps[0].ServiceName)
			assert.Equal(t, tt.endpointType, eps[0].VpcEndpointType)
			assert.Equal(t, "vpce-tag", backends.EC2.Backend.TagsForResource(epID)["Name"])

			_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
			require.NoError(t, err)
			assert.Empty(t, backends.EC2.Backend.DescribeVpcEndpoints(nil))
		})
	}
}

func TestCreateStack_AutoScalingScalingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType string
		extraProps string
	}{
		{
			name: "simple_scaling", policyType: "SimpleScaling",
			extraProps: `"AdjustmentType": "ChangeInCapacity", "ScalingAdjustment": 1`,
		},
		{
			name: "target_tracking", policyType: "TargetTrackingScaling",
			extraProps: `"TargetTrackingConfiguration": {"PredefinedMetricSpecification":
				{"PredefinedMetricType": "ASGAverageCPUUtilization"}, "TargetValue": 50}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends, client := newManagedTypesTestClient(t)

			tmpl := fmt.Sprintf(`{
"Resources": {
  "ASG": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {"MinSize": 1, "MaxSize": 3}},
  "Policy": {
    "Type": "AWS::AutoScaling::ScalingPolicy",
    "Properties": {"AutoScalingGroupName": {"Ref": "ASG"}, "PolicyType": "%s", %s}
  }
},
"Outputs": {
  "Arn": {"Value": {"Ref": "Policy"}},
  "PolicyName": {"Value": {"Fn::GetAtt": ["Policy", "PolicyName"]}}
}
}`, tt.policyType, tt.extraProps)

			stackName := "sp-stack-" + tt.name
			outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

			policyARN := outputs["Arn"]
			require.Contains(t, policyARN, "arn:aws:autoscaling:")
			assert.Contains(t, policyARN, outputs["PolicyName"])

			groups, err := backends.Autoscaling.Backend.DescribeAutoScalingGroups(nil, nil)
			require.NoError(t, err)
			require.Len(t, groups, 1)

			pols, err := backends.Autoscaling.Backend.DescribePolicies(groups[0].AutoScalingGroupName, nil, nil)
			require.NoError(t, err)
			require.Len(t, pols, 1)
			assert.Equal(t, tt.policyType, pols[0].PolicyType)
			assert.Equal(t, policyARN, pols[0].PolicyARN)

			_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
			require.NoError(t, err)

			pols, err = backends.Autoscaling.Backend.DescribePolicies(groups[0].AutoScalingGroupName, nil, nil)
			require.NoError(t, err)
			assert.Empty(t, pols)
		})
	}
}

func TestCreateStack_AutoScalingScheduledAction(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "ASG": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {"MinSize": 1, "MaxSize": 5}},
  "Action": {
    "Type": "AWS::AutoScaling::ScheduledAction",
    "Properties": {
      "AutoScalingGroupName": {"Ref": "ASG"},
      "MinSize": 1, "MaxSize": 5, "DesiredCapacity": 2,
      "Recurrence": "0 10 * * *"
    }
  }
},
"Outputs": {"Name": {"Value": {"Ref": "Action"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sched-stack", tmpl)
	actionName := outputs["Name"]
	require.NotEmpty(t, actionName)

	actions, err := backends.Autoscaling.Backend.DescribeScheduledActions(
		"", []string{actionName}, time.Time{}, time.Time{},
	)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, "0 10 * * *", actions[0].Recurrence)
	require.NotNil(t, actions[0].DesiredCapacity)
	assert.Equal(t, int32(2), *actions[0].DesiredCapacity)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sched-stack")})
	require.NoError(t, err)

	actions, err = backends.Autoscaling.Backend.DescribeScheduledActions(
		"", []string{actionName}, time.Time{}, time.Time{},
	)
	require.NoError(t, err)
	assert.Empty(t, actions)
}

func TestCreateStack_AutoScalingLifecycleHook(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "ASG": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {"MinSize": 1, "MaxSize": 3}},
  "Hook": {
    "Type": "AWS::AutoScaling::LifecycleHook",
    "Properties": {
      "AutoScalingGroupName": {"Ref": "ASG"},
      "LifecycleTransition": "autoscaling:EC2_INSTANCE_LAUNCHING",
      "HeartbeatTimeout": 300,
      "DefaultResult": "CONTINUE"
    }
  }
},
"Outputs": {"Name": {"Value": {"Ref": "Hook"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "hook-stack", tmpl)
	hookName := outputs["Name"]
	require.NotEmpty(t, hookName)

	groups, err := backends.Autoscaling.Backend.DescribeAutoScalingGroups(nil, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	hooks, err := backends.Autoscaling.Backend.DescribeLifecycleHooks(
		groups[0].AutoScalingGroupName, []string{hookName},
	)
	require.NoError(t, err)
	require.Len(t, hooks, 1)
	assert.Equal(t, "autoscaling:EC2_INSTANCE_LAUNCHING", hooks[0].LifecycleTransition)
	assert.Equal(t, int32(300), hooks[0].HeartbeatTimeout)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("hook-stack")})
	require.NoError(t, err)

	// The AutoScalingGroup itself is torn down along with the hook.
	remaining, err := backends.Autoscaling.Backend.DescribeAutoScalingGroups(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}

func TestCreateStack_IAMOIDCProvider(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Provider": {
    "Type": "AWS::IAM::OIDCProvider",
    "Properties": {
      "Url": "https://token.actions.githubusercontent.com",
      "ClientIdList": ["sts.amazonaws.com"],
      "ThumbprintList": ["6938fd4d98bab03faadb97b34396831e3780aea1"]
    }
  }
},
"Outputs": {"Arn": {"Value": {"Ref": "Provider"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "oidc-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	providerARN := outputs["Arn"]
	require.Contains(t, providerARN, "oidc-provider/token.actions.githubusercontent.com")

	provider, err := backends.IAM.Backend.GetOpenIDConnectProvider(providerARN)
	require.NoError(t, err)
	assert.Equal(t, []string{"sts.amazonaws.com"}, provider.ClientIDList)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("oidc-stack")})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetOpenIDConnectProvider(providerARN)
	require.Error(t, err)
}

func TestCreateStack_RDSDBProxy(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Proxy": {
    "Type": "AWS::RDS::DBProxy",
    "Properties": {
      "DBProxyName": "my-proxy",
      "EngineFamily": "MYSQL",
      "RoleArn": "arn:aws:iam::000000000000:role/proxy-role",
      "VpcSubnetIds": ["subnet-1", "subnet-2"],
      "Auth": [{"AuthScheme": "SECRETS",
        "SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
        "IAMAuth": "DISABLED"}],
      "Tags": [{"Key": "Name", "Value": "proxy-tag"}]
    }
  }
},
"Outputs": {
  "Name": {"Value": {"Ref": "Proxy"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Proxy", "DBProxyArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "dbproxy-stack", tmpl)
	assert.Equal(t, "my-proxy", outputs["Name"])
	assert.Contains(t, outputs["Arn"], "db-proxy:prx-my-proxy")

	proxies, err := backends.RDS.Backend.DescribeDBProxies("my-proxy")
	require.NoError(t, err)
	require.Len(t, proxies, 1)
	assert.Equal(t, "MYSQL", proxies[0].EngineFamily)
	assert.Equal(t, []string{"subnet-1", "subnet-2"}, proxies[0].VpcSubnetIDs)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dbproxy-stack")})
	require.NoError(t, err)

	// DescribeDBProxies with a specific name that no longer exists errors
	// (matching real AWS's DBProxyNotFoundFault), rather than returning an
	// empty list.
	_, err = backends.RDS.Backend.DescribeDBProxies("my-proxy")
	require.Error(t, err)
}
