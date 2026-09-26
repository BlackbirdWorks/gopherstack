package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestCreateStack_MiscMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCodeBuildReportGroup, "codebuild_report_group"},
		{testKinesisResourcePolicy, "kinesis_resource_policy"},
		{testLambdaResourcePolicy, "lambda_resource_policy"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCodeBuildReportGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "RG": {
    "Type": "AWS::CodeBuild::ReportGroup",
    "Properties": {
      "Name": "my-report-group",
      "Type": "TEST",
      "ExportConfig": {"ExportConfigType": "NO_EXPORT"}
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "RG"}},
  "Arn": {"Value": {"Fn::GetAtt": ["RG", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "cb-rg-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Ref"], "my-report-group")

	found, notFound := backends.CodeBuild.Backend.BatchGetReportGroups([]string{outputs["Ref"]})
	require.Len(t, found, 1)
	assert.Empty(t, notFound)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("cb-rg-stack")})
	require.NoError(t, err)

	found, notFound = backends.CodeBuild.Backend.BatchGetReportGroups([]string{outputs["Ref"]})
	assert.Empty(t, found)
	assert.Len(t, notFound, 1)
}

func testKinesisResourcePolicy(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Stream": {
    "Type": "AWS::Kinesis::Stream",
    "Properties": {"Name": "my-stream", "ShardCount": 1}
  },
  "RP": {
    "Type": "AWS::Kinesis::ResourcePolicy",
    "Properties": {
      "ResourceArn": {"Ref": "Stream"},
      "ResourcePolicy": {
        "Version": "2012-10-17",
        "Statement": [{
          "Sid": "Write",
          "Effect": "Allow",
          "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
          "Action": ["kinesis:PutRecord"],
          "Resource": {"Ref": "Stream"}
        }]
      }
    }
  }
},
"Outputs": {
  "StreamArn": {"Value": {"Ref": "Stream"}},
  "Ref": {"Value": {"Ref": "RP"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kn-rp-stack", tmpl)
	assert.Equal(t, outputs["StreamArn"], outputs["Ref"])

	out, err := backends.Kinesis.Backend.GetResourcePolicy(
		t.Context(), &kinesisbackend.GetResourcePolicyInput{ResourceARN: outputs["Ref"]},
	)
	require.NoError(t, err)
	assert.Contains(t, out.Policy, "kinesis:PutRecord")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kn-rp-stack")})
	require.NoError(t, err)

	_, err = backends.Kinesis.Backend.GetResourcePolicy(
		t.Context(), &kinesisbackend.GetResourcePolicyInput{ResourceARN: outputs["Ref"]},
	)
	require.Error(t, err)
}

func testLambdaResourcePolicy(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Fn": {
    "Type": "AWS::Lambda::Function",
    "Properties": {
      "FunctionName": "my-function",
      "Runtime": "python3.12",
      "Handler": "index.handler",
      "Role": "arn:aws:iam::000000000000:role/lambda-role",
      "Code": {"ZipFile": "def handler(event, context): pass"}
    }
  },
  "RP": {
    "Type": "AWS::Lambda::ResourcePolicy",
    "Properties": {
      "FunctionResourceArn": {"Ref": "Fn"},
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Id": "default",
        "Statement": [{
          "Sid": "Invoke",
          "Effect": "Allow",
          "Principal": {"Service": "s3.amazonaws.com"},
          "Action": "lambda:InvokeFunction",
          "Resource": {"Ref": "Fn"}
        }]
      }
    }
  }
},
"Outputs": {
  "FnArn": {"Value": {"Ref": "Fn"}},
  "Ref": {"Value": {"Ref": "RP"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "lm-rp-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, outputs["FnArn"], outputs["Ref"])

	imb, ok := backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	require.True(t, ok)

	policy, err := imb.GetResourcePolicy(outputs["Ref"])
	require.NoError(t, err)
	assert.Contains(t, policy.Policy, "s3.amazonaws.com")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("lm-rp-stack")})
	require.NoError(t, err)

	_, err = imb.GetResourcePolicy(outputs["Ref"])
	require.Error(t, err)
}
