package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const samHeader = "AWSTemplateFormatVersion: '2010-09-09'\nTransform: AWS::Serverless-2016-10-31\n"

const samAPIFunctionTemplate = samHeader + `
Resources:
  HelloFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      Runtime: python3.12
      CodeUri: s3://deploy-bucket/hello.zip
      MemorySize: 256
      Environment:
        Variables:
          STAGE: dev
      Policies:
        - arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
        - Statement:
            - Effect: Allow
              Action: sqs:SendMessage
              Resource: '*'
      Events:
        Hello:
          Type: Api
          Properties:
            Path: /hello
            Method: get
`

const samQueueTableTemplate = samHeader + `
Globals:
  Function:
    Runtime: nodejs20.x
    Timeout: 30
    Environment:
      Variables:
        TABLE: !Ref Table
Resources:
  Table:
    Type: AWS::Serverless::SimpleTable
    Properties:
      PrimaryKey:
        Name: pk
        Type: String
  Queue:
    Type: AWS::SQS::Queue
  Worker:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      InlineCode: "exports.handler = async () => 'ok';"
      Events:
        Jobs:
          Type: SQS
          Properties:
            Queue: !GetAtt Queue.Arn
            BatchSize: 5
`

const samScheduleAliasTemplate = samHeader + `
Resources:
  Cron:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      Runtime: python3.12
      InlineCode: "def handler(e, c): return 1"
      AutoPublishAlias: live
      Events:
        Nightly:
          Type: Schedule
          Properties:
            Schedule: rate(5 minutes)
`

const samLayerStateMachineTemplate = samHeader + `
Resources:
  Lib:
    Type: AWS::Serverless::LayerVersion
    Properties:
      LayerName: shared-lib
      ContentUri: s3://deploy-bucket/lib.zip
      CompatibleRuntimes: [python3.12]
  Api:
    Type: AWS::Serverless::Api
    Properties:
      StageName: v1
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      Runtime: python3.12
      InlineCode: "def handler(e, c): return 1"
      Layers: [!Ref Lib]
      Events:
        Root:
          Type: Api
          Properties:
            Path: /items/{id}
            Method: any
            RestApiId: !Ref Api
  Flow:
    Type: AWS::Serverless::StateMachine
    Properties:
      Name: sam-flow
      Definition:
        StartAt: Done
        States:
          Done:
            Type: Succeed
`

func newSAMBackends(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newLambdaServiceBackends(t)
	b := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
	)

	return backends, newTestClientForBackend(t, b)
}

func deploySAM(t *testing.T, client *cfnsdk.Client, name, body string) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String(name),
		ChangeSetName: aws.String("deploy"),
		ChangeSetType: types.ChangeSetTypeCreate,
		TemplateBody:  aws.String(body),
		Capabilities:  []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
	})
	require.NoError(t, err)

	_, err = client.ExecuteChangeSet(ctx, &cfnsdk.ExecuteChangeSetInput{
		StackName:     aws.String(name),
		ChangeSetName: aws.String("deploy"),
	})
	require.NoError(t, err)

	out, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.NoError(t, err)
	require.Len(t, out.Stacks, 1)
	require.Equal(t, types.StackStatusCreateComplete, out.Stacks[0].StackStatus,
		aws.ToString(out.Stacks[0].StackStatusReason))
}

func logicalIDs(t *testing.T, client *cfnsdk.Client) map[string]string {
	t.Helper()

	out, err := client.DescribeStackResources(t.Context(), &cfnsdk.DescribeStackResourcesInput{
		StackName: aws.String("sam-stack"),
	})
	require.NoError(t, err)

	ids := make(map[string]string, len(out.StackResources))
	for _, r := range out.StackResources {
		ids[aws.ToString(r.LogicalResourceId)] = aws.ToString(r.ResourceType)
	}

	return ids
}

func getTemplate(t *testing.T, client *cfnsdk.Client, name string, stage types.TemplateStage) string {
	t.Helper()

	out, err := client.GetTemplate(t.Context(), &cfnsdk.GetTemplateInput{
		StackName: aws.String(name), TemplateStage: stage,
	})
	require.NoError(t, err)

	return aws.ToString(out.TemplateBody)
}

func lambdaFn(t *testing.T, b *cloudformation.ServiceBackends, prefix string) *lambdabackend.FunctionConfiguration {
	t.Helper()

	for _, f := range b.Lambda.Backend.ListFunctions("", 100).Data {
		if strings.HasPrefix(f.FunctionName, prefix) {
			return f
		}
	}
	require.FailNow(t, "no function with prefix "+prefix)

	return nil
}

func TestSAMTransform_Deploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify   func(t *testing.T, b *cloudformation.ServiceBackends, client *cfnsdk.Client)
		name     string
		template string
	}{
		{
			name:     "function_with_api_event_and_role",
			template: samAPIFunctionTemplate,
			verify:   verifyFunctionAPIEvent,
		},
		{
			name:     "globals_simpletable_sqs_event",
			template: samQueueTableTemplate,
			verify:   verifyQueueTable,
		},
		{
			name:     "schedule_event_and_autopublishalias",
			template: samScheduleAliasTemplate,
			verify:   verifyScheduleAlias,
		},
		{
			name:     "layer_explicit_api_statemachine",
			template: samLayerStateMachineTemplate,
			verify:   verifyLayerAPIStateMachine,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends, client := newSAMBackends(t)
			deploySAM(t, client, "sam-stack", tc.template)
			tc.verify(t, backends, client)
			assertDeleteClean(t, client)
		})
	}
}

func verifyFunctionAPIEvent(t *testing.T, b *cloudformation.ServiceBackends, client *cfnsdk.Client) {
	t.Helper()

	ids := logicalIDs(t, client)
	for id, typ := range map[string]string{
		"HelloFunction":                    "AWS::Lambda::Function",
		"HelloFunctionRole":                "AWS::IAM::Role",
		"ServerlessRestApi":                "AWS::ApiGateway::RestApi",
		"ServerlessRestApiProdStage":       "AWS::ApiGateway::Stage",
		"HelloFunctionHelloPermissionProd": "AWS::Lambda::Permission",
		"HelloFunctionHelloPermissionTest": "AWS::Lambda::Permission",
	} {
		assert.Equal(t, typ, ids[id], id)
	}

	fn := lambdaFn(t, b, "HelloFunction")
	assert.Equal(t, "python3.12", fn.Runtime)
	assert.Equal(t, 256, fn.MemorySize)
	assert.Equal(t, "dev", fn.Environment.Variables["STAGE"])
	assert.Equal(t, "deploy-bucket", fn.S3BucketCode)
	assert.Contains(t, fn.Role, ":role/HelloFunctionRole-")

	roleName := fn.Role[strings.LastIndex(fn.Role, "/")+1:]
	attached, err := b.IAM.Backend.ListAttachedRolePolicies(roleName)
	require.NoError(t, err)
	arns := make([]string, 0, len(attached))
	for _, p := range attached {
		arns = append(arns, p.PolicyArn)
	}
	assert.Contains(t, arns, "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
	assert.Contains(t, arns, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

	inline, err := b.IAM.Backend.ListRolePolicies(roleName)
	require.NoError(t, err)
	assert.Equal(t, []string{"HelloFunctionRolePolicy1"}, inline)

	apis, _, err := b.APIGateway.Backend.GetRestAPIs(100, "")
	require.NoError(t, err)
	require.Len(t, apis, 1)
	resources, _, err := b.APIGateway.Backend.GetResources(apis[0].ID, "", 100)
	require.NoError(t, err)
	paths := make([]string, 0, len(resources))
	for _, r := range resources {
		paths = append(paths, r.Path)
	}
	assert.Contains(t, paths, "/hello")
	_, err = b.APIGateway.Backend.GetStage(apis[0].ID, "Prod")
	require.NoError(t, err)

	policy, err := b.Lambda.Backend.(*lambdabackend.InMemoryBackend).GetPolicy(fn.FunctionName, "")
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(policy.Policy), "apigateway.amazonaws.com")

	assertOriginalAndProcessed(t, client)
}

func assertOriginalAndProcessed(t *testing.T, client *cfnsdk.Client) {
	t.Helper()

	original := getTemplate(t, client, "sam-stack", types.TemplateStageOriginal)
	assert.Contains(t, original, "AWS::Serverless::Function")
	assert.Contains(t, original, "AWS::Serverless-2016-10-31")

	processed := getTemplate(t, client, "sam-stack", types.TemplateStageProcessed)
	assert.NotContains(t, processed, "AWS::Serverless")
	assert.Contains(t, processed, "AWS::Lambda::Function")
	assert.Contains(t, processed, "HelloFunctionRole")
	assert.Contains(t, processed, "ServerlessRestApiProdStage")
}

func verifyQueueTable(t *testing.T, b *cloudformation.ServiceBackends, client *cfnsdk.Client) {
	t.Helper()

	ids := logicalIDs(t, client)
	assert.Equal(t, "AWS::DynamoDB::Table", ids["Table"])
	assert.Equal(t, "AWS::Lambda::EventSourceMapping", ids["WorkerJobs"])
	assert.Equal(t, "AWS::IAM::Role", ids["WorkerRole"])

	fn := lambdaFn(t, b, "Worker")
	assert.Equal(t, 30, fn.Timeout)
	assert.Equal(t, "nodejs20.x", fn.Runtime)
	assert.Equal(t, "Table", fn.Environment.Variables["TABLE"])
	assert.NotEmpty(t, fn.ZipData)

	imb, ok := b.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	require.True(t, ok)
	esms := imb.ListEventSourceMappings(fn.FunctionName, "", "", 10)
	require.Len(t, esms.Data, 1)
	assert.Equal(t, 5, esms.Data[0].BatchSize)
	assert.Contains(t, esms.Data[0].EventSourceARN, ":sqs:")
}

func verifyScheduleAlias(t *testing.T, b *cloudformation.ServiceBackends, client *cfnsdk.Client) {
	t.Helper()

	ids := logicalIDs(t, client)
	assert.Equal(t, "AWS::Events::Rule", ids["CronNightly"])
	assert.Equal(t, "AWS::Lambda::Permission", ids["CronNightlyPermission"])
	assert.Equal(t, "AWS::Lambda::Alias", ids["CronAliaslive"])
	var versionID string
	for id, typ := range ids {
		if typ == "AWS::Lambda::Version" {
			versionID = id
		}
	}
	assert.True(t, strings.HasPrefix(versionID, "CronVersion"), versionID)

	fn := lambdaFn(t, b, "Cron")
	assert.NotEmpty(t, fn.FunctionName)
}

func verifyLayerAPIStateMachine(t *testing.T, b *cloudformation.ServiceBackends, client *cfnsdk.Client) {
	t.Helper()

	ids := logicalIDs(t, client)
	assert.Equal(t, "AWS::ApiGateway::RestApi", ids["Api"])
	assert.Equal(t, "AWS::ApiGateway::Stage", ids["Apiv1Stage"])
	assert.Equal(t, "AWS::StepFunctions::StateMachine", ids["Flow"])
	assert.Equal(t, "AWS::IAM::Role", ids["FlowRole"])
	assert.Equal(t, "AWS::Lambda::Permission", ids["FnRootPermissionv1"])

	fn := lambdaFn(t, b, "Fn")
	require.Len(t, fn.Layers, 1)
	assert.Contains(t, fn.Layers[0].Arn, ":layer:shared-lib:")

	apis, _, err := b.APIGateway.Backend.GetRestAPIs(100, "")
	require.NoError(t, err)
	require.Len(t, apis, 1)
	_, err = b.APIGateway.Backend.GetStage(apis[0].ID, "v1")
	require.NoError(t, err)
	resources, _, err := b.APIGateway.Backend.GetResources(apis[0].ID, "", 100)
	require.NoError(t, err)
	paths := make([]string, 0, len(resources))
	for _, r := range resources {
		paths = append(paths, r.Path)
	}
	assert.Contains(t, paths, "/items/{id}")
}

func TestSAMTransform_Failures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		reason   string
	}{
		{
			name:     "unsupported_resource_type",
			template: samHeader + "Resources:\n  G:\n    Type: AWS::Serverless::GraphQLApi\n    Properties: {}\n",
			reason:   "AWS::Serverless::GraphQLApi",
		},
		{
			name: "unsupported_function_property",
			template: samHeader + "Resources:\n  F:\n    Type: AWS::Serverless::Function\n    Properties:\n" +
				"      Handler: h\n      Runtime: python3.12\n      InlineCode: x\n      DeploymentPreference: {}\n",
			reason: "DeploymentPreference",
		},
		{
			name: "unsupported_event_type",
			template: samHeader + "Resources:\n  F:\n    Type: AWS::Serverless::Function\n    Properties:\n" +
				"      Handler: h\n      Runtime: python3.12\n      InlineCode: x\n      Events:\n" +
				"        E:\n          Type: MSK\n          Properties: {}\n",
			reason: "MSK",
		},
		{
			name: "local_codeuri_rejected",
			template: samHeader + "Resources:\n  F:\n    Type: AWS::Serverless::Function\n    Properties:\n" +
				"      Handler: h\n      Runtime: python3.12\n      CodeUri: ./src\n",
			reason: "CodeUri",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newSAMBackends(t)
			ctx := t.Context()

			_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
				StackName:    aws.String("bad"),
				TemplateBody: aws.String(tc.template),
				Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
			})
			require.NoError(t, err)

			out, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{StackName: aws.String("bad")})
			require.NoError(t, err)
			require.Len(t, out.Stacks, 1)
			assert.Equal(t, types.StackStatusCreateFailed, out.Stacks[0].StackStatus)
			assert.Contains(t, aws.ToString(out.Stacks[0].StackStatusReason), tc.reason)

			_, err = client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
				StackName:     aws.String("bad-cs"),
				ChangeSetName: aws.String("cs"),
				ChangeSetType: types.ChangeSetTypeCreate,
				TemplateBody:  aws.String(tc.template),
				Capabilities:  []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
			})
			require.NoError(t, err)
			cs, err := client.DescribeChangeSet(ctx, &cfnsdk.DescribeChangeSetInput{
				StackName: aws.String("bad-cs"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ChangeSetStatusFailed, cs.Status)
			assert.Contains(t, aws.ToString(cs.StatusReason), tc.reason)
		})
	}
}

func assertDeleteClean(t *testing.T, client *cfnsdk.Client) {
	t.Helper()

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sam-stack")})
	require.NoError(t, err)

	out, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("sam-stack")})
	if err == nil && len(out.Stacks) == 1 {
		assert.Equal(t, types.StackStatusDeleteComplete, out.Stacks[0].StackStatus,
			aws.ToString(out.Stacks[0].StackStatusReason))
	}
}

func TestSAMTransform_UpdateChangeSet(t *testing.T) {
	t.Parallel()

	_, client := newSAMBackends(t)
	deploySAM(t, client, "sam-stack", samScheduleAliasTemplate)

	_, err := client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("sam-stack"),
		ChangeSetName: aws.String("update"),
		ChangeSetType: types.ChangeSetTypeUpdate,
		TemplateBody: aws.String(
			strings.Replace(samScheduleAliasTemplate, "      AutoPublishAlias: live\n", "      Description: v2\n", 1),
		),
		Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
	})
	require.NoError(t, err)

	cs, err := client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
		StackName: aws.String("sam-stack"), ChangeSetName: aws.String("update"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChangeSetStatusCreateComplete, cs.Status, aws.ToString(cs.StatusReason))

	var removed []string
	for _, c := range cs.Changes {
		if c.ResourceChange != nil && c.ResourceChange.Action == types.ChangeActionRemove {
			removed = append(removed, aws.ToString(c.ResourceChange.LogicalResourceId))
		}
	}
	assert.Contains(t, removed, "CronAliaslive")
}
