package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	eksTestRole    = "arn:aws:iam::000000000000:role/eks-role"
	novaInvokeBody = `{"messages":[{"role":"user","content":[{"text":"hello"}]}]}`
)

func eksSyncDef(resource, params, next string) string {
	return `"Resource":"arn:aws:states:::eks:` + resource + `","Parameters":` + params + next
}

func TestStepFunctionsEKSAndBedrockIntegrations(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)

	clusterSync := `{"StartAt":"C","States":{"C":{"Type":"Task",` +
		eksSyncDef("createCluster.sync",
			`{"Name":"c-sync","RoleArn":"`+eksTestRole+`","ResourcesVpcConfig":{"SubnetIds":["subnet-1"]}}`,
			`,"End":true}}}`)

	nodegroupFlow := `{"StartAt":"C","States":{` +
		`"C":{"Type":"Task",` + eksSyncDef("createCluster.sync",
		`{"Name":"c-ng","RoleArn":"`+eksTestRole+`","ResourcesVpcConfig":{"SubnetIds":["subnet-1"]}}`,
		`,"ResultPath":"$.cluster","Next":"N"},`) +
		`"N":{"Type":"Task",` + eksSyncDef("createNodegroup.sync",
		`{"ClusterName":"c-ng","NodegroupName":"ng1","NodeRole":"`+eksTestRole+`","Subnets":["subnet-1"],`+
			`"ScalingConfig":{"MinSize":1,"MaxSize":2,"DesiredSize":1}}`,
		`,"ResultPath":"$.ng","Next":"DN"},`) +
		`"DN":{"Type":"Task",` + eksSyncDef("deleteNodegroup.sync",
		`{"ClusterName":"c-ng","NodegroupName":"ng1"}`, `,"ResultPath":"$.dng","Next":"DC"},`) +
		`"DC":{"Type":"Task",` + eksSyncDef("deleteCluster.sync", `{"Name":"c-ng"}`, `,"ResultPath":"$.dc","End":true}}}`)

	tests := []struct {
		check      func(t *testing.T, out *sfn.StartSyncExecutionOutput)
		name       string
		definition string
		input      string
	}{
		{
			name: "create_cluster_sync_returns_active_cluster", definition: clusterSync, input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "ACTIVE", jsonAt(t, out.Output, "Cluster", "Status"))
				assert.Equal(t, "c-sync", jsonAt(t, out.Output, "Cluster", "Name"))
			},
		},
		{
			name: "nodegroup_and_cluster_lifecycle_sync", definition: nodegroupFlow, input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "ACTIVE", jsonAt(t, out.Output, "cluster", "Cluster", "Status"))
				assert.Equal(t, "ACTIVE", jsonAt(t, out.Output, "ng", "Nodegroup", "Status"))
				assert.Equal(t, "ng1", jsonAt(t, out.Output, "dng", "Nodegroup", "NodegroupName"))
				assert.Equal(t, "c-ng", jsonAt(t, out.Output, "dc", "Cluster", "Name"))

				_, err := eks.NewFromConfig(fx.cfg).DescribeCluster(t.Context(),
					&eks.DescribeClusterInput{Name: aws.String("c-ng")})
				require.Error(t, err, "cluster must be gone after deleteCluster.sync")
			},
		},
		{
			name: "create_nodegroup_unknown_cluster_caught",
			definition: `{"StartAt":"N","States":{"N":{"Type":"Task",` + eksSyncDef("createNodegroup.sync",
				`{"ClusterName":"nope","NodegroupName":"ng","NodeRole":"`+eksTestRole+`","Subnets":["s"]}`,
				`,`+catchTo("Eks.InvalidParameterException")+`,"End":true},`+caughtState+`}}`),
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "Eks.InvalidParameterException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "eks_call_without_engine_fails_task",
			definition: `{"StartAt":"K","States":{"K":{"Type":"Task","Resource":"arn:aws:states:::eks:call",` +
				`"Parameters":{"ClusterName":"c","Endpoint":"https://X.gr7.us-east-1.eks.amazonaws.com",` +
				`"Method":"GET","Path":"/version"},` + catchTo("States.TaskFailed") + `,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "States.TaskFailed", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "bedrock_invoke_model",
			definition: `{"StartAt":"B","States":{"B":{"Type":"Task","Resource":"arn:aws:states:::bedrock:invokeModel",` +
				`"Parameters":{"ModelId":"amazon.nova-pro-v1:0","Body":` + novaInvokeBody + `},"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "application/json", jsonAt(t, out.Output, "ContentType"))
				body, ok := jsonAt(t, out.Output, "Body").(map[string]any)
				require.True(t, ok, "Body must be a parsed JSON object: %s", aws.ToString(out.Output))
				assert.NotEmpty(t, body)
			},
		},
		{
			name: "bedrock_invoke_model_string_body",
			definition: `{"StartAt":"B","States":{"B":{"Type":"Task","Resource":"arn:aws:states:::bedrock:invokeModel",` +
				`"Parameters":{"ModelId":"amazon.nova-pro-v1:0","Body.$":"$.body"},"End":true}}}`,
			input: `{"body":"{\"messages\":[{\"role\":\"user\",\"content\":[{\"text\":\"hi\"}]}]}"}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.IsType(t, map[string]any{}, jsonAt(t, out.Output, "Body"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := runSyncSFNAs(t, fx, sfnTestRole, "sm-"+tt.name, tt.definition, tt.input)
			require.Equal(t, types.SyncExecutionStatusSucceeded, out.Status,
				"error=%s cause=%s", aws.ToString(out.Error), aws.ToString(out.Cause))
			tt.check(t, out)
		})
	}
}

func TestStepFunctionsEKSAndBedrockExecutionRole(t *testing.T) {
	t.Parallel()

	const (
		trust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	)

	invoke := `{"StartAt":"B","States":{"B":{"Type":"Task","Resource":"arn:aws:states:::bedrock:invokeModel",` +
		`"Parameters":{"ModelId":"amazon.nova-pro-v1:0","Body":` + novaInvokeBody + `},` +
		catchTo("Bedrock.AccessDeniedException") + `,"End":true},` + caughtState + `}}`
	describe := `{"StartAt":"E","States":{"E":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:eks:listClusters",` +
		`"Parameters":{},` + catchTo("Eks.AccessDeniedException") + `,"End":true},` + caughtState + `}}`

	tests := []struct {
		name       string
		action     string
		definition string
		wantDenied bool
	}{
		{name: "bedrock_allowed", action: "bedrock:InvokeModel", definition: invoke},
		{name: "bedrock_denied", action: "sqs:SendMessage", definition: invoke, wantDenied: true},
		{name: "eks_allowed", action: "eks:ListClusters", definition: describe},
		{name: "eks_denied", action: "sqs:SendMessage", definition: describe, wantDenied: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, true)
			iamc := iam.NewFromConfig(fx.cfg)
			role := "sfn-" + tt.name

			_, err := iamc.CreateRole(t.Context(), &iam.CreateRoleInput{
				RoleName: aws.String(role), AssumeRolePolicyDocument: aws.String(trust),
			})
			require.NoError(t, err)

			_, err = iamc.PutRolePolicy(t.Context(), &iam.PutRolePolicyInput{
				RoleName: aws.String(role), PolicyName: aws.String("p"),
				PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"` +
					tt.action + `","Resource":"*"}]}`),
			})
			require.NoError(t, err)

			out := runSyncSFNAs(t, fx, "arn:aws:iam::000000000000:role/"+role, "sm-"+tt.name, tt.definition, `{}`)
			require.Equal(t, types.SyncExecutionStatusSucceeded, out.Status,
				"error=%s cause=%s", aws.ToString(out.Error), aws.ToString(out.Cause))

			if tt.wantDenied {
				assert.NotNil(t, jsonAt(t, out.Output, "err", "Error"))
			} else {
				assert.NotContains(t, aws.ToString(out.Output), `"err"`)
			}
		})
	}
}
