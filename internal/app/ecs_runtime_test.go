package app

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newECSTaskFixture(t *testing.T, role string) (*sfnFixture, *ecs.Client, string) {
	t.Helper()

	fx := newSFNFixture(t)
	c := ecs.NewFromConfig(fx.cfg)

	_, err := c.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String("rt")})
	require.NoError(t, err)

	in := &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("rt-app"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("busybox"), Essential: aws.Bool(true)},
		},
	}
	if role != "" {
		in.TaskRoleArn = aws.String(role)
	}

	td, err := c.RegisterTaskDefinition(t.Context(), in)
	require.NoError(t, err)

	return fx, c, aws.ToString(td.TaskDefinition.TaskDefinitionArn)
}

func TestECSTaskStateEventsReachRuleTarget(t *testing.T) {
	t.Parallel()

	fx, c, tdArn := newECSTaskFixture(t, "")
	authzStartWorkers(t, fx, "EventBridge")

	queueURL, queueArn := authzQueue(t, fx, "ecs-events")
	ebc := eventbridge.NewFromConfig(fx.cfg)

	_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
		Name: aws.String("ecs-all"), EventPattern: aws.String(`{"source":["aws.ecs"]}`),
	})
	require.NoError(t, err)

	_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
		Rule:    aws.String("ecs-all"),
		Targets: []ebtypes.Target{{Id: aws.String("q"), Arn: aws.String(queueArn)}},
	})
	require.NoError(t, err)

	run, err := c.RunTask(t.Context(), &ecs.RunTaskInput{
		Cluster: aws.String("rt"), TaskDefinition: aws.String(tdArn), StartedBy: aws.String("events-test"),
	})
	require.NoError(t, err)
	require.Len(t, run.Tasks, 1)

	taskArn := aws.ToString(run.Tasks[0].TaskArn)

	_, err = c.StopTask(t.Context(), &ecs.StopTaskInput{
		Cluster: aws.String("rt"), Task: aws.String(taskArn), Reason: aws.String("test done"),
	})
	require.NoError(t, err)

	got := map[string]map[string]any{}
	sq := sqs.NewFromConfig(fx.cfg)

	require.Eventually(t, func() bool {
		out, rerr := sq.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10,
		})
		if rerr != nil {
			return false
		}

		for _, m := range out.Messages {
			var env map[string]any
			uErr := json.Unmarshal([]byte(aws.ToString(m.Body)), &env)
			if uErr != nil || env["detail-type"] != "ECS Task State Change" {
				continue
			}

			d, _ := env["detail"].(map[string]any)
			got[d["lastStatus"].(string)] = env
		}

		return got["RUNNING"] != nil && got["STOPPED"] != nil
	}, authzDeadline, authzTick)

	for status, env := range got {
		assert.Equal(t, "aws.ecs", env["source"], status)
		assert.Equal(t, "us-east-1", env["region"], status)
		assert.Equal(t, []any{taskArn}, env["resources"], status)

		d, _ := env["detail"].(map[string]any)
		assert.Equal(t, taskArn, d["taskArn"], status)
		assert.Equal(t, aws.ToString(run.Tasks[0].ClusterArn), d["clusterArn"], status)
		assert.Equal(t, tdArn, d["taskDefinitionArn"], status)
		assert.Equal(t, "events-test", d["startedBy"], status)
	}

	stopped, _ := got["STOPPED"]["detail"].(map[string]any)
	assert.Equal(t, "test done", stopped["stoppedReason"])
	assert.Equal(t, "STOPPED", stopped["desiredStatus"])

	running, _ := got["RUNNING"]["detail"].(map[string]any)
	assert.Equal(t, "RUNNING", running["desiredStatus"])
	assert.Equal(t, "app", running["containers"].([]any)[0].(map[string]any)["name"])
}

func TestECSTaskRoleCredentialsAssumedIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		wantErr   bool
	}{
		{name: "trusted_role", principal: "ecs-tasks.amazonaws.com"},
		{name: "untrusted_role", principal: "lambda.amazonaws.com", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			roleArn := authzRole(t, fx, "task-role", tt.principal, "s3:*")
			c := ecs.NewFromConfig(fx.cfg)

			_, err := c.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String("rt")})
			require.NoError(t, err)

			td, err := c.RegisterTaskDefinition(t.Context(), &ecs.RegisterTaskDefinitionInput{
				Family:      aws.String("rt-app"),
				TaskRoleArn: aws.String(roleArn),
				ContainerDefinitions: []ecstypes.ContainerDefinition{
					{Name: aws.String("app"), Image: aws.String("busybox")},
				},
			})
			require.NoError(t, err)

			run, err := c.RunTask(t.Context(), &ecs.RunTaskInput{
				Cluster: aws.String("rt"), TaskDefinition: td.TaskDefinition.TaskDefinitionArn,
			})
			require.NoError(t, err)

			taskArn := aws.ToString(run.Tasks[0].TaskArn)
			taskID := taskArn[len(taskArn)-36:]
			endpoint := aws.ToString(fx.cfg.BaseEndpoint)

			provider := endpointcreds.New(endpoint + "/_gopherstack/ecs/credentials/" + taskID)
			creds, err := provider.Retrieve(t.Context())

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			stsCfg := fx.cfg.Copy()
			stsCfg.Credentials = aws.NewCredentialsCache(provider)

			id, err := sts.NewFromConfig(stsCfg).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:sts::000000000000:assumed-role/task-role/"+taskID, aws.ToString(id.Arn))
			assert.NotEmpty(t, creds.SessionToken)
		})
	}
}

func TestECSWiringSetsEndpointAndIssuer(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8125"}
	cli.Lambda.DockerHost = "172.17.0.1"
	byName := newWiredServices(t, cli)

	bk := ecsBackendOf(byName)
	require.NotNil(t, bk)
	assert.Equal(t, "http://172.17.0.1:8125", bk.EndpointURL())
}
