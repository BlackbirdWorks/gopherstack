package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedtypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	authzAccount  = "arn:aws:iam::000000000000:role/"
	authzDeadline = 20 * time.Second
	authzTick     = 50 * time.Millisecond
)

func authzTrust(principal string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"` + principal + `"},"Action":"sts:AssumeRole"}]}`
}

func authzPolicy(action string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["` + action + `"],"Resource":"*"}]}`
}

func authzRole(t *testing.T, fx *sfnFixture, name, principal, action string) string {
	t.Helper()

	iamc := iam.NewFromConfig(fx.cfg)
	_, err := iamc.CreateRole(t.Context(), &iam.CreateRoleInput{
		RoleName: aws.String(name), AssumeRolePolicyDocument: aws.String(authzTrust(principal)),
	})
	require.NoError(t, err)

	_, err = iamc.PutRolePolicy(t.Context(), &iam.PutRolePolicyInput{
		RoleName: aws.String(name), PolicyName: aws.String("p"), PolicyDocument: aws.String(authzPolicy(action)),
	})
	require.NoError(t, err)

	return authzAccount + name
}

func authzStartWorkers(t *testing.T, fx *sfnFixture, names ...string) {
	t.Helper()

	byName := serviceByName(fx.services)

	for _, n := range names {
		w, ok := byName[n].(service.BackgroundWorker)
		require.True(t, ok, n)
		require.NoError(t, w.StartWorker(t.Context()))
	}
}

func authzQueue(t *testing.T, fx *sfnFixture, name string) (string, string) {
	t.Helper()

	c := sqs.NewFromConfig(fx.cfg)
	q, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	return aws.ToString(q.QueueUrl), attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
}

func authzReceived(t *testing.T, fx *sfnFixture, queueURL string) bool {
	t.Helper()

	out, err := sqs.NewFromConfig(fx.cfg).ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1,
	})
	if err != nil {
		return false
	}

	return len(out.Messages) > 0
}

func TestServiceRoleAuthzStepFunctionsLegacy(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:fn"
	catch := func(resource, params string) string {
		return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + resource + `","Parameters":` + params +
			`,"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.err","Next":"C"}],"End":true},"C":{"Type":"Succeed"}}}`
	}

	tests := []struct {
		name       string
		definition string
		action     string
		wantErr    string
		enforce    bool
		allowOther bool
	}{
		{
			name: "direct_lambda_allowed", enforce: true, action: "lambda:InvokeFunction",
			definition: catch(lambdaARN, `{}`),
		},
		{
			name: "direct_lambda_denied", enforce: true, action: "sqs:SendMessage",
			wantErr:    "Lambda.AccessDeniedException",
			definition: catch(lambdaARN, `{}`),
		},
		{
			name: "optimized_lambda_allowed", enforce: true, action: "lambda:InvokeFunction",
			definition: catch("arn:aws:states:::lambda:invoke", `{"FunctionName":"fn"}`),
		},
		{
			name: "optimized_lambda_denied", enforce: true, action: "sqs:SendMessage",
			wantErr:    "Lambda.AccessDeniedException",
			definition: catch("arn:aws:states:::lambda:invoke", `{"FunctionName":"fn"}`),
		},
		{
			name: "ecs_denied", enforce: true, action: "lambda:InvokeFunction", wantErr: "Ecs.AccessDeniedException",
			definition: catch("arn:aws:states:::ecs:runTask", `{"TaskDefinition":"td","Cluster":"c"}`),
		},
		{
			name: "ecs_allowed_reaches_service", enforce: true, action: "ecs:RunTask", allowOther: true,
			definition: catch("arn:aws:states:::ecs:runTask", `{"TaskDefinition":"td","Cluster":"c"}`),
		},
		{
			name: "glue_denied", enforce: true, action: "lambda:InvokeFunction", wantErr: "Glue.AccessDeniedException",
			definition: catch("arn:aws:states:::glue:startJobRun", `{"JobName":"j"}`),
		},
		{
			name: "glue_allowed_reaches_service", enforce: true, action: "glue:StartJobRun", allowOther: true,
			definition: catch("arn:aws:states:::glue:startJobRun", `{"JobName":"j"}`),
		},
		{
			name: "enforcement_off_unchanged", action: "sqs:SendMessage",
			definition: catch(lambdaARN, `{}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			fx.sfn.SetLambdaInvoker(&fakeLambda{fn: func(int64, []byte) ([]byte, error) { return []byte(`"ok"`), nil }})

			role := authzRole(t, fx, "sfn-"+strings.ReplaceAll(tt.name, "_", "-"), "states.amazonaws.com", tt.action)
			if !tt.enforce {
				role = authzAccount + "absent"
			}

			out := runSyncSFNAs(t, fx, role, "sm", tt.definition, `{}`)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status)

			caught, _ := jsonAtOptional(out.Output, "err", "Error")

			switch {
			case tt.wantErr != "":
				assert.Equal(t, tt.wantErr, caught)
			case tt.allowOther:
				assert.NotContains(t, caught, "AccessDeniedException")
			default:
				assert.Empty(t, caught)
			}
		})
	}
}

func jsonAtOptional(raw *string, path ...string) (string, bool) {
	var cur any
	if raw == nil || json.Unmarshal([]byte(*raw), &cur) != nil {
		return "", false
	}

	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}

		cur = m[p]
	}

	s, ok := cur.(string)

	return s, ok
}

func TestServiceRoleAuthzScheduler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantTo    string
		enforce   bool
		untrusted bool
	}{
		{name: "allowed", enforce: true, action: "sqs:SendMessage", wantTo: "target"},
		{name: "denied_goes_to_dlq", enforce: true, action: "lambda:InvokeFunction", wantTo: "dlq"},
		{name: "untrusted_goes_to_dlq", enforce: true, action: "sqs:SendMessage", untrusted: true, wantTo: "dlq"},
		{name: "enforcement_off_unchanged", action: "lambda:InvokeFunction", wantTo: "target"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)

			authzStartWorkers(t, fx, "Scheduler")

			principal := "scheduler.amazonaws.com"
			if tt.untrusted {
				principal = "events.amazonaws.com"
			}

			role := authzRole(t, fx, "sched-role", principal, tt.action)
			targetURL, targetARN := authzQueue(t, fx, "sched-target")
			dlqURL, dlqARN := authzQueue(t, fx, "sched-dlq")

			pastAt := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15:04:05")

			_, err := scheduler.NewFromConfig(fx.cfg).CreateSchedule(t.Context(), &scheduler.CreateScheduleInput{
				Name:                  aws.String("s1"),
				ScheduleExpression:    aws.String("at(" + pastAt + ")"),
				FlexibleTimeWindow:    &schedtypes.FlexibleTimeWindow{Mode: schedtypes.FlexibleTimeWindowModeOff},
				ActionAfterCompletion: schedtypes.ActionAfterCompletionNone,
				Target: &schedtypes.Target{
					Arn: aws.String(targetARN), RoleArn: aws.String(role), Input: aws.String("hello"),
					DeadLetterConfig: &schedtypes.DeadLetterConfig{Arn: aws.String(dlqARN)},
					RetryPolicy:      &schedtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
				},
			})
			require.NoError(t, err)

			want, other := targetURL, dlqURL
			if tt.wantTo == "dlq" {
				want, other = dlqURL, targetURL
			}

			require.Eventually(t, func() bool { return authzReceived(t, fx, want) }, authzDeadline, authzTick)
			assert.False(t, authzReceived(t, fx, other))
		})
	}
}

func TestServiceRoleAuthzEventBridgeTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantData  bool
		enforce   bool
		untrusted bool
	}{
		{name: "allowed", enforce: true, action: "events:PutEvents", wantData: true},
		{name: "denied_goes_to_dlq", enforce: true, action: "sqs:SendMessage"},
		{name: "untrusted_goes_to_dlq", enforce: true, action: "events:PutEvents", untrusted: true},
		{name: "enforcement_off_unchanged", action: "sqs:SendMessage", wantData: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			authzStartWorkers(t, fx, "EventBridge")
			principal := "events.amazonaws.com"
			if tt.untrusted {
				principal = "scheduler.amazonaws.com"
			}

			role := authzRole(t, fx, "eb-role", principal, tt.action)
			dlqURL, dlqARN := authzQueue(t, fx, "eb-dlq")
			authzQueuePolicy(t, fx, dlqURL, dlqARN, "events.amazonaws.com", "")

			destURL, destARN := authzQueue(t, fx, "eb-dest")
			authzQueuePolicy(t, fx, destURL, destARN, "events.amazonaws.com", "arn:aws:events:*:*:rule/*")

			ebc := eventbridge.NewFromConfig(fx.cfg)
			bus, err := ebc.CreateEventBus(t.Context(), &eventbridge.CreateEventBusInput{Name: aws.String("dst")})
			require.NoError(t, err)

			_, err = ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("dst-rule"), EventBusName: aws.String("dst"),
				EventPattern: aws.String(`{"source":["authz.test"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("dst-rule"), EventBusName: aws.String("dst"),
				Targets: []ebtypes.Target{{Id: aws.String("q"), Arn: aws.String(destARN)}},
			})
			require.NoError(t, err)

			_, err = ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["authz.test"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []ebtypes.Target{{
					Id: aws.String("t"), Arn: bus.EventBusArn, RoleArn: aws.String(role),
					DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(dlqARN)},
					RetryPolicy:      &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
				}},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("authz.test"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			destHasData := func() bool { return authzReceived(t, fx, destURL) }

			if tt.wantData {
				require.Eventually(t, destHasData, authzDeadline, authzTick)

				return
			}

			require.Eventually(t, func() bool { return authzReceived(t, fx, dlqURL) }, authzDeadline, authzTick)
			assert.False(t, destHasData())
		})
	}
}

func TestServiceRoleAuthzPipes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		allowed  []string
		enforce  bool
		wantData bool
	}{
		{
			name: "allowed", enforce: true, wantData: true,
			allowed: []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"},
		},
		{
			name: "source_denied", enforce: true,
			allowed: []string{"sqs:SendMessage"},
		},
		{
			name: "target_denied", enforce: true,
			allowed: []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"},
		},
		{name: "enforcement_off_unchanged", wantData: true, allowed: []string{"sqs:SendMessage"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			ctx := t.Context()
			authzStartWorkers(t, fx, "Pipes")

			mkRole := func(name string, actions []string) string {
				doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["` +
					strings.Join(actions, `","`) + `"],"Resource":"*"}]}`
				iamc := iam.NewFromConfig(fx.cfg)
				_, err := iamc.CreateRole(ctx, &iam.CreateRoleInput{
					RoleName: aws.String(name), AssumeRolePolicyDocument: aws.String(authzTrust("pipes.amazonaws.com")),
				})
				require.NoError(t, err)
				_, err = iamc.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
					RoleName: aws.String(name), PolicyName: aws.String("p"), PolicyDocument: aws.String(doc),
				})
				require.NoError(t, err)

				return authzAccount + name
			}

			mkPipe := func(name, role string) (string, string) {
				srcURL, srcARN := authzQueue(t, fx, name+"-src")
				dstURL, dstARN := authzQueue(t, fx, name+"-dst")
				_, err := pipes.NewFromConfig(fx.cfg).CreatePipe(ctx, &pipes.CreatePipeInput{
					Name: aws.String(name), RoleArn: aws.String(role),
					Source: aws.String(srcARN), Target: aws.String(dstARN),
				})
				require.NoError(t, err)

				_, err = sqs.NewFromConfig(fx.cfg).SendMessage(ctx, &sqs.SendMessageInput{
					QueueUrl: aws.String(srcURL), MessageBody: aws.String("m"),
				})
				require.NoError(t, err)

				return srcURL, dstURL
			}

			_, controlDst := mkPipe("control", mkRole("pipe-control", []string{
				"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage",
			}))
			_, dst := mkPipe("subject", mkRole("pipe-subject", tt.allowed))

			require.Eventually(t, func() bool { return authzReceived(t, fx, controlDst) }, authzDeadline, authzTick)

			if tt.wantData {
				require.Eventually(t, func() bool { return authzReceived(t, fx, dst) }, authzDeadline, authzTick)

				return
			}

			assert.False(t, authzReceived(t, fx, dst))
		})
	}
}
