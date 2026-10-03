package main

import (
	"archive/zip"
	"bytes"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
)

func authzResourcePolicy(principal, action, resource, sourceLike string) string {
	cond := ""
	if sourceLike != "" {
		cond = `,"Condition":{"ArnLike":{"aws:SourceArn":"` + sourceLike + `"}}`
	}

	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"` + principal +
		`"},"Action":"` + action + `","Resource":"` + resource + `"` + cond + `}]}`
}

func authzQueuePolicy(t *testing.T, fx *sfnFixture, queueURL, queueARN, principal, sourceLike string) {
	t.Helper()

	_, err := sqs.NewFromConfig(fx.cfg).SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNamePolicy): authzResourcePolicy(
				principal, "sqs:SendMessage", queueARN, sourceLike),
		},
	})
	require.NoError(t, err)
}

func authzDLQErrorCode(t *testing.T, fx *sfnFixture, queueURL string) string {
	t.Helper()

	out, err := sqs.NewFromConfig(fx.cfg).ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, MessageAttributeNames: []string{"All"},
	})
	if err != nil || len(out.Messages) == 0 {
		return ""
	}

	if v, ok := out.Messages[0].MessageAttributes["ERROR_CODE"]; ok {
		return aws.ToString(v.StringValue)
	}

	return "none"
}

func TestServiceResourceAuthzEventBridgeQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principal  string
		sourceLike string
		wantCode   string
		enforce    bool
		noPolicy   bool
	}{
		{name: "allowed", enforce: true, principal: "events.amazonaws.com", sourceLike: "arn:aws:events:*:*:rule/*r"},
		{name: "allowed_no_condition", enforce: true, principal: "events.amazonaws.com"},
		{name: "denied_no_policy", enforce: true, noPolicy: true, wantCode: "NO_PERMISSIONS"},
		{
			name: "denied_other_principal", enforce: true, principal: "sns.amazonaws.com",
			sourceLike: "arn:aws:events:*:*:rule/*r", wantCode: "NO_PERMISSIONS",
		},
		{
			name: "denied_other_source", enforce: true, principal: "events.amazonaws.com",
			sourceLike: "arn:aws:events:*:*:rule/*other", wantCode: "NO_PERMISSIONS",
		},
		{name: "enforcement_off_unchanged", noPolicy: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			authzStartWorkers(t, fx, "EventBridge")

			destURL, destARN := authzQueue(t, fx, "res-dest")
			dlqURL, dlqARN := authzQueue(t, fx, "res-dlq")

			if !tt.noPolicy {
				authzQueuePolicy(t, fx, destURL, destARN, tt.principal, tt.sourceLike)
			}

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["authz.res"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []ebtypes.Target{{
					Id: aws.String("t"), Arn: aws.String(destARN),
					DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(dlqARN)},
					RetryPolicy:      &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
				}},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("authz.res"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			if tt.wantCode == "" {
				require.Eventually(t, func() bool { return authzReceived(t, fx, destURL) }, authzDeadline, authzTick)

				return
			}

			var code string

			require.Eventually(t, func() bool {
				code = authzDLQErrorCode(t, fx, dlqURL)

				return code != ""
			}, authzDeadline, authzTick)
			require.Equal(t, tt.wantCode, code)
			require.False(t, authzReceived(t, fx, destURL))
		})
	}
}

func TestServiceResourceAuthzEventBridgeTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		enforce  bool
		noPolicy bool
	}{
		{name: "allowed", enforce: true},
		{name: "denied_no_policy", enforce: true, noPolicy: true, wantCode: "NO_PERMISSIONS"},
		{name: "enforcement_off_unchanged", noPolicy: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			authzStartWorkers(t, fx, "EventBridge")

			subURL, subARN := authzQueue(t, fx, "topic-sub")
			dlqURL, dlqARN := authzQueue(t, fx, "topic-dlq")

			snsc := sns.NewFromConfig(fx.cfg)
			topic, err := snsc.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("authz-topic")})
			require.NoError(t, err)

			authzQueuePolicy(t, fx, subURL, subARN, "sns.amazonaws.com", "")

			_, err = snsc.Subscribe(t.Context(), &sns.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(subARN),
			})
			require.NoError(t, err)

			if !tt.noPolicy {
				_, err = snsc.SetTopicAttributes(t.Context(), &sns.SetTopicAttributesInput{
					TopicArn: topic.TopicArn, AttributeName: aws.String("Policy"),
					AttributeValue: aws.String(authzResourcePolicy(
						"events.amazonaws.com", "sns:Publish", aws.ToString(topic.TopicArn), "")),
				})
				require.NoError(t, err)
			}

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err = ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["authz.topic"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []ebtypes.Target{{
					Id: aws.String("t"), Arn: topic.TopicArn,
					DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(dlqARN)},
					RetryPolicy:      &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
				}},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("authz.topic"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			if tt.wantCode == "" {
				require.Eventually(t, func() bool { return authzReceived(t, fx, subURL) }, authzDeadline, authzTick)

				return
			}

			var code string

			require.Eventually(t, func() bool {
				code = authzDLQErrorCode(t, fx, dlqURL)

				return code != ""
			}, authzDeadline, authzTick)
			require.Equal(t, tt.wantCode, code)
			require.False(t, authzReceived(t, fx, subURL))
		})
	}
}

func authzLambda(t *testing.T, fx *sfnFixture, name string) string {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.py")
	require.NoError(t, err)
	_, err = w.Write([]byte("def handler(event, context):\n    return event\n"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	out, err := lambda.NewFromConfig(fx.cfg).CreateFunction(t.Context(), &lambda.CreateFunctionInput{
		FunctionName: aws.String(name), PackageType: lambdatypes.PackageTypeZip,
		Runtime: lambdatypes.RuntimePython312, Handler: aws.String("index.handler"),
		Role: aws.String(authzAccount + "lambda-exec"), Code: &lambdatypes.FunctionCode{ZipFile: buf.Bytes()},
	})
	require.NoError(t, err)

	return aws.ToString(out.FunctionArn)
}

func TestServiceResourceAuthzLambda(t *testing.T) {
	t.Parallel()

	const sourceRule = "arn:aws:events:us-east-1:000000000000:rule/r"

	tests := []struct {
		name      string
		principal string
		callAs    string
		grantSrc  string
		callSrc   string
		wantErr   bool
	}{
		{
			name: "allowed", principal: "events.amazonaws.com", callAs: "events.amazonaws.com",
			grantSrc: sourceRule, callSrc: sourceRule,
		},
		{
			name: "allowed_no_source_condition", principal: "events.amazonaws.com", callAs: "events.amazonaws.com",
			callSrc: sourceRule,
		},
		{
			name: "denied_other_source", principal: "events.amazonaws.com", callAs: "events.amazonaws.com",
			grantSrc: sourceRule, callSrc: "arn:aws:events:us-east-1:000000000000:rule/other", wantErr: true,
		},
		{
			name: "denied_other_principal", principal: "sns.amazonaws.com", callAs: "events.amazonaws.com",
			callSrc: sourceRule, wantErr: true,
		},
		{name: "denied_no_policy", callAs: "events.amazonaws.com", callSrc: sourceRule, wantErr: true},
		{
			name: "logs_principal", principal: "logs.amazonaws.com", callAs: "logs.amazonaws.com",
			callSrc: "arn:aws:logs:us-east-1:000000000000:log-group:g:*",
		},
		{
			name: "iot_principal_denied_for_events", principal: "iot.amazonaws.com", callAs: "events.amazonaws.com",
			callSrc: sourceRule, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, true)
			fnARN := authzLambda(t, fx, "authz-fn")

			if tt.principal != "" {
				in := &lambda.AddPermissionInput{
					FunctionName: aws.String("authz-fn"), StatementId: aws.String("s1"),
					Action: aws.String("lambda:InvokeFunction"), Principal: aws.String(tt.principal),
				}
				if tt.grantSrc != "" {
					in.SourceArn = aws.String(tt.grantSrc)
				}

				_, err := lambda.NewFromConfig(fx.cfg).AddPermission(t.Context(), in)
				require.NoError(t, err)
			}

			auth := buildServiceRoleAuthorizer(fx.services)
			require.NotNil(t, auth)

			ra, ok := auth.(roleauth.ResourceAuthorizer)
			require.True(t, ok)

			err := ra.AuthorizeServiceResource(tt.callAs, "lambda:InvokeFunction", fnARN, tt.callSrc)
			if tt.wantErr {
				require.ErrorIs(t, err, roleauth.ErrAccessDenied)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServiceResourceAuthzS3Notification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principal  string
		sourceLike string
		wantErr    bool
		enforce    bool
		noPolicy   bool
	}{
		{name: "allowed", enforce: true, principal: "s3.amazonaws.com", sourceLike: "arn:aws:s3:::notif-bucket"},
		{name: "denied_no_policy", enforce: true, noPolicy: true, wantErr: true},
		{
			name: "denied_other_bucket", enforce: true, principal: "s3.amazonaws.com",
			sourceLike: "arn:aws:s3:::other", wantErr: true,
		},
		{
			name: "denied_other_principal", enforce: true, principal: "events.amazonaws.com",
			sourceLike: "arn:aws:s3:::notif-bucket", wantErr: true,
		},
		{name: "enforcement_off_unchanged", noPolicy: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			queueURL, queueARN := authzQueue(t, fx, "s3-dest")

			if !tt.noPolicy {
				authzQueuePolicy(t, fx, queueURL, queueARN, tt.principal, tt.sourceLike)
			}

			s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
			_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("notif-bucket")})
			require.NoError(t, err)

			_, err = s3c.PutBucketNotificationConfiguration(t.Context(), &s3.PutBucketNotificationConfigurationInput{
				Bucket: aws.String("notif-bucket"),
				NotificationConfiguration: &s3types.NotificationConfiguration{
					QueueConfigurations: []s3types.QueueConfiguration{{
						QueueArn: aws.String(queueARN), Events: []s3types.Event{s3types.EventS3ObjectCreated},
					}},
				},
			})

			if tt.wantErr {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidArgument", apiErr.ErrorCode())
				assert.Contains(t, apiErr.ErrorMessage(), "Unable to validate the following destination configurations")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServiceResourceAuthzSNSToQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principal  string
		sourceLike string
		wantDLQ    bool
		enforce    bool
		noPolicy   bool
	}{
		{name: "allowed", enforce: true, principal: "sns.amazonaws.com", sourceLike: "arn:aws:sns:*:*:authz-t"},
		{name: "denied_no_policy", enforce: true, noPolicy: true, wantDLQ: true},
		{
			name: "denied_other_topic", enforce: true, principal: "sns.amazonaws.com",
			sourceLike: "arn:aws:sns:*:*:other", wantDLQ: true,
		},
		{name: "enforcement_off_unchanged", noPolicy: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			subURL, subARN := authzQueue(t, fx, "sns-sub")
			dlqURL, dlqARN := authzQueue(t, fx, "sns-dlq")

			if !tt.noPolicy {
				authzQueuePolicy(t, fx, subURL, subARN, tt.principal, tt.sourceLike)
			}

			snsc := sns.NewFromConfig(fx.cfg)
			topic, err := snsc.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("authz-t")})
			require.NoError(t, err)

			_, err = snsc.Subscribe(t.Context(), &sns.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(subARN),
				Attributes: map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `"}`},
			})
			require.NoError(t, err)

			_, err = snsc.Publish(t.Context(), &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("hi")})
			require.NoError(t, err)

			want, other := subURL, dlqURL
			if tt.wantDLQ {
				want, other = dlqURL, subURL
			}

			require.Eventually(t, func() bool { return authzReceived(t, fx, want) }, authzDeadline, authzTick)
			assert.False(t, authzReceived(t, fx, other))
		})
	}
}

func TestServiceRoleAuthzLogsSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantMsg   string
		enforce   bool
		untrusted bool
	}{
		{name: "allowed", enforce: true, action: "kinesis:PutRecord"},
		{
			name: "denied", enforce: true, action: "sqs:SendMessage",
			wantMsg: "Could not deliver test message to specified Kinesis stream",
		},
		{
			name: "untrusted", enforce: true, action: "kinesis:PutRecord", untrusted: true,
			wantMsg: "Could not deliver test message to specified Kinesis stream",
		},
		{name: "enforcement_off_unchanged", action: "sqs:SendMessage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			principal := "logs.amazonaws.com"
			if tt.untrusted {
				principal = "events.amazonaws.com"
			}

			role := authzRole(t, fx, "logs-sub-role", principal, tt.action)
			streamARN := kinesisStreamARN(t, fx, "logs-stream", true)

			cwl := cloudwatchlogs.NewFromConfig(fx.cfg)
			_, err := cwl.CreateLogGroup(
				t.Context(),
				&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("g")},
			)
			require.NoError(t, err)

			_, err = cwl.CreateLogStream(t.Context(), &cloudwatchlogs.CreateLogStreamInput{
				LogGroupName: aws.String("g"), LogStreamName: aws.String("s"),
			})
			require.NoError(t, err)

			_, err = cwl.PutSubscriptionFilter(t.Context(), &cloudwatchlogs.PutSubscriptionFilterInput{
				LogGroupName: aws.String("g"), FilterName: aws.String("f"), FilterPattern: aws.String(""),
				DestinationArn: aws.String(streamARN), RoleArn: aws.String(role),
			})

			if tt.wantMsg != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
				assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)

				return
			}

			require.NoError(t, err)

			_, err = cwl.PutLogEvents(t.Context(), &cloudwatchlogs.PutLogEventsInput{
				LogGroupName:  aws.String("g"),
				LogStreamName: aws.String("s"),
				LogEvents: []cwltypes.InputLogEvent{
					{Message: aws.String("m"), Timestamp: aws.Int64(time.Now().UnixMilli())},
				},
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool { return len(kinesisRecords(t, fx, "logs-stream")) > 0 },
				authzDeadline, authzTick)
		})
	}
}

func TestServiceResourceAuthzLogsSubscriptionLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		grant   string
		wantErr bool
		enforce bool
	}{
		{name: "allowed", enforce: true, grant: "logs.amazonaws.com"},
		{name: "denied_no_permission", enforce: true, wantErr: true},
		{name: "denied_other_principal", enforce: true, grant: "events.amazonaws.com", wantErr: true},
		{name: "enforcement_off_unchanged"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			fnARN := authzLambda(t, fx, "logs-fn")

			if tt.grant != "" {
				_, err := lambda.NewFromConfig(fx.cfg).AddPermission(t.Context(), &lambda.AddPermissionInput{
					FunctionName: aws.String("logs-fn"), StatementId: aws.String("s1"),
					Action: aws.String("lambda:InvokeFunction"), Principal: aws.String(tt.grant),
				})
				require.NoError(t, err)
			}

			cwl := cloudwatchlogs.NewFromConfig(fx.cfg)
			_, err := cwl.CreateLogGroup(
				t.Context(),
				&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("g")},
			)
			require.NoError(t, err)

			_, err = cwl.PutSubscriptionFilter(t.Context(), &cloudwatchlogs.PutSubscriptionFilterInput{
				LogGroupName: aws.String("g"), FilterName: aws.String("f"), FilterPattern: aws.String(""),
				DestinationArn: aws.String(fnARN),
			})

			if tt.wantErr {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
				assert.Contains(t, apiErr.ErrorMessage(), "Could not execute the lambda function")

				return
			}

			require.NoError(t, err)
		})
	}
}

func authzFirehoseStream(t *testing.T, fx *sfnFixture, name, bucket, role string, withLogs bool) {
	t.Helper()

	dest := &fhtypes.ExtendedS3DestinationConfiguration{
		BucketARN: aws.String("arn:aws:s3:::" + bucket), RoleARN: aws.String(role),
	}
	if withLogs {
		dest.CloudWatchLoggingOptions = &fhtypes.CloudWatchLoggingOptions{
			Enabled: aws.Bool(true), LogGroupName: aws.String("/fh/errors"), LogStreamName: aws.String("S3Delivery"),
		}
	}

	_, err := firehose.NewFromConfig(fx.cfg).CreateDeliveryStream(t.Context(), &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName:                 aws.String(name),
		ExtendedS3DestinationConfiguration: dest,
	})
	require.NoError(t, err)
}

func TestServiceRoleAuthzFirehoseS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantCode  string
		enforce   bool
		untrusted bool
	}{
		{name: "allowed", enforce: true, action: "s3:PutObject"},
		{name: "denied", enforce: true, action: "sqs:SendMessage", wantCode: "S3.AccessDenied"},
		{
			name:      "untrusted",
			enforce:   true,
			action:    "s3:PutObject",
			untrusted: true,
			wantCode:  "S3.AssumeRoleAccessDenied",
		},
		{name: "enforcement_off_unchanged", action: "sqs:SendMessage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			principal := "firehose.amazonaws.com"
			if tt.untrusted {
				principal = "events.amazonaws.com"
			}

			role := authzRole(t, fx, "fh-role", principal, tt.action)

			s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
			_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("fh-bucket")})
			require.NoError(t, err)

			cwl := cloudwatchlogs.NewFromConfig(fx.cfg)
			_, err = cwl.CreateLogGroup(
				t.Context(),
				&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("/fh/errors")},
			)
			require.NoError(t, err)

			authzFirehoseStream(t, fx, "fh-stream", "fh-bucket", role, true)

			_, err = firehose.NewFromConfig(fx.cfg).PutRecord(t.Context(), &firehose.PutRecordInput{
				DeliveryStreamName: aws.String("fh-stream"), Record: &fhtypes.Record{Data: []byte("hello")},
			})
			require.NoError(t, err)

			fhH, ok := serviceByName(fx.services)["Firehose"].(*firehosebackend.Handler)
			require.True(t, ok)

			fhBk, ok := fhH.Backend.(*firehosebackend.InMemoryBackend)
			require.True(t, ok)
			fhBk.FlushAll(t.Context())

			listed, err := s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String("fh-bucket")})
			require.NoError(t, err)

			if tt.wantCode == "" {
				assert.NotEmpty(t, listed.Contents)

				return
			}

			assert.Empty(t, listed.Contents)

			var msgs []string

			require.Eventually(t, func() bool {
				out, evErr := cwl.FilterLogEvents(
					t.Context(),
					&cloudwatchlogs.FilterLogEventsInput{LogGroupName: aws.String("/fh/errors")},
				)
				if evErr != nil {
					return false
				}

				msgs = msgs[:0]
				for _, e := range out.Events {
					msgs = append(msgs, aws.ToString(e.Message))
				}

				return len(msgs) > 0
			}, authzDeadline, authzTick)
			assert.Contains(t, msgs[0], tt.wantCode)
		})
	}
}

func TestServiceRoleAuthzSNSFirehoseSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantDLQ   bool
		enforce   bool
		untrusted bool
	}{
		{name: "allowed", enforce: true, action: "firehose:PutRecordBatch"},
		{name: "denied", enforce: true, action: "sqs:SendMessage", wantDLQ: true},
		{name: "untrusted", enforce: true, action: "firehose:PutRecordBatch", untrusted: true, wantDLQ: true},
		{name: "enforcement_off_unchanged", action: "sqs:SendMessage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			principal := "sns.amazonaws.com"
			if tt.untrusted {
				principal = "events.amazonaws.com"
			}

			subRole := authzRole(t, fx, "sns-fh-role", principal, tt.action)
			fhRole := authzRole(t, fx, "fh-dest-role", "firehose.amazonaws.com", "s3:PutObject")
			dlqURL, dlqARN := authzQueue(t, fx, "sns-fh-dlq")

			s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
			_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("snsfh-bucket")})
			require.NoError(t, err)

			authzFirehoseStream(t, fx, "snsfh-stream", "snsfh-bucket", fhRole, false)

			snsc := sns.NewFromConfig(fx.cfg)
			topic, err := snsc.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("snsfh-topic")})
			require.NoError(t, err)

			_, err = snsc.Subscribe(t.Context(), &sns.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String("firehose"),
				Endpoint: aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/snsfh-stream"),
				Attributes: map[string]string{
					"SubscriptionRoleArn": subRole,
					"RedrivePolicy":       `{"deadLetterTargetArn":"` + dlqARN + `"}`,
				},
			})
			require.NoError(t, err)

			_, err = snsc.Publish(t.Context(), &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("hi")})
			require.NoError(t, err)

			if tt.wantDLQ {
				require.Eventually(t, func() bool { return authzReceived(t, fx, dlqURL) }, authzDeadline, authzTick)

				return
			}

			fhH, ok := serviceByName(fx.services)["Firehose"].(*firehosebackend.Handler)
			require.True(t, ok)

			fhBk, ok := fhH.Backend.(*firehosebackend.InMemoryBackend)
			require.True(t, ok)

			require.Eventually(t, func() bool {
				fhBk.FlushAll(t.Context())

				listed, listErr := s3c.ListObjectsV2(
					t.Context(),
					&s3.ListObjectsV2Input{Bucket: aws.String("snsfh-bucket")},
				)

				return listErr == nil && len(listed.Contents) > 0
			}, authzDeadline, authzTick)
			assert.False(t, authzReceived(t, fx, dlqURL))
		})
	}
}

func TestServiceRoleAuthzIoTRuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		enforce   bool
		untrusted bool
		wantError bool
	}{
		{name: "allowed", enforce: true, action: "sqs:SendMessage"},
		{name: "denied_runs_error_action", enforce: true, action: "lambda:InvokeFunction", wantError: true},
		{
			name:      "untrusted_runs_error_action",
			enforce:   true,
			action:    "sqs:SendMessage",
			untrusted: true,
			wantError: true,
		},
		{name: "enforcement_off_unchanged", action: "lambda:InvokeFunction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			principal := "iot.amazonaws.com"
			if tt.untrusted {
				principal = "events.amazonaws.com"
			}

			role := authzRole(t, fx, "iot-role", principal, tt.action)
			errRole := authzRole(t, fx, "iot-error-role", "iot.amazonaws.com", "sqs:SendMessage")
			dataURL, _ := authzQueue(t, fx, "iot-data")
			errURL, _ := authzQueue(t, fx, "iot-error")

			iotH, ok := serviceByName(fx.services)["IoT"].(*iotbackend.Handler)
			require.True(t, ok)

			iotBk, ok := iotH.Backend.(*iotbackend.InMemoryBackend)
			require.True(t, ok)

			broker := iotbackend.NewBroker(iotBk, freeTCPPort(t))

			go broker.Run(t.Context())

			_, err := iot.NewFromConfig(fx.cfg).CreateTopicRule(t.Context(), &iot.CreateTopicRuleInput{
				RuleName: aws.String("authz_rule"),
				TopicRulePayload: &iottypes.TopicRulePayload{
					Sql: aws.String("SELECT * FROM 'authz/topic'"),
					Actions: []iottypes.Action{{Sqs: &iottypes.SqsAction{
						QueueUrl: aws.String(dataURL), RoleArn: aws.String(role),
					}}},
					ErrorAction: &iottypes.Action{Sqs: &iottypes.SqsAction{
						QueueUrl: aws.String(errURL), RoleArn: aws.String(errRole),
					}},
				},
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return broker.Publish("authz/topic", []byte(`{"k":"v"}`), false, 0) == nil
			}, authzDeadline, authzTick)

			wantURL, otherURL := dataURL, errURL
			if tt.wantError {
				wantURL, otherURL = errURL, dataURL
			}

			require.Eventually(t, func() bool { return authzReceived(t, fx, wantURL) }, authzDeadline, authzTick)
			assert.False(t, authzReceived(t, fx, otherURL))
		})
	}
}
