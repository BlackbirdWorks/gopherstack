package cloudformation_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const queueUpdateTemplate = `{
"Parameters": {
  "Name": {"Type": "String", "Default": "upd-q"},
  "Visibility": {"Type": "String", "Default": "30"}
},
"Resources": {
  "Q": {
    "Type": "AWS::SQS::Queue",
    "UpdateReplacePolicy": "Retain",
    "Properties": {"QueueName": {"Ref": "Name"}, "VisibilityTimeout": {"Ref": "Visibility"}}
  },
  "T": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "upd-topic"}}
}
}`

func stackEvents(t *testing.T, client *cfnsdk.Client, stack string) []cfntypes.StackEvent {
	t.Helper()

	out, err := client.DescribeStackEvents(t.Context(), &cfnsdk.DescribeStackEventsInput{StackName: aws.String(stack)})
	require.NoError(t, err)

	return out.StackEvents
}

func TestUpdateStack_ResourceChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     map[string]string
		wantQueue  string
		wantVis    string
		wantStatus string
		wantReason string
	}{
		{
			name: "in place", params: map[string]string{"Visibility": "77"},
			wantQueue: "upd-q", wantVis: "77", wantStatus: "UPDATE_COMPLETE",
		},
		{
			name: "replacement", params: map[string]string{"Name": "upd-q2"},
			wantQueue: "upd-q2", wantVis: "30", wantStatus: "UPDATE_COMPLETE",
			wantReason: "Requested update requires the creation of a new physical resource; hence creating one.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends(t)
			backend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
			)
			client := newTestClientForBackend(t, backend)

			createStackAndGetOutputs(t, client, "upd", queueUpdateTemplate)
			before := len(stackEvents(t, client, "upd"))

			var params []cfntypes.Parameter
			for k, v := range tt.params {
				params = append(params, cfntypes.Parameter{ParameterKey: aws.String(k), ParameterValue: aws.String(v)})
			}

			_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName:    aws.String("upd"),
				TemplateBody: aws.String(queueUpdateTemplate),
				Parameters:   params,
			})
			require.NoError(t, err)

			stack, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("upd")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, string(stack.Stacks[0].StackStatus))

			url, err := backends.SQS.Backend.GetQueueURL(&sqsbackend.GetQueueURLInput{
				QueueName: tt.wantQueue, Region: "us-east-1",
			})
			require.NoError(t, err)

			attrs, err := backends.SQS.Backend.GetQueueAttributes(&sqsbackend.GetQueueAttributesInput{
				QueueURL: url.QueueURL, Region: "us-east-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantVis, attrs.Attributes["VisibilityTimeout"])

			events := stackEvents(t, client, "upd")[:len(stackEvents(t, client, "upd"))-before]

			reasons := make([]string, 0, len(events))
			topicEvents := 0

			for _, ev := range events {
				reasons = append(reasons, aws.ToString(ev.ResourceStatusReason))

				if aws.ToString(ev.LogicalResourceId) == "T" {
					topicEvents++
				}
			}

			assert.Zero(t, topicEvents, "unchanged resource must emit no events")

			if tt.wantReason != "" {
				assert.Contains(t, reasons, tt.wantReason)
			}
		})
	}
}

func TestUpdateStack_ReplacementRetainsOldResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policy   string
		wantKept bool
	}{
		{name: "retain", policy: "Retain", wantKept: true},
		{name: "delete", policy: "", wantKept: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends(t)
			backend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
			)
			client := newTestClientForBackend(t, backend)

			tmpl := func(name string) string {
				policy := ""
				if tt.policy != "" {
					policy = `"UpdateReplacePolicy": "` + tt.policy + `",`
				}

				return `{"Resources":{"Q":{"Type":"AWS::SQS::Queue",` + policy +
					`"Properties":{"QueueName":"` + name + `"}}}}`
			}

			createStackAndGetOutputs(t, client, "repl", tmpl("old-q"))

			_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("repl"), TemplateBody: aws.String(tmpl("new-q")),
			})
			require.NoError(t, err)

			_, err = backends.SQS.Backend.GetQueueURL(
				&sqsbackend.GetQueueURLInput{QueueName: "new-q", Region: "us-east-1"},
			)
			require.NoError(t, err)

			_, err = backends.SQS.Backend.GetQueueURL(
				&sqsbackend.GetQueueURLInput{QueueName: "old-q", Region: "us-east-1"},
			)
			if tt.wantKept {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestUpdateStack_CustomNamedReplacementRejected(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends(t)
	backend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
	)
	client := newTestClientForBackend(t, backend)

	createStackAndGetOutputs(t, client, "named", `{"Resources":{"Q":{"Type":"AWS::SQS::Queue",`+
		`"Properties":{"QueueName":"fixed-q","FifoQueue":false}}}}`)

	_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("named"),
		TemplateBody: aws.String(
			`{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"fixed-q","FifoQueue":true}}}}`,
		),
	})
	require.NoError(t, err)

	stack, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("named")})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", string(stack.Stacks[0].StackStatus))
	assert.Contains(t, aws.ToString(stack.Stacks[0].StackStatusReason), "custom-named resource requires replacing")
}

func TestUpdateStack_LambdaConfigAppliedInPlace(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends(t)
	backend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
	)
	client := newTestClientForBackend(t, backend)

	tmpl := func(timeout int, handlerBody string) string {
		return fmt.Sprintf(`{"Resources":{"F":{"Type":"AWS::Lambda::Function","Properties":{
			"FunctionName":"upd-fn","Runtime":"python3.12","Handler":"index.handler",
			"Role":"arn:aws:iam::000000000000:role/r","Timeout":%d,
			"Environment":{"Variables":{"K":"v"}},
			"Code":{"ZipFile":%q}}}}}`, timeout, handlerBody)
	}

	createStackAndGetOutputs(t, client, "lfn", tmpl(5, "def handler(e, c): return 1"))

	before, err := backends.Lambda.Backend.GetFunction("upd-fn")
	require.NoError(t, err)

	_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("lfn"), TemplateBody: aws.String(tmpl(20, "def handler(e, c): return 2")),
	})
	require.NoError(t, err)

	after, err := backends.Lambda.Backend.GetFunction("upd-fn")
	require.NoError(t, err)
	assert.Equal(t, 20, after.Timeout)
	assert.NotEqual(t, before.CodeSha256, after.CodeSha256)
	assert.NotEqual(t, before.RevisionID, after.RevisionID)
}
