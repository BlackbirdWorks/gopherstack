package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudControlQueueURLMatchesSQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		desir string
		want  string
	}{
		{name: "standard", desir: `{"QueueName":"q1"}`, want: "q1"},
		{name: "fifo", desir: `{"QueueName":"q2.fifo","FifoQueue":true}`, want: "q2.fifo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			cc := cloudcontrol.NewFromConfig(fx.cfg)
			client := sqs.NewFromConfig(fx.cfg)

			created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String("AWS::SQS::Queue"), DesiredState: aws.String(tt.desir),
			})
			require.NoError(t, err)

			id := aws.ToString(created.ProgressEvent.Identifier)
			assert.NotContains(t, id, ":///")
			assert.True(t, strings.HasSuffix(id, "/"+tt.want))

			got, err := client.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(tt.want)})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(got.QueueUrl), id)

			in := &sqs.SendMessageInput{QueueUrl: aws.String(id), MessageBody: aws.String("hi")}
			if strings.HasSuffix(tt.want, ".fifo") {
				in.MessageGroupId = aws.String("g")
				in.MessageDeduplicationId = aws.String("d")
			}

			_, err = client.SendMessage(t.Context(), in)
			require.NoError(t, err)
		})
	}
}

func TestCloudFormationQueueURLMatchesSQS(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "cfn-url-q"}},
		},
		"Outputs": map[string]any{"Url": map[string]any{"Value": map[string]any{"Ref": "Q"}}},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("q-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	var stack cfntypes.Stack

	require.Eventually(t, func() bool {
		out, descErr := cfn.DescribeStacks(
			t.Context(),
			&cloudformation.DescribeStacksInput{StackName: aws.String("q-stack")},
		)
		if descErr != nil || len(out.Stacks) == 0 {
			return false
		}

		stack = out.Stacks[0]

		return stack.StackStatus != cfntypes.StackStatusCreateInProgress
	}, 20*time.Second, 50*time.Millisecond)

	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))
	require.Len(t, stack.Outputs, 1)

	got, err := sqs.NewFromConfig(fx.cfg).
		GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("cfn-url-q")})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(got.QueueUrl), aws.ToString(stack.Outputs[0].OutputValue))
	assert.NotContains(t, aws.ToString(got.QueueUrl), ":///")
}
