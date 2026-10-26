package app

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationProvisionsAppIntegrationsAndCodeGuru(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Event": map[string]any{
				"Type": "AWS::AppIntegrations::EventIntegration",
				"Properties": map[string]any{
					"Name": "cfn-event", "EventBridgeBus": "default",
					"EventFilter": map[string]any{"Source": "aws.partner/example/src"},
				},
			},
			"Bucket": map[string]any{
				"Type":       "AWS::S3::Bucket",
				"Properties": map[string]any{"BucketName": "codeguru-reviewer-cfn"},
			},
			"Assoc": map[string]any{
				"Type": "AWS::CodeGuruReviewer::RepositoryAssociation",
				"Properties": map[string]any{
					"Name":       "cfn-repo",
					"Type":       "S3Bucket",
					"BucketName": map[string]any{"Ref": "Bucket"},
				},
			},
		},
		"Outputs": map[string]any{
			"EventArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Event", "EventIntegrationArn"}}},
			"AssocArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Assoc", "AssociationArn"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("connectfamily-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	stack := waitStack(t, cfn, "connectfamily-stack")
	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))

	outs := map[string]string{}
	for _, o := range stack.Outputs {
		outs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	ai := appintegrations.NewFromConfig(fx.cfg)

	ev, err := ai.GetEventIntegration(t.Context(), &appintegrations.GetEventIntegrationInput{
		Name: aws.String("cfn-event"),
	})
	require.NoError(t, err)
	assert.Equal(t, outs["EventArn"], aws.ToString(ev.EventIntegrationArn))

	cgr := codegurureviewer.NewFromConfig(fx.cfg)
	_, err = cgr.DescribeRepositoryAssociation(t.Context(), &codegurureviewer.DescribeRepositoryAssociationInput{
		AssociationArn: aws.String(outs["AssocArn"]),
	})
	require.NoError(t, err)

	_, err = cfn.DeleteStack(
		t.Context(),
		&cloudformation.DeleteStackInput{StackName: aws.String("connectfamily-stack")},
	)
	require.NoError(t, err)

	events, err := ai.ListEventIntegrations(t.Context(), &appintegrations.ListEventIntegrationsInput{})
	require.NoError(t, err)
	assert.Empty(t, events.EventIntegrations)
}

func TestCloudFormationProvisionsConnect(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Instance": map[string]any{
				"Type": "AWS::Connect::Instance",
				"Properties": map[string]any{
					"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-wired",
					"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
				},
			},
			"Hours": map[string]any{
				"Type": "AWS::Connect::HoursOfOperation",
				"Properties": map[string]any{
					"InstanceArn": map[string]any{"Ref": "Instance"}, "Name": "wired-hours", "TimeZone": "UTC",
					"Config": []any{map[string]any{
						"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
						"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
					}},
				},
			},
			"Queue": map[string]any{
				"Type": "AWS::Connect::Queue",
				"Properties": map[string]any{
					"InstanceArn": map[string]any{"Ref": "Instance"}, "Name": "wired-queue",
					"HoursOfOperationArn": map[string]any{"Fn::GetAtt": []string{"Hours", "HoursOfOperationArn"}},
				},
			},
		},
		"Outputs": map[string]any{
			"InstanceId": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Instance", "Id"}}},
			"QueueArn":   map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Queue", "QueueArn"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	cn := connect.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("connect-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	stack := waitStack(t, cfn, "connect-stack")
	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))

	outs := map[string]string{}
	for _, o := range stack.Outputs {
		outs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	queue, err := cn.DescribeQueue(t.Context(), &connect.DescribeQueueInput{
		InstanceId: aws.String(
			outs["InstanceId"],
		), QueueId: aws.String(outs["QueueArn"][strings.LastIndex(outs["QueueArn"], "/")+1:]),
	})
	require.NoError(t, err)
	assert.Equal(t, "wired-queue", aws.ToString(queue.Queue.Name))

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("connect-stack")})
	require.NoError(t, err)

	instances, err := cn.ListInstances(t.Context(), &connect.ListInstancesInput{})
	require.NoError(t, err)
	assert.Empty(t, instances.InstanceSummaryList)
}
