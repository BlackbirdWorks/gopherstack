package cloudformation_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	connecttypes "github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

func newConnectEnv(t *testing.T) (familyStackEnv, *connectsdk.Client) {
	t.Helper()

	handler := connectbackend.NewHandler(connectbackend.NewBackend("000000000000", "us-east-1"))
	env := newFamilyStackEnv(t, handler, func(b *cloudformation.ServiceBackends) { b.Connect = handler })

	return env, connectsdk.NewFromConfig(env.cfg, func(o *connectsdk.Options) { o.BaseEndpoint = aws.String(env.url) })
}

func connectFlowContent(t *testing.T) string {
	t.Helper()

	doc, err := json.Marshal(map[string]any{
		"Version": "2019-10-30", "StartAction": "end",
		"Actions": []any{map[string]any{
			"Identifier": "end", "Type": "DisconnectParticipant", "Parameters": map[string]any{},
			"Transitions": map[string]any{},
		}},
	})
	require.NoError(t, err)

	return string(doc)
}

// connectCoreTemplate renders Instance, HoursOfOperation, QuickConnect, Queue, RoutingProfile, Prompt, AgentStatus,
// ContactFlow and ContactFlowModule; ver changes every mutable property.
func connectCoreTemplate(t *testing.T, ver string) string {
	t.Helper()

	inst := cfnRef("Instance")
	tags := cfnTags(map[string]string{"ver": ver})
	maxContacts := 5
	state := "ENABLED"

	if ver == "v2" {
		maxContacts = 7
		state = "DISABLED"
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-inst",
			"Attributes": map[string]any{
				"InboundCalls": true, "OutboundCalls": true, "ContactflowLogs": ver == "v2",
			},
			"Tags": tags,
		}),
		"Hours": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "cfn-hours", "TimeZone": "UTC", "Description": "hours " + ver,
			"Config": []any{map[string]any{
				"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
				"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
			}},
			"Tags": tags,
		}),
		"QC": cfnRes("AWS::Connect::QuickConnect", map[string]any{
			"InstanceArn": inst, "Name": "cfn-qc", "Description": "qc " + ver,
			"QuickConnectConfig": map[string]any{
				"QuickConnectType": "PHONE_NUMBER", "PhoneConfig": map[string]any{"PhoneNumber": "+12065550100"},
			},
			"Tags": tags,
		}),
		"Queue": cfnRes("AWS::Connect::Queue", map[string]any{
			"InstanceArn": inst, "Name": "cfn-queue", "Description": "queue " + ver, "MaxContacts": maxContacts,
			"HoursOfOperationArn": cfnGetAtt("Hours", "HoursOfOperationArn"),
			"QuickConnectArns":    []any{cfnGetAtt("QC", "QuickConnectArn")},
			"Tags":                tags,
		}),
		"Profile": cfnRes("AWS::Connect::RoutingProfile", map[string]any{
			"InstanceArn": inst, "Name": "cfn-profile", "Description": "profile " + ver,
			"DefaultOutboundQueueArn": cfnGetAtt("Queue", "QueueArn"),
			"MediaConcurrencies":      []any{map[string]any{"Channel": "VOICE", "Concurrency": 1}},
			"QueueConfigs": []any{map[string]any{
				"Delay": 0, "Priority": 1,
				"QueueReference": map[string]any{"Channel": "VOICE", "QueueArn": cfnGetAtt("Queue", "QueueArn")},
			}},
			"Tags": tags,
		}),
		"Prompt": cfnRes("AWS::Connect::Prompt", map[string]any{
			"InstanceArn": inst, "Name": "cfn-prompt", "Description": "prompt " + ver, "S3Uri": "s3://bkt/p.wav",
			"Tags": tags,
		}),
		"Status": cfnRes("AWS::Connect::AgentStatus", map[string]any{
			"InstanceArn": inst, "Name": "cfn-status", "State": state, "Description": "status " + ver,
			"DisplayOrder": 3, "Tags": tags,
		}),
		"Flow": cfnRes("AWS::Connect::ContactFlow", map[string]any{
			"InstanceArn": inst, "Name": "cfn-flow", "Type": "CONTACT_FLOW", "Description": "flow " + ver,
			"Content": connectFlowContent(t), "State": map[string]string{"v1": "ACTIVE", "v2": "ARCHIVED"}[ver],
			"Tags": tags,
		}),
		"Module": cfnRes("AWS::Connect::ContactFlowModule", map[string]any{
			"InstanceArn": inst, "Name": "cfn-module", "Description": "module " + ver,
			"Content": connectFlowContent(t), "Tags": tags,
		}),
	}, map[string]any{
		"InstanceRef": inst, "InstanceArn": cfnGetAtt("Instance", "Arn"), "InstanceId": cfnGetAtt("Instance", "Id"),
		"InstanceStatus": cfnGetAtt("Instance", "InstanceStatus"), "ServiceRole": cfnGetAtt("Instance", "ServiceRole"),
		"HoursRef": cfnRef("Hours"), "HoursArn": cfnGetAtt("Hours", "HoursOfOperationArn"),
		"QCRef": cfnRef("QC"), "QCType": cfnGetAtt("QC", "QuickConnectType"),
		"QueueRef": cfnRef(
			"Queue",
		), "QueueArn": cfnGetAtt("Queue", "QueueArn"), "QueueType": cfnGetAtt("Queue", "Type"),
		"ProfileRef": cfnRef("Profile"), "ProfileArn": cfnGetAtt("Profile", "RoutingProfileArn"),
		"PromptRef": cfnRef("Prompt"), "PromptArn": cfnGetAtt("Prompt", "PromptArn"),
		"StatusRef": cfnRef("Status"), "StatusArn": cfnGetAtt("Status", "AgentStatusArn"),
		"FlowRef": cfnRef("Flow"), "FlowArn": cfnGetAtt("Flow", "ContactFlowArn"),
		"ModuleRef": cfnRef("Module"), "ModuleArn": cfnGetAtt("Module", "ContactFlowModuleArn"),
		"ModuleStatus": cfnGetAtt("Module", "Status"),
	})
}

func lastARNSegment(arn string) string {
	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == '/' {
			return arn[i+1:]
		}
	}

	return arn
}

func TestCreateStack_ConnectCore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, env familyStackEnv, cn *connectsdk.Client)
		name string
	}{
		{name: "create ref and getatt", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			out := env.createStack(t, "cn-create", connectCoreTemplate(t, "v1"))
			for _, k := range []string{"Hours", "QC", "Queue", "Profile", "Prompt", "Status", "Flow", "Module"} {
				assert.NotEmpty(t, out[k+"Ref"], k)
			}

			assert.Equal(t, out["InstanceArn"], out["InstanceRef"])
			assert.NotEmpty(t, out["InstanceId"])
			assert.Equal(t, out["HoursArn"], out["HoursRef"])
			assert.Equal(t, out["QueueArn"], out["QueueRef"])
			assert.Equal(t, "STANDARD", out["QueueType"])
			assert.Equal(t, "PHONE_NUMBER", out["QCType"])
			assert.NotEmpty(t, out["InstanceStatus"])
			assert.NotEmpty(t, out["ModuleStatus"])

			inst := aws.String(out["InstanceId"])
			hours, err := cn.DescribeHoursOfOperation(t.Context(), &connectsdk.DescribeHoursOfOperationInput{
				InstanceId: inst, HoursOfOperationId: aws.String(lastARNSegment(out["HoursRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "cfn-hours", aws.ToString(hours.HoursOfOperation.Name))
			assert.Equal(t, map[string]string{"ver": "v1"}, hours.HoursOfOperation.Tags)

			queue, err := cn.DescribeQueue(t.Context(), &connectsdk.DescribeQueueInput{
				InstanceId: inst, QueueId: aws.String(lastARNSegment(out["QueueRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, int32(5), aws.ToInt32(queue.Queue.MaxContacts))
			assert.Equal(t, lastARNSegment(out["HoursRef"]), aws.ToString(queue.Queue.HoursOfOperationId))

			qcs, err := cn.ListQueueQuickConnects(t.Context(), &connectsdk.ListQueueQuickConnectsInput{
				InstanceId: inst, QueueId: aws.String(lastARNSegment(out["QueueRef"])),
			})
			require.NoError(t, err)
			require.Len(t, qcs.QuickConnectSummaryList, 1)
			assert.Equal(t, lastARNSegment(out["QCRef"]), aws.ToString(qcs.QuickConnectSummaryList[0].Id))

			profile, err := cn.DescribeRoutingProfile(t.Context(), &connectsdk.DescribeRoutingProfileInput{
				InstanceId: inst, RoutingProfileId: aws.String(lastARNSegment(out["ProfileRef"])),
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				lastARNSegment(out["QueueRef"]),
				aws.ToString(profile.RoutingProfile.DefaultOutboundQueueId),
			)
			assert.Equal(t, int32(1), aws.ToInt32(profile.RoutingProfile.MediaConcurrencies[0].Concurrency))

			flow, err := cn.DescribeContactFlow(t.Context(), &connectsdk.DescribeContactFlowInput{
				InstanceId: inst, ContactFlowId: aws.String(lastARNSegment(out["FlowRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, connecttypes.ContactFlowTypeContactFlow, flow.ContactFlow.Type)
			assert.Equal(t, connecttypes.ContactFlowStateActive, flow.ContactFlow.State)
		}},
		{name: "update in place", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			out := env.createStack(t, "cn-update", connectCoreTemplate(t, "v1"))
			env.updateStack(t, "cn-update", connectCoreTemplate(t, "v2"))

			inst := aws.String(out["InstanceId"])
			queue, err := cn.DescribeQueue(t.Context(), &connectsdk.DescribeQueueInput{
				InstanceId: inst, QueueId: aws.String(lastARNSegment(out["QueueRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, int32(7), aws.ToInt32(queue.Queue.MaxContacts), "queue updated, not replaced")
			assert.Equal(t, "queue v2", aws.ToString(queue.Queue.Description))
			assert.Equal(t, map[string]string{"ver": "v2"}, queue.Queue.Tags)

			hours, err := cn.DescribeHoursOfOperation(t.Context(), &connectsdk.DescribeHoursOfOperationInput{
				InstanceId: inst, HoursOfOperationId: aws.String(lastARNSegment(out["HoursRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "hours v2", aws.ToString(hours.HoursOfOperation.Description))

			status, err := cn.DescribeAgentStatus(t.Context(), &connectsdk.DescribeAgentStatusInput{
				InstanceId: inst, AgentStatusId: aws.String(lastARNSegment(out["StatusRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, connecttypes.AgentStatusStateDisabled, status.AgentStatus.State)

			flow, err := cn.DescribeContactFlow(t.Context(), &connectsdk.DescribeContactFlowInput{
				InstanceId: inst, ContactFlowId: aws.String(lastARNSegment(out["FlowRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, connecttypes.ContactFlowStateArchived, flow.ContactFlow.State)
			assert.Equal(t, "flow v2", aws.ToString(flow.ContactFlow.Description))

			attr, err := cn.DescribeInstanceAttribute(t.Context(), &connectsdk.DescribeInstanceAttributeInput{
				InstanceId: inst, AttributeType: connecttypes.InstanceAttributeTypeContactflowLogs,
			})
			require.NoError(t, err)
			assert.Equal(t, "true", aws.ToString(attr.Attribute.Value))
		}},
		{name: "delete stack", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			env.createStack(t, "cn-delete", connectCoreTemplate(t, "v1"))
			env.deleteStack(t, "cn-delete")

			insts, err := cn.ListInstances(t.Context(), &connectsdk.ListInstancesInput{})
			require.NoError(t, err)
			assert.Empty(t, insts.InstanceSummaryList)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, cn := newConnectEnv(t)
			tc.run(t, env, cn)
		})
	}
}
