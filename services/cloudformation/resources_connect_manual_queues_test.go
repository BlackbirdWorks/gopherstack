package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connectManualQueuesTemplate(t *testing.T, queues ...string) string {
	t.Helper()

	inst := cfnRef("Instance")

	manual := make([]any, 0, len(queues))
	for _, q := range queues {
		manual = append(manual, map[string]any{"QueueReference": map[string]any{
			"QueueArn": cfnGetAtt(q, "QueueArn"), "Channel": "TASK",
		}})
	}

	profile := map[string]any{
		"InstanceArn": inst, "Name": "rp", "Description": "rp",
		"DefaultOutboundQueueArn": cfnGetAtt("QueueA", "QueueArn"),
		"MediaConcurrencies":      []any{map[string]any{"Channel": "VOICE", "Concurrency": 1}},
	}
	if len(manual) > 0 {
		profile["ManualAssignmentQueueConfigs"] = manual
	}

	queue := func(name string) map[string]any {
		return cfnRes("AWS::Connect::Queue", map[string]any{
			"InstanceArn": inst, "Name": name, "HoursOfOperationArn": cfnGetAtt("Hours", "HoursOfOperationArn"),
		})
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-manual",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Hours": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "h", "TimeZone": "UTC",
			"Config": []any{map[string]any{
				"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
				"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
			}},
		}),
		"QueueA":  queue("qa"),
		"QueueB":  queue("qb"),
		"Profile": cfnRes("AWS::Connect::RoutingProfile", profile),
	}, map[string]any{
		"InstanceId": cfnGetAtt("Instance", "Id"), "ProfileArn": cfnGetAtt("Profile", "RoutingProfileArn"),
		"QueueAArn": cfnGetAtt("QueueA", "QueueArn"), "QueueBArn": cfnGetAtt("QueueB", "QueueArn"),
	})
}

func TestCreateStack_ConnectRoutingProfileManualAssignmentQueues(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "manual", connectManualQueuesTemplate(t, "QueueA"))
	inst, profile := aws.String(out["InstanceId"]), aws.String(lastARNSegment(out["ProfileArn"]))

	tests := []struct {
		name   string
		queues []string
		want   []string
	}{
		{name: "one", queues: []string{"QueueA"}, want: []string{out["QueueAArn"]}},
		{
			name:   "swap_and_add",
			queues: []string{"QueueB", "QueueA"},
			want:   []string{out["QueueAArn"], out["QueueBArn"]},
		},
		{name: "drop_one", queues: []string{"QueueB"}, want: []string{out["QueueBArn"]}},
		{name: "none", queues: nil, want: nil},
	}

	for _, tc := range tests {
		if tc.name != "one" {
			env.updateStack(t, "manual", connectManualQueuesTemplate(t, tc.queues...))
		}

		got, err := cn.ListRoutingProfileManualAssignmentQueues(
			t.Context(), &connectsdk.ListRoutingProfileManualAssignmentQueuesInput{
				InstanceId: inst, RoutingProfileId: profile,
			})
		require.NoError(t, err, tc.name)

		arns := make([]string, 0, len(got.RoutingProfileManualAssignmentQueueConfigSummaryList))
		for _, q := range got.RoutingProfileManualAssignmentQueueConfigSummaryList {
			arns = append(arns, aws.ToString(q.QueueArn))
		}

		assert.ElementsMatch(t, tc.want, arns, tc.name)
	}
}
