package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connectQueueEmailsTemplate(t *testing.T, addresses ...string) string {
	t.Helper()

	inst := cfnRef("Instance")

	emails := make([]any, 0, len(addresses))
	for _, a := range addresses {
		emails = append(emails, map[string]any{"EmailAddressArn": cfnGetAtt(a, "EmailAddressArn")})
	}

	queue := map[string]any{
		"InstanceArn": inst, "Name": "q", "HoursOfOperationArn": cfnGetAtt("Hours", "HoursOfOperationArn"),
	}
	if len(emails) > 0 {
		queue["AdditionalEmailAddresses"] = emails
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-qmail",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Hours": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "h", "TimeZone": "UTC",
			"Config": []any{map[string]any{
				"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
				"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
			}},
		}),
		"MailA": cfnRes(
			"AWS::Connect::EmailAddress",
			map[string]any{"InstanceArn": inst, "EmailAddress": "a@example.com"},
		),
		"MailB": cfnRes(
			"AWS::Connect::EmailAddress",
			map[string]any{"InstanceArn": inst, "EmailAddress": "b@example.com"},
		),
		"Queue": cfnRes("AWS::Connect::Queue", queue),
	}, map[string]any{
		"InstanceId": cfnGetAtt("Instance", "Id"), "QueueId": cfnGetAtt("Queue", "QueueArn"),
		"MailAId": cfnRef("MailA"), "MailBId": cfnRef("MailB"),
	})
}

func TestCreateStack_ConnectQueueAdditionalEmailAddresses(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "qmail", connectQueueEmailsTemplate(t, "MailA"))
	inst, queue := aws.String(out["InstanceId"]), aws.String(lastARNSegment(out["QueueId"]))

	tests := []struct {
		name      string
		addresses []string
		want      []string
	}{
		{name: "one", addresses: []string{"MailA"}, want: []string{out["MailAId"]}},
		{name: "swap_and_add", addresses: []string{"MailB", "MailA"}, want: []string{out["MailAId"], out["MailBId"]}},
		{name: "drop_one", addresses: []string{"MailB"}, want: []string{out["MailBId"]}},
		{name: "none", addresses: nil, want: nil},
	}

	for _, tc := range tests {
		if tc.name != "one" {
			env.updateStack(t, "qmail", connectQueueEmailsTemplate(t, tc.addresses...))
		}

		got, err := cn.ListQueueEmailAddresses(t.Context(), &connectsdk.ListQueueEmailAddressesInput{
			InstanceId: inst, QueueId: queue,
		})
		require.NoError(t, err, tc.name)

		ids := make([]string, 0, len(got.EmailAddressMetadataList))
		for _, e := range got.EmailAddressMetadataList {
			ids = append(ids, aws.ToString(e.Arn))
		}

		assert.ElementsMatch(t, tc.want, ids, tc.name)
	}
}
