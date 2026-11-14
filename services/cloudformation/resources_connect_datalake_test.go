package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connectDataLakeTemplate(t *testing.T, target string) string {
	t.Helper()

	props := map[string]any{"InstanceId": cfnRef("Instance"), "DataSetId": "contact_record"}
	if target != "" {
		props["TargetAccountId"] = target
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-lake",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Lake": cfnRes("AWS::Connect::DataLakeAssociation", props),
	}, map[string]any{
		"InstanceArn": cfnRef("Instance"), "LakeRef": cfnRef("Lake"),
		"ShareArn": cfnGetAtt("Lake", "ResourceShareArn"), "ShareId": cfnGetAtt("Lake", "ResourceShareId"),
	})
}

func TestConnectDataLakeAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, target, wantTarget string
	}{
		{name: "default account", wantTarget: "000000000000"},
		{name: "other account", target: "111122223333", wantTarget: "111122223333"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env, cn := newConnectEnv(t)
			out := env.createStack(t, "lake", connectDataLakeTemplate(t, tt.target))
			inst := aws.String(lastARNSegment(out["InstanceArn"]))

			assert.Equal(t, lastARNSegment(out["InstanceArn"])+"|contact_record|"+tt.wantTarget, out["LakeRef"])

			list, err := cn.ListAnalyticsDataAssociations(t.Context(),
				&connectsdk.ListAnalyticsDataAssociationsInput{InstanceId: inst})
			require.NoError(t, err)
			require.Len(t, list.Results, 1)
			assert.Equal(t, out["ShareId"], aws.ToString(list.Results[0].ResourceShareId))
			assert.Equal(t, out["ShareArn"], aws.ToString(list.Results[0].ResourceShareArn))
			assert.Equal(t, tt.wantTarget, aws.ToString(list.Results[0].TargetAccountId))

			env.deleteStack(t, "lake")

			list, err = cn.ListAnalyticsDataAssociations(t.Context(),
				&connectsdk.ListAnalyticsDataAssociationsInput{InstanceId: inst})
			if err == nil {
				assert.Empty(t, list.Results)
			}
		})
	}
}
