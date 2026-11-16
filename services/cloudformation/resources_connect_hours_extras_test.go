package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hoursShape struct {
	desc      string
	overrides []string
	parent    bool
	child     bool
}

func connectHoursExtrasTemplate(t *testing.T, s hoursShape) string {
	t.Helper()

	inst := cfnRef("Instance")
	config := []any{map[string]any{
		"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
		"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
	}}

	main := map[string]any{"InstanceArn": inst, "Name": "main", "TimeZone": "UTC", "Config": config}
	if s.parent {
		main["ParentHoursOfOperations"] = []any{map[string]any{"Id": cfnGetAtt("Parent", "HoursOfOperationArn")}}
	}

	if len(s.overrides) > 0 {
		ovs := make([]any, 0, len(s.overrides))

		for _, name := range s.overrides {
			ovs = append(ovs, map[string]any{
				"OverrideName": name, "OverrideDescription": s.desc, "OverrideType": "STANDARD",
				"EffectiveFrom": "2030-01-01", "EffectiveTill": "2030-01-02",
				"OverrideConfig": []any{map[string]any{
					"Day": "TUESDAY", "StartTime": map[string]any{"Hours": 10, "Minutes": 0},
					"EndTime": map[string]any{"Hours": 12, "Minutes": 0},
				}},
			})
		}

		main["HoursOfOperationOverrides"] = ovs
	}

	top := map[string]any{"InstanceArn": inst, "Name": "top", "TimeZone": "UTC", "Config": config}
	if s.child {
		top["ChildHoursOfOperations"] = []any{map[string]any{"Id": cfnGetAtt("Leaf", "HoursOfOperationArn")}}
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-hx",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Parent": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "parent", "TimeZone": "UTC", "Config": config,
		}),
		"Leaf": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "leaf", "TimeZone": "UTC", "Config": config,
		}),
		"Main": cfnRes("AWS::Connect::HoursOfOperation", main),
		"Top":  cfnRes("AWS::Connect::HoursOfOperation", top),
	}, map[string]any{
		"InstanceId": cfnGetAtt("Instance", "Id"),
		"MainArn":    cfnGetAtt("Main", "HoursOfOperationArn"),
		"ParentArn": cfnGetAtt(
			"Parent",
			"HoursOfOperationArn",
		),
		"TopArn":  cfnGetAtt("Top", "HoursOfOperationArn"),
		"LeafArn": cfnGetAtt("Leaf", "HoursOfOperationArn"),
	})
}

func TestCreateStack_ConnectHoursOverridesParentsChildren(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "hx", connectHoursExtrasTemplate(t, hoursShape{}))
	inst, main := aws.String(out["InstanceId"]), aws.String(lastARNSegment(out["MainArn"]))

	tests := []struct {
		name         string
		wantDesc     string
		wantOverride []string
		shape        hoursShape
		wantParents  int
	}{
		{
			name:         "created",
			shape:        hoursShape{overrides: []string{"holiday", "storm"}, desc: "v1", parent: true},
			wantOverride: []string{"holiday", "storm"}, wantDesc: "v1", wantParents: 1,
		},
		{
			name:         "changed",
			shape:        hoursShape{overrides: []string{"holiday"}, desc: "v2", child: true},
			wantOverride: []string{"holiday"}, wantDesc: "v2", wantParents: 0,
		},
	}

	for _, tc := range tests {
		env.updateStack(t, "hx", connectHoursExtrasTemplate(t, tc.shape))

		ovs, err := cn.ListHoursOfOperationOverrides(t.Context(), &connectsdk.ListHoursOfOperationOverridesInput{
			InstanceId: inst, HoursOfOperationId: main,
		})
		require.NoError(t, err, tc.name)

		names := make([]string, 0, len(ovs.HoursOfOperationOverrideList))

		for _, o := range ovs.HoursOfOperationOverrideList {
			names = append(names, aws.ToString(o.Name))
			assert.Equal(t, tc.wantDesc, aws.ToString(o.Description), tc.name)
		}

		assert.ElementsMatch(t, tc.wantOverride, names, tc.name)

		desc, err := cn.DescribeHoursOfOperation(t.Context(), &connectsdk.DescribeHoursOfOperationInput{
			InstanceId: inst, HoursOfOperationId: main,
		})
		require.NoError(t, err, tc.name)
		assert.Len(t, desc.HoursOfOperation.ParentHoursOfOperations, tc.wantParents, tc.name)
	}

	// Top lists Main as a child only while the template says so.
	env.updateStack(t, "hx", connectHoursExtrasTemplate(t, hoursShape{child: true}))

	top := aws.String(lastARNSegment(out["TopArn"]))
	kids, err := cn.ListChildHoursOfOperations(t.Context(), &connectsdk.ListChildHoursOfOperationsInput{
		InstanceId: inst, HoursOfOperationId: top,
	})
	require.NoError(t, err)
	require.Len(t, kids.ChildHoursOfOperationsSummaryList, 1)
	assert.Equal(t, lastARNSegment(out["LeafArn"]), aws.ToString(kids.ChildHoursOfOperationsSummaryList[0].Id))

	env.updateStack(t, "hx", connectHoursExtrasTemplate(t, hoursShape{}))

	kids, err = cn.ListChildHoursOfOperations(t.Context(), &connectsdk.ListChildHoursOfOperationsInput{
		InstanceId: inst, HoursOfOperationId: top,
	})
	require.NoError(t, err)
	assert.Empty(t, kids.ChildHoursOfOperationsSummaryList)
}
