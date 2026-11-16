package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateStack_ConnectHoursSiblingRenameKeepsDeclaredChild(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	shape := hoursShape{child: true}
	out := env.createStack(t, "hs", connectHoursExtrasTemplate(t, shape))
	inst, top := aws.String(out["InstanceId"]), aws.String(lastARNSegment(out["TopArn"]))

	children := func() int {
		kids, err := cn.ListChildHoursOfOperations(t.Context(), &connectsdk.ListChildHoursOfOperationsInput{
			InstanceId: inst, HoursOfOperationId: top,
		})
		require.NoError(t, err)

		return len(kids.ChildHoursOfOperationsSummaryList)
	}

	require.Equal(t, 1, children())

	tpl := connectHoursExtrasTemplate(t, shape)
	renamed := strings.Replace(tpl, `"Name":"leaf"`, `"Name":"leaf2"`, 1)
	require.NotEqual(t, tpl, renamed)

	env.updateStack(t, "hs", renamed)
	assert.Equal(t, 1, children())
}

func TestCreateStack_ConnectFailedExtrasLeaveNoOrphan(t *testing.T) {
	t.Parallel()

	hoursConfig := []any{map[string]any{
		"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
		"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
	}}

	tests := []struct {
		props    func(inst, hours string) map[string]any
		count    func(t *testing.T, cn *connectsdk.Client, inst *string) int
		name     string
		resource string
	}{
		{
			name: "workspace", resource: "AWS::Connect::Workspace",
			props: func(inst, _ string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "orphan", "Title": "T",
					"Pages": []any{map[string]any{
						"Page": "HOME", "ResourceArn": "arn:aws:connect:us-east-1:000000000000:instance/x/view/y",
					}},
				}
			},
			count: func(t *testing.T, cn *connectsdk.Client, inst *string) int {
				t.Helper()

				out, err := cn.SearchWorkspaces(t.Context(), &connectsdk.SearchWorkspacesInput{InstanceId: inst})
				require.NoError(t, err)

				return len(out.Workspaces)
			},
		},
		{
			name: "hours", resource: "AWS::Connect::HoursOfOperation",
			props: func(inst, _ string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "orphan", "TimeZone": "UTC", "Config": hoursConfig,
					"HoursOfOperationOverrides": []any{map[string]any{
						"OverrideName": "o", "OverrideType": "BOGUS", "EffectiveFrom": "2030-01-01",
						"EffectiveTill": "2030-01-02", "OverrideConfig": hoursConfig,
					}},
				}
			},
			count: func(t *testing.T, cn *connectsdk.Client, inst *string) int {
				t.Helper()

				out, err := cn.ListHoursOfOperations(
					t.Context(), &connectsdk.ListHoursOfOperationsInput{InstanceId: inst})
				require.NoError(t, err)

				return len(out.HoursOfOperationSummaryList)
			},
		},
		{
			name: "queue", resource: "AWS::Connect::Queue",
			props: func(inst, hours string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "orphan",
					"HoursOfOperationArn": hours,
					"AdditionalEmailAddresses": []any{map[string]any{
						"EmailAddressArn": "arn:aws:connect:us-east-1:000000000000:instance/x/email-address/nope",
					}},
				}
			},
			count: func(t *testing.T, cn *connectsdk.Client, inst *string) int {
				t.Helper()

				out, err := cn.ListQueues(t.Context(), &connectsdk.ListQueuesInput{InstanceId: inst})
				require.NoError(t, err)

				return len(out.QueueSummaryList)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, cn := newConnectEnv(t)
			out := env.createStack(t, "base", stackTemplate(t, map[string]any{
				"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
					"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-orphan",
					"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
				}),
				"BaseHours": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
					"InstanceArn": cfnRef("Instance"), "Name": "base", "TimeZone": "UTC", "Config": hoursConfig,
				}),
			}, map[string]any{
				"InstanceId": cfnGetAtt("Instance", "Id"), "InstanceArn": cfnGetAtt("Instance", "Arn"),
				"HoursArn": cfnGetAtt("BaseHours", "HoursOfOperationArn"),
			}))

			inst := aws.String(out["InstanceId"])
			before := tc.count(t, cn, inst)

			_, err := env.cfn.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("bad"), DisableRollback: aws.Bool(true),
				TemplateBody: aws.String(stackTemplate(t, map[string]any{
					"R": cfnRes(tc.resource, tc.props(out["InstanceArn"], out["HoursArn"])),
				}, nil)),
			})
			require.NoError(t, err)

			desc, err := env.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("bad")})
			require.NoError(t, err)
			require.NotEqual(t, cfntypes.StackStatusCreateComplete, desc.Stacks[0].StackStatus)
			assert.Equal(t, before, tc.count(t, cn, inst))
		})
	}
}

func TestUpdateStack_ConnectFailedPageReplaceKeepsOldPage(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "wsx", connectWorkspaceExtrasTemplate(t, workspaceShape{}))
	inst, ws, view := aws.String(out["InstanceId"]), aws.String(out["WorkspaceId"]), out["ViewArn"]

	page := func(data string) workspaceShape {
		return workspaceShape{pages: []map[string]any{{"Page": "HOME", "ResourceArn": view, "InputData": data}}}
	}

	env.updateStack(t, "wsx", connectWorkspaceExtrasTemplate(t, page(`{"a":"1"}`)))

	_, err := env.cfn.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("wsx"), DisableRollback: aws.Bool(true),
		TemplateBody: aws.String(connectWorkspaceExtrasTemplate(t, page(`not json`))),
	})
	require.NoError(t, err)

	pages, err := cn.ListWorkspacePages(
		t.Context(), &connectsdk.ListWorkspacePagesInput{InstanceId: inst, WorkspaceId: ws})
	require.NoError(t, err)
	require.Len(t, pages.WorkspacePageList, 1)
	assert.JSONEq(t, `{"a":"1"}`, aws.ToString(pages.WorkspacePageList[0].InputData))
}
