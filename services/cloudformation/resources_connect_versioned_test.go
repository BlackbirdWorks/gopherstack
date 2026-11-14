package cloudformation_test

import (
	"maps"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	connecttypes "github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const connectFarFuture = "4102444800"

// connectVersionedBase is an instance with a flow and a flow module; extra resources and outputs are merged in.
func connectVersionedBase(t *testing.T, extra, outputs map[string]any) string {
	t.Helper()

	res := map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-versioned",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Flow": cfnRes("AWS::Connect::ContactFlow", map[string]any{
			"InstanceArn": cfnRef("Instance"), "Name": "flow", "Type": "CONTACT_FLOW", "Content": connectFlowContent(t),
		}),
		"Module": cfnRes("AWS::Connect::ContactFlowModule", map[string]any{
			"InstanceArn": cfnRef("Instance"), "Name": "module", "Content": connectFlowContent(t),
		}),
	}
	maps.Copy(res, extra)

	outs := map[string]any{
		"InstanceId": cfnGetAtt("Instance", "Id"), "InstanceArn": cfnRef("Instance"),
		"FlowArn": cfnRef("Flow"), "ModuleArn": cfnRef("Module"),
	}
	maps.Copy(outs, outputs)

	return stackTemplate(t, res, outs)
}

func TestCreateStack_ConnectFlowVersions(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	extra := func(desc, alias string, aliasVersion int) map[string]any {
		return map[string]any{
			"FlowVersion": cfnRes("AWS::Connect::ContactFlowVersion", map[string]any{
				"ContactFlowId": cfnRef("Flow"), "Description": desc,
			}),
			"ModuleVersion": cfnRes("AWS::Connect::ContactFlowModuleVersion", map[string]any{
				"ContactFlowModuleId": cfnRef("Module"), "Description": desc,
			}),
			"Alias": withDependsOn(cfnRes("AWS::Connect::ContactFlowModuleAlias", map[string]any{
				"ContactFlowModuleId": cfnRef("Module"), "ContactFlowModuleVersion": aliasVersion,
				"Name": alias, "Description": "alias " + alias,
			}), "ModuleVersion"),
		}
	}
	outputs := map[string]any{
		"FlowVersionRef": cfnRef("FlowVersion"), "FlowVersionArn": cfnGetAtt("FlowVersion", "ContactFlowVersionARN"),
		"FlowVersion": cfnGetAtt("FlowVersion", "Version"), "FlowSha": cfnGetAtt("FlowVersion", "FlowContentSha256"),
		"ModuleVersionArn": cfnGetAtt("ModuleVersion", "ContactFlowModuleVersionARN"),
		"ModuleVersion":    cfnGetAtt("ModuleVersion", "Version"),
		"ModuleSha":        cfnGetAtt("ModuleVersion", "FlowModuleContentSha256"),
		"AliasRef":         cfnRef("Alias"), "AliasId": cfnGetAtt("Alias", "AliasId"),
		"AliasArn": cfnGetAtt("Alias", "ContactFlowModuleAliasARN"),
	}

	out := env.createStack(t, "cfv", connectVersionedBase(t, extra("v two", "live", 1), outputs))
	inst := aws.String(out["InstanceId"])

	assert.Equal(t, out["FlowVersionRef"], out["FlowVersionArn"])
	assert.Equal(t, out["FlowArn"]+":"+out["FlowVersion"], out["FlowVersionArn"])
	assert.Equal(t, out["ModuleArn"]+":"+out["ModuleVersion"], out["ModuleVersionArn"])
	assert.NotEmpty(t, out["FlowSha"])
	assert.NotEmpty(t, out["ModuleSha"])
	assert.NotEmpty(t, out["AliasId"])

	versions, err := cn.ListContactFlowVersions(t.Context(), &connectsdk.ListContactFlowVersionsInput{
		InstanceId: inst, ContactFlowId: aws.String(lastARNSegment(out["FlowArn"])),
	})
	require.NoError(t, err)
	assert.Len(t, versions.ContactFlowVersionSummaryList, 2)

	alias, err := cn.DescribeContactFlowModuleAlias(t.Context(), &connectsdk.DescribeContactFlowModuleAliasInput{
		InstanceId: inst, ContactFlowModuleId: aws.String(lastARNSegment(out["ModuleArn"])),
		AliasId: aws.String(out["AliasId"]),
	})
	require.NoError(t, err)
	assert.Equal(t, "live", aws.ToString(alias.ContactFlowModuleAlias.Name))

	listed, err := cn.ListContactFlowModuleAliases(t.Context(), &connectsdk.ListContactFlowModuleAliasesInput{
		InstanceId: inst, ContactFlowModuleId: aws.String(lastARNSegment(out["ModuleArn"])),
	})
	require.NoError(t, err)
	require.Len(t, listed.ContactFlowModuleAliasSummaryList, 1)
	assert.Equal(t, out["AliasArn"], aws.ToString(listed.ContactFlowModuleAliasSummaryList[0].Arn))

	moduleVersion, err := strconv.Atoi(out["ModuleVersion"])
	require.NoError(t, err)

	env.updateStack(t, "cfv", connectVersionedBase(t, extra("v two", "renamed", moduleVersion), outputs))

	alias, err = cn.DescribeContactFlowModuleAlias(t.Context(), &connectsdk.DescribeContactFlowModuleAliasInput{
		InstanceId: inst, ContactFlowModuleId: aws.String(lastARNSegment(out["ModuleArn"])),
		AliasId: aws.String(out["AliasId"]),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(alias.ContactFlowModuleAlias.Name), "renamed in place")
	assert.EqualValues(t, moduleVersion, aws.ToInt64(alias.ContactFlowModuleAlias.Version))

	env.updateStack(t, "cfv", connectVersionedBase(t, nil, nil))

	versions, err = cn.ListContactFlowVersions(t.Context(), &connectsdk.ListContactFlowVersionsInput{
		InstanceId: inst, ContactFlowId: aws.String(lastARNSegment(out["FlowArn"])),
	})
	require.NoError(t, err)
	assert.Len(t, versions.ContactFlowVersionSummaryList, 1)

	aliases, err := cn.ListContactFlowModuleAliases(t.Context(), &connectsdk.ListContactFlowModuleAliasesInput{
		InstanceId: inst, ContactFlowModuleId: aws.String(lastARNSegment(out["ModuleArn"])),
	})
	require.NoError(t, err)
	assert.Empty(t, aliases.ContactFlowModuleAliasSummaryList)
}

func connectRecordTemplate(t *testing.T, value string, withRecord bool) string {
	t.Helper()

	inst := cfnRef("Instance")
	extra := map[string]any{
		"Table": cfnRes("AWS::Connect::DataTable", map[string]any{
			"InstanceArn": inst, "Name": "table", "TimeZone": "UTC", "ValueLockLevel": "NONE", "Status": "PUBLISHED",
		}),
		"Key": cfnRes("AWS::Connect::DataTableAttribute", map[string]any{
			"InstanceArn": inst, "DataTableArn": cfnGetAtt("Table", "Arn"), "Name": "key", "ValueType": "TEXT",
			"Primary": true,
		}),
		"Col": cfnRes("AWS::Connect::DataTableAttribute", map[string]any{
			"InstanceArn": inst, "DataTableArn": cfnGetAtt("Table", "Arn"), "Name": "col", "ValueType": "TEXT",
		}),
	}
	outputs := map[string]any{"TableArn": cfnGetAtt("Table", "Arn")}

	if withRecord {
		extra["Record"] = cfnRes("AWS::Connect::DataTableRecord", map[string]any{
			"InstanceArn": inst, "DataTableArn": cfnGetAtt("Table", "Arn"),
			"DataTableRecord": map[string]any{
				"PrimaryValues": []any{map[string]any{
					"AttributeId": cfnGetAtt("Key", "AttributeId"), "AttributeValue": "k1",
				}},
				"Values": []any{map[string]any{
					"AttributeId": cfnGetAtt("Col", "AttributeId"), "AttributeValue": value,
				}},
			},
		})
		outputs["RecordRef"] = cfnRef("Record")
		outputs["RecordId"] = cfnGetAtt("Record", "RecordId")
	}

	return connectVersionedBase(t, extra, outputs)
}

func TestCreateStack_ConnectDataTableRecord(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "cfr", connectRecordTemplate(t, "v1", true))
	inst, table := aws.String(out["InstanceId"]), aws.String(lastARNSegment(out["TableArn"]))

	assert.NotEmpty(t, out["RecordId"])
	assert.Contains(t, out["RecordRef"], out["RecordId"])

	read := func() (string, string) {
		got, err := cn.BatchDescribeDataTableValue(t.Context(), &connectsdk.BatchDescribeDataTableValueInput{
			InstanceId: inst, DataTableId: table,
			Values: []connecttypes.DataTableValueIdentifier{{
				AttributeName: aws.String("col"),
				PrimaryValues: []connecttypes.PrimaryValue{{AttributeName: aws.String("key"), Value: aws.String("k1")}},
			}},
		})
		require.NoError(t, err)

		if len(got.Successful) == 0 {
			return "", ""
		}

		return aws.ToString(got.Successful[0].RecordId), aws.ToString(got.Successful[0].Value)
	}

	id, value := read()
	assert.Equal(t, out["RecordId"], id)
	assert.Equal(t, "v1", value)

	env.updateStack(t, "cfr", connectRecordTemplate(t, "v2", true))

	id, value = read()
	assert.Equal(t, out["RecordId"], id, "updated in place")
	assert.Equal(t, "v2", value)

	env.updateStack(t, "cfr", connectRecordTemplate(t, "", false))

	id, _ = read()
	assert.Empty(t, id)
}

func connectContentTemplate(t *testing.T, ver string, withAll bool) string {
	t.Helper()

	inst := cfnRef("Instance")
	extra := map[string]any{}
	outputs := map[string]any{}

	if withAll {
		tags := cfnTags(map[string]string{"ver": ver})
		extra["Note"] = cfnRes("AWS::Connect::Notification", map[string]any{
			"InstanceArn": inst, "ExpiresAt": connectFarFuture, "Priority": "HIGH", "Recipients": []any{inst},
			"Content": map[string]any{"EnUS": "hello " + ver, "DeDE": "hallo " + ver}, "Tags": tags,
		})
		extra["Case"] = cfnRes("AWS::Connect::TestCase", map[string]any{
			"InstanceArn": inst, "Name": "case", "Description": "case " + ver, "Status": "PUBLISHED",
			"Content": `{"k":"` + ver + `"}`, "Tags": tags,
			"EntryPoint": map[string]any{
				"Type": "CHAT", "ChatEntryPointParameters": map[string]any{"FlowId": cfnRef("Flow")},
			},
		})
		extra["Metric"] = cfnRes("AWS::Connect::Metric", map[string]any{
			"InstanceArn": inst, "Name": "metric", "Description": "metric " + ver, "Status": "SAVED",
			"Unit": map[string]string{"v1": "INTEGER", "v2": "PERCENT"}[ver], "Tags": tags,
			"MetricCalculation": map[string]any{
				"Calculation": "SUM(A)",
				"CalculationComponents": []any{map[string]any{
					"Alias": "A", "MetricName": "CONTACTS_HANDLED",
				}},
			},
		})
		extra["Levels"] = cfnRes("AWS::Connect::UserHierarchyStructure", map[string]any{
			"InstanceArn": inst,
			"UserHierarchyStructure": map[string]any{
				"LevelOne": map[string]any{"Name": "Region " + ver}, "LevelTwo": map[string]any{"Name": "Team"},
			},
		})
		outputs = map[string]any{
			"NoteRef": cfnRef("Note"), "NoteArn": cfnGetAtt("Note", "Arn"), "NoteId": cfnGetAtt("Note", "Id"),
			"NoteCreated": cfnGetAtt("Note", "CreatedAt"),
			"CaseRef":     cfnRef("Case"), "CaseArn": cfnGetAtt("Case", "TestCaseArn"),
			"CaseRegion": cfnGetAtt("Case", "LastModifiedRegion"),
			"MetricRef":  cfnRef("Metric"), "MetricArn": cfnGetAtt("Metric", "MetricArn"),
			"MetricType": cfnGetAtt("Metric", "Type"), "MetricCategory": cfnGetAtt("Metric", "Category"),
			"LevelsRef": cfnRef("Levels"), "LevelsArn": cfnGetAtt("Levels", "UserHierarchyStructureArn"),
		}
	}

	return connectVersionedBase(t, extra, outputs)
}

func TestCreateStack_ConnectNotificationTestCaseMetricLevels(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "cfc", connectContentTemplate(t, "v1", true))
	inst := aws.String(out["InstanceId"])

	assert.Equal(t, out["NoteArn"], out["NoteRef"])
	assert.Equal(t, lastARNSegment(out["NoteArn"]), out["NoteId"])
	assert.NotEmpty(t, out["NoteCreated"])
	assert.Equal(t, out["CaseArn"], out["CaseRef"])
	assert.Equal(t, "us-east-1", out["CaseRegion"])
	assert.Equal(t, out["MetricArn"], out["MetricRef"])
	assert.Equal(t, "CUSTOMER_MANAGED", out["MetricType"])
	assert.Equal(t, "CUSTOM", out["MetricCategory"])
	assert.Equal(t, out["InstanceArn"], out["LevelsArn"])

	note, err := cn.DescribeNotification(t.Context(), &connectsdk.DescribeNotificationInput{
		InstanceId: inst, NotificationId: aws.String(out["NoteId"]),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"en_US": "hello v1", "de_DE": "hallo v1"}, note.Notification.Content)
	assert.EqualValues(t, "HIGH", note.Notification.Priority)
	assert.Equal(t, int64(4102444800), note.Notification.ExpiresAt.Unix())

	tc, err := cn.DescribeTestCase(t.Context(), &connectsdk.DescribeTestCaseInput{
		InstanceId: inst, TestCaseId: aws.String(lastARNSegment(out["CaseArn"])),
	})
	require.NoError(t, err)
	assert.Equal(t, "case", aws.ToString(tc.TestCase.Name))
	assert.EqualValues(t, "PUBLISHED", tc.TestCase.Status)
	assert.Equal(
		t,
		lastARNSegment(out["FlowArn"]),
		aws.ToString(tc.TestCase.EntryPoint.ChatEntryPointParameters.FlowId),
	)

	levels, err := cn.DescribeUserHierarchyStructure(t.Context(), &connectsdk.DescribeUserHierarchyStructureInput{
		InstanceId: inst,
	})
	require.NoError(t, err)
	assert.Equal(t, "Region v1", aws.ToString(levels.HierarchyStructure.LevelOne.Name))
	assert.Equal(t, "Team", aws.ToString(levels.HierarchyStructure.LevelTwo.Name))

	env.updateStack(t, "cfc", connectContentTemplate(t, "v2", true))

	note, err = cn.DescribeNotification(t.Context(), &connectsdk.DescribeNotificationInput{
		InstanceId: inst, NotificationId: aws.String(out["NoteId"]),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"en_US": "hello v2", "de_DE": "hallo v2"}, note.Notification.Content)

	tc, err = cn.DescribeTestCase(t.Context(), &connectsdk.DescribeTestCaseInput{
		InstanceId: inst, TestCaseId: aws.String(lastARNSegment(out["CaseArn"])),
	})
	require.NoError(t, err)
	assert.Equal(t, "case v2", aws.ToString(tc.TestCase.Description))

	metric, err := cn.DescribeMetric(t.Context(), &connectsdk.DescribeMetricInput{
		InstanceId: inst, MetricId: aws.String(lastARNSegment(out["MetricArn"])),
	})
	require.NoError(t, err)
	assert.EqualValues(t, "PERCENT", metric.Metric.Unit)
	assert.Equal(t, "metric v2", aws.ToString(metric.Metric.Description))

	levels, err = cn.DescribeUserHierarchyStructure(t.Context(), &connectsdk.DescribeUserHierarchyStructureInput{
		InstanceId: inst,
	})
	require.NoError(t, err)
	assert.Equal(t, "Region v2", aws.ToString(levels.HierarchyStructure.LevelOne.Name))

	env.updateStack(t, "cfc", connectContentTemplate(t, "v2", false))

	_, err = cn.DescribeNotification(t.Context(), &connectsdk.DescribeNotificationInput{
		InstanceId: inst, NotificationId: aws.String(out["NoteId"]),
	})
	require.Error(t, err)

	_, err = cn.DescribeTestCase(t.Context(), &connectsdk.DescribeTestCaseInput{
		InstanceId: inst, TestCaseId: aws.String(lastARNSegment(out["CaseArn"])),
	})
	require.Error(t, err)

	_, err = cn.DescribeMetric(t.Context(), &connectsdk.DescribeMetricInput{
		InstanceId: inst, MetricId: aws.String(lastARNSegment(out["MetricArn"])),
	})
	require.Error(t, err)
}

func withDependsOn(res map[string]any, deps ...string) map[string]any {
	res["DependsOn"] = deps

	return res
}
