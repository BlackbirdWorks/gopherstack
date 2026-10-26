package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	connecttypes "github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPublicKey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n" +
	"-----END PUBLIC KEY-----"

// connectExtendedTemplate renders the people, content and instance-level types; ver changes the mutable properties.
func connectExtendedTemplate(t *testing.T, ver string) string {
	t.Helper()

	inst := cfnRef("Instance")
	tags := cfnTags(map[string]string{"ver": ver})
	perm := "BasicAgentAccess"

	if ver == "v2" {
		perm = "OutboundCallAccess"
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-ext",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Hours": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "h", "TimeZone": "UTC",
			"Config": []any{map[string]any{
				"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
				"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
			}},
		}),
		"Queue": cfnRes("AWS::Connect::Queue", map[string]any{
			"InstanceArn": inst, "Name": "q", "HoursOfOperationArn": cfnGetAtt("Hours", "HoursOfOperationArn"),
		}),
		"Profile": cfnRes("AWS::Connect::RoutingProfile", map[string]any{
			"InstanceArn": inst, "Name": "rp", "Description": "rp",
			"DefaultOutboundQueueArn": cfnGetAtt("Queue", "QueueArn"),
			"MediaConcurrencies":      []any{map[string]any{"Channel": "VOICE", "Concurrency": 1}},
		}),
		"Security": cfnRes("AWS::Connect::SecurityProfile", map[string]any{
			"InstanceArn": inst, "SecurityProfileName": "sp", "Description": "sec " + ver,
			"Permissions":              []string{perm},
			"AllowedAccessControlTags": cfnTags(map[string]string{"team": ver}),
			"Tags":                     tags,
		}),
		"User": cfnRes("AWS::Connect::User", map[string]any{
			"InstanceArn": inst, "Username": "cfn-user", "Password": "Passw0rd1",
			"RoutingProfileArn":   cfnGetAtt("Profile", "RoutingProfileArn"),
			"SecurityProfileArns": []any{cfnGetAtt("Security", "SecurityProfileArn")},
			"PhoneConfig":         map[string]any{"PhoneType": "SOFT_PHONE", "AutoAccept": ver == "v2"},
			"IdentityInfo":        map[string]any{"FirstName": "First " + ver, "LastName": "Last"},
			"Tags":                tags,
		}),
		"Phone": cfnRes("AWS::Connect::PhoneNumber", map[string]any{
			"TargetArn": inst, "Type": "DID", "CountryCode": "US", "Description": "phone " + ver, "Tags": tags,
		}),
		"Rule": cfnRes("AWS::Connect::Rule", map[string]any{
			"InstanceArn": inst, "Name": "rule", "Function": "contains", "PublishStatus": "PUBLISHED",
			"TriggerEventSource": map[string]any{"EventSourceName": "OnPostCallAnalysisAvailable"},
			"Actions":            map[string]any{"EventBridgeActions": []any{map[string]any{"Name": "evt-" + ver}}},
			"Tags":               tags,
		}),
		"View": cfnRes("AWS::Connect::View", map[string]any{
			"InstanceArn": inst, "Name": "view", "Description": "view " + ver,
			"Template": map[string]any{"Head": map[string]any{"Title": ver}}, "Actions": []string{"Go"},
			"Tags": tags,
		}),
		"Vocab": cfnRes("AWS::Connect::Vocabulary", map[string]any{
			"InstanceId": inst, "VocabularyName": "vocab", "LanguageCode": "en-US", "Content": "Phrase", "Tags": tags,
		}),
		"Email": cfnRes("AWS::Connect::EmailAddress", map[string]any{
			"InstanceArn": inst, "EmailAddress": "agent@example.com", "DisplayName": "Agent " + ver, "Tags": tags,
		}),
		"Predefined": cfnRes("AWS::Connect::PredefinedAttribute", map[string]any{
			"InstanceArn": inst, "Name": "attr", "Values": map[string]any{"StringList": []string{ver}},
		}),
		"Task": cfnRes("AWS::Connect::TaskTemplate", map[string]any{
			"InstanceArn": inst, "Name": "task", "Description": "task " + ver,
			"Fields": []any{map[string]any{"Id": map[string]any{"Name": "Description"}, "Type": "DESCRIPTION"}},
			"Tags":   tags,
		}),
		"Origin": cfnRes(
			"AWS::Connect::ApprovedOrigin",
			map[string]any{"InstanceId": inst, "Origin": "https://example.com"},
		),
		"Key": cfnRes("AWS::Connect::SecurityKey", map[string]any{"InstanceId": inst, "Key": testPublicKey}),
		"Storage": cfnRes("AWS::Connect::InstanceStorageConfig", map[string]any{
			"InstanceArn": inst, "ResourceType": "CALL_RECORDINGS", "StorageType": "S3",
			"S3Config": map[string]any{"BucketName": "recordings-" + ver, "BucketPrefix": "calls"},
		}),
		"TDG": cfnRes("AWS::Connect::TrafficDistributionGroup", map[string]any{
			"InstanceArn": inst, "Name": "tdg", "Tags": tags,
		}),
		"Workspace": cfnRes("AWS::Connect::Workspace", map[string]any{
			"InstanceArn": inst, "Name": "ws", "Title": "Title " + ver, "Visibility": "ALL", "Tags": tags,
		}),
		"Form": cfnRes("AWS::Connect::EvaluationForm", map[string]any{
			"InstanceArn": inst, "Title": "form", "Status": "ACTIVE", "Description": "form " + ver,
			"Items": []any{map[string]any{"Section": map[string]any{
				"Title": "S", "RefId": "s1", "Items": []any{map[string]any{"Question": map[string]any{
					"Title": "Q", "RefId": "q1", "QuestionType": "TEXT",
					"Instructions": "answer",
				}}},
			}}},
			"Tags": tags,
		}),
		"Table": cfnRes("AWS::Connect::DataTable", map[string]any{
			"InstanceArn": inst, "Name": "table", "TimeZone": "UTC", "ValueLockLevel": "NONE",
			"Status": "PUBLISHED", "Description": "table " + ver, "Tags": tags,
		}),
		"Column": cfnRes("AWS::Connect::DataTableAttribute", map[string]any{
			"InstanceArn": inst, "DataTableArn": cfnGetAtt("Table", "Arn"), "Name": "col", "ValueType": "TEXT",
			"Primary": true, "Description": "col " + ver,
		}),
	}, map[string]any{
		"SecurityRef": cfnRef("Security"), "SecurityArn": cfnGetAtt("Security", "SecurityProfileArn"),
		"UserRef": cfnRef("User"), "UserArn": cfnGetAtt("User", "UserArn"),
		"PhoneRef": cfnRef("Phone"), "PhoneArn": cfnGetAtt("Phone", "PhoneNumberArn"),
		"PhoneAddress": cfnGetAtt("Phone", "Address"),
		"RuleRef":      cfnRef("Rule"), "RuleArn": cfnGetAtt("Rule", "RuleArn"),
		"ViewRef": cfnRef(
			"View",
		), "ViewId": cfnGetAtt("View", "ViewId"), "ViewSha": cfnGetAtt("View", "ViewContentSha256"),
		"VocabRef": cfnRef(
			"Vocab",
		), "VocabId": cfnGetAtt("Vocab", "VocabularyId"), "VocabState": cfnGetAtt("Vocab", "State"),
		"EmailRef": cfnRef("Email"), "EmailArn": cfnGetAtt("Email", "EmailAddressArn"),
		"PredefinedRef": cfnRef("Predefined"), "PredefinedRegion": cfnGetAtt("Predefined", "LastModifiedRegion"),
		"TaskRef": cfnRef("Task"), "TaskArn": cfnGetAtt("Task", "Arn"),
		"KeyAssoc":     cfnGetAtt("Key", "AssociationId"),
		"StorageAssoc": cfnGetAtt("Storage", "AssociationId"),
		"TDGRef":       cfnRef("TDG"), "TDGStatus": cfnGetAtt("TDG", "Status"),
		"WorkspaceRef": cfnRef("Workspace"), "WorkspaceId": cfnGetAtt("Workspace", "Id"),
		"FormRef": cfnRef("Form"), "FormArn": cfnGetAtt("Form", "EvaluationFormArn"),
		"TableRef": cfnRef("Table"), "TableLock": cfnGetAtt("Table", "LockVersion.DataTable"),
		"ColumnRef": cfnRef("Column"), "ColumnId": cfnGetAtt("Column", "AttributeId"),
		"InstanceId": cfnGetAtt("Instance", "Id"),
	})
}

func TestCreateStack_ConnectExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, env familyStackEnv, cn *connectsdk.Client)
		name string
	}{
		{name: "create ref and getatt", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			out := env.createStack(t, "cx-create", connectExtendedTemplate(t, "v1"))
			inst := aws.String(out["InstanceId"])

			assert.Equal(t, out["SecurityArn"], out["SecurityRef"])
			assert.Equal(t, out["UserArn"], out["UserRef"])
			assert.Equal(t, out["PhoneArn"], out["PhoneRef"])
			assert.Regexp(t, `^\+1`, out["PhoneAddress"])
			assert.Equal(t, out["RuleArn"], out["RuleRef"])
			assert.Equal(t, lastARNSegment(out["ViewRef"]), out["ViewId"])
			assert.NotEmpty(t, out["ViewSha"])
			assert.Equal(t, out["VocabId"], lastARNSegment(out["VocabRef"]))
			assert.NotEmpty(t, out["VocabState"])
			assert.Equal(t, out["EmailArn"], out["EmailRef"])
			assert.Equal(t, out["InstanceId"]+"|attr", out["PredefinedRef"])
			assert.Equal(t, "us-east-1", out["PredefinedRegion"])
			assert.Equal(t, out["TaskArn"], out["TaskRef"])
			assert.NotEmpty(t, out["KeyAssoc"])
			assert.NotEmpty(t, out["StorageAssoc"])
			assert.Equal(t, lastARNSegment(out["WorkspaceRef"]), out["WorkspaceId"])
			assert.Equal(t, out["FormArn"], out["FormRef"])
			assert.NotEmpty(t, out["TableLock"])
			assert.NotEmpty(t, out["ColumnId"])

			sec, err := cn.DescribeSecurityProfile(t.Context(), &connectsdk.DescribeSecurityProfileInput{
				InstanceId: inst, SecurityProfileId: aws.String(lastARNSegment(out["SecurityRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"team": "v1"}, sec.SecurityProfile.AllowedAccessControlTags)

			user, err := cn.DescribeUser(t.Context(), &connectsdk.DescribeUserInput{
				InstanceId: inst, UserId: aws.String(lastARNSegment(out["UserRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "cfn-user", aws.ToString(user.User.Username))
			assert.Equal(t, "First v1", aws.ToString(user.User.IdentityInfo.FirstName))
			assert.Equal(t, lastARNSegment(out["SecurityRef"]), user.User.SecurityProfileIds[0])

			phone, err := cn.DescribePhoneNumber(t.Context(), &connectsdk.DescribePhoneNumberInput{
				PhoneNumberId: aws.String(lastARNSegment(out["PhoneRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, out["PhoneAddress"], aws.ToString(phone.ClaimedPhoneNumberSummary.PhoneNumber))
			assert.Equal(t, connecttypes.PhoneNumberTypeDid, phone.ClaimedPhoneNumberSummary.PhoneNumberType)

			rule, err := cn.DescribeRule(t.Context(), &connectsdk.DescribeRuleInput{
				InstanceId: inst, RuleId: aws.String(lastARNSegment(out["RuleRef"])),
			})
			require.NoError(t, err)
			require.Len(t, rule.Rule.Actions, 1)
			assert.Equal(t, connecttypes.ActionTypeGenerateEventbridgeEvent, rule.Rule.Actions[0].ActionType)
			assert.Equal(t, "evt-v1", aws.ToString(rule.Rule.Actions[0].EventBridgeAction.Name))

			origins, err := cn.ListApprovedOrigins(t.Context(), &connectsdk.ListApprovedOriginsInput{InstanceId: inst})
			require.NoError(t, err)
			assert.Equal(t, []string{"https://example.com"}, origins.Origins)

			keys, err := cn.ListSecurityKeys(t.Context(), &connectsdk.ListSecurityKeysInput{InstanceId: inst})
			require.NoError(t, err)
			require.Len(t, keys.SecurityKeys, 1)
			assert.Equal(t, out["KeyAssoc"], aws.ToString(keys.SecurityKeys[0].AssociationId))

			form, err := cn.DescribeEvaluationForm(t.Context(), &connectsdk.DescribeEvaluationFormInput{
				InstanceId: inst, EvaluationFormId: aws.String(lastARNSegment(out["FormRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, connecttypes.EvaluationFormVersionStatusActive, form.EvaluationForm.Status)

			attr, err := cn.DescribeDataTableAttribute(t.Context(), &connectsdk.DescribeDataTableAttributeInput{
				InstanceId:    inst,
				DataTableId:   aws.String(lastARNSegment(out["TableRef"])),
				AttributeName: aws.String("col"),
			})
			require.NoError(t, err)
			assert.Equal(t, out["ColumnId"], aws.ToString(attr.Attribute.AttributeId))
		}},
		{name: "update in place", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			out := env.createStack(t, "cx-update", connectExtendedTemplate(t, "v1"))
			env.updateStack(t, "cx-update", connectExtendedTemplate(t, "v2"))
			inst := aws.String(out["InstanceId"])

			sec, err := cn.DescribeSecurityProfile(t.Context(), &connectsdk.DescribeSecurityProfileInput{
				InstanceId: inst, SecurityProfileId: aws.String(lastARNSegment(out["SecurityRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "sec v2", aws.ToString(sec.SecurityProfile.Description))
			assert.Equal(t, map[string]string{"team": "v2"}, sec.SecurityProfile.AllowedAccessControlTags)

			user, err := cn.DescribeUser(t.Context(), &connectsdk.DescribeUserInput{
				InstanceId: inst, UserId: aws.String(lastARNSegment(out["UserRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "First v2", aws.ToString(user.User.IdentityInfo.FirstName), "user updated, not replaced")
			assert.True(t, user.User.PhoneConfig.AutoAccept)

			rule, err := cn.DescribeRule(t.Context(), &connectsdk.DescribeRuleInput{
				InstanceId: inst, RuleId: aws.String(lastARNSegment(out["RuleRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "evt-v2", aws.ToString(rule.Rule.Actions[0].EventBridgeAction.Name))

			phone, err := cn.DescribePhoneNumber(t.Context(), &connectsdk.DescribePhoneNumberInput{
				PhoneNumberId: aws.String(lastARNSegment(out["PhoneRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "phone v2", aws.ToString(phone.ClaimedPhoneNumberSummary.PhoneNumberDescription))

			view, err := cn.DescribeView(t.Context(), &connectsdk.DescribeViewInput{
				InstanceId: inst, ViewId: aws.String(out["ViewId"]),
			})
			require.NoError(t, err)
			assert.Equal(t, "view v2", aws.ToString(view.View.Description))

			email, err := cn.DescribeEmailAddress(t.Context(), &connectsdk.DescribeEmailAddressInput{
				InstanceId: inst, EmailAddressId: aws.String(lastARNSegment(out["EmailRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "Agent v2", aws.ToString(email.DisplayName))

			pa, err := cn.DescribePredefinedAttribute(t.Context(), &connectsdk.DescribePredefinedAttributeInput{
				InstanceId: inst, Name: aws.String("attr"),
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				[]string{"v2"},
				pa.PredefinedAttribute.Values.(*connecttypes.PredefinedAttributeValuesMemberStringList).Value,
			)

			task, err := cn.GetTaskTemplate(t.Context(), &connectsdk.GetTaskTemplateInput{
				InstanceId: inst, TaskTemplateId: aws.String(lastARNSegment(out["TaskRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "task v2", aws.ToString(task.Description))

			ws, err := cn.DescribeWorkspace(t.Context(), &connectsdk.DescribeWorkspaceInput{
				InstanceId: inst, WorkspaceId: aws.String(out["WorkspaceId"]),
			})
			require.NoError(t, err)
			assert.Equal(t, "Title v2", aws.ToString(ws.Workspace.Title))

			table, err := cn.DescribeDataTable(t.Context(), &connectsdk.DescribeDataTableInput{
				InstanceId: inst, DataTableId: aws.String(lastARNSegment(out["TableRef"])),
			})
			require.NoError(t, err)
			assert.Equal(t, "table v2", aws.ToString(table.DataTable.Description))

			cfg, err := cn.DescribeInstanceStorageConfig(t.Context(), &connectsdk.DescribeInstanceStorageConfigInput{
				InstanceId: inst, AssociationId: aws.String(out["StorageAssoc"]),
				ResourceType: connecttypes.InstanceStorageResourceTypeCallRecordings,
			})
			require.NoError(t, err)
			assert.Equal(t, "recordings-v2", aws.ToString(cfg.StorageConfig.S3Config.BucketName))
		}},
		{name: "remove resources", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			out := env.createStack(t, "cx-remove", connectExtendedTemplate(t, "v1"))
			inst := aws.String(out["InstanceId"])

			env.updateStack(t, "cx-remove", stackTemplate(t, map[string]any{
				"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
					"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-ext",
					"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
				}),
			}, nil))

			_, err := cn.DescribeUser(t.Context(), &connectsdk.DescribeUserInput{
				InstanceId: inst, UserId: aws.String(lastARNSegment(out["UserRef"])),
			})
			require.Error(t, err)

			_, err = cn.DescribeRule(t.Context(), &connectsdk.DescribeRuleInput{
				InstanceId: inst, RuleId: aws.String(lastARNSegment(out["RuleRef"])),
			})
			require.Error(t, err)

			_, err = cn.DescribeEvaluationForm(t.Context(), &connectsdk.DescribeEvaluationFormInput{
				InstanceId: inst, EvaluationFormId: aws.String(lastARNSegment(out["FormRef"])),
			})
			require.Error(t, err, "active form is deactivated, then deleted")

			origins, err := cn.ListApprovedOrigins(t.Context(), &connectsdk.ListApprovedOriginsInput{InstanceId: inst})
			require.NoError(t, err)
			assert.Empty(t, origins.Origins)

			keys, err := cn.ListSecurityKeys(t.Context(), &connectsdk.ListSecurityKeysInput{InstanceId: inst})
			require.NoError(t, err)
			assert.Empty(t, keys.SecurityKeys)

			storage, err := cn.ListInstanceStorageConfigs(t.Context(), &connectsdk.ListInstanceStorageConfigsInput{
				InstanceId: inst, ResourceType: connecttypes.InstanceStorageResourceTypeCallRecordings,
			})
			require.NoError(t, err)
			assert.Empty(t, storage.StorageConfigs)

			phones, err := cn.ListPhoneNumbersV2(t.Context(), &connectsdk.ListPhoneNumbersV2Input{})
			require.NoError(t, err)
			assert.Empty(t, phones.ListPhoneNumbersSummaryList)

			tdgs, err := cn.ListTrafficDistributionGroups(t.Context(), &connectsdk.ListTrafficDistributionGroupsInput{})
			require.NoError(t, err)
			assert.Empty(t, tdgs.TrafficDistributionGroupSummaryList)

			_, err = cn.DescribeWorkspace(t.Context(), &connectsdk.DescribeWorkspaceInput{
				InstanceId: inst, WorkspaceId: aws.String(out["WorkspaceId"]),
			})
			require.Error(t, err)

			_, err = cn.DescribeDataTable(t.Context(), &connectsdk.DescribeDataTableInput{
				InstanceId: inst, DataTableId: aws.String(lastARNSegment(out["TableRef"])),
			})
			require.Error(t, err)

			_, err = cn.DescribePredefinedAttribute(t.Context(), &connectsdk.DescribePredefinedAttributeInput{
				InstanceId: inst, Name: aws.String("attr"),
			})
			require.Error(t, err)

			_, err = cn.GetTaskTemplate(t.Context(), &connectsdk.GetTaskTemplateInput{
				InstanceId: inst, TaskTemplateId: aws.String(lastARNSegment(out["TaskRef"])),
			})
			require.Error(t, err)

			_, err = cn.DescribeView(t.Context(), &connectsdk.DescribeViewInput{
				InstanceId: inst, ViewId: aws.String(out["ViewId"]),
			})
			require.Error(t, err)
		}},
		{name: "delete stack", run: func(t *testing.T, env familyStackEnv, cn *connectsdk.Client) {
			t.Helper()

			env.createStack(t, "cx-delete", connectExtendedTemplate(t, "v1"))
			env.deleteStack(t, "cx-delete")

			insts, err := cn.ListInstances(t.Context(), &connectsdk.ListInstancesInput{})
			require.NoError(t, err)
			assert.Empty(t, insts.InstanceSummaryList)

			phones, err := cn.ListPhoneNumbersV2(t.Context(), &connectsdk.ListPhoneNumbersV2Input{})
			require.NoError(t, err)
			assert.Empty(t, phones.ListPhoneNumbersSummaryList)
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

func connectHierarchyTemplate(t *testing.T, groupName string, withGroup bool) string {
	t.Helper()

	resources := map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-hier",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
	}
	outputs := map[string]any{"InstanceId": cfnGetAtt("Instance", "Id")}

	if withGroup {
		resources["Group"] = cfnRes("AWS::Connect::UserHierarchyGroup", map[string]any{
			"InstanceArn": cfnRef("Instance"), "Name": groupName,
		})
		outputs["GroupRef"] = cfnRef("Group")
		outputs["GroupArn"] = cfnGetAtt("Group", "UserHierarchyGroupArn")
	}

	return stackTemplate(t, resources, outputs)
}

func TestCreateStack_ConnectHierarchyGroup(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)
	out := env.createStack(t, "ch-stack", connectHierarchyTemplate(t, "", false))
	inst := aws.String(out["InstanceId"])

	_, err := cn.UpdateUserHierarchyStructure(t.Context(), &connectsdk.UpdateUserHierarchyStructureInput{
		InstanceId: inst,
		HierarchyStructure: &connecttypes.HierarchyStructureUpdate{
			LevelOne: &connecttypes.HierarchyLevelUpdate{Name: aws.String("Region")},
		},
	})
	require.NoError(t, err)

	env.updateStack(t, "ch-stack", connectHierarchyTemplate(t, "emea", true))

	desc, err := env.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("ch-stack")})
	require.NoError(t, err)

	got := map[string]string{}
	for _, o := range desc.Stacks[0].Outputs {
		got[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	assert.Equal(t, got["GroupArn"], got["GroupRef"])

	group, err := cn.DescribeUserHierarchyGroup(t.Context(), &connectsdk.DescribeUserHierarchyGroupInput{
		InstanceId: inst, HierarchyGroupId: aws.String(lastARNSegment(got["GroupRef"])),
	})
	require.NoError(t, err)
	assert.Equal(t, "emea", aws.ToString(group.HierarchyGroup.Name))

	env.updateStack(t, "ch-stack", connectHierarchyTemplate(t, "apac", true))

	group, err = cn.DescribeUserHierarchyGroup(t.Context(), &connectsdk.DescribeUserHierarchyGroupInput{
		InstanceId: inst, HierarchyGroupId: aws.String(lastARNSegment(got["GroupRef"])),
	})
	require.NoError(t, err)
	assert.Equal(t, "apac", aws.ToString(group.HierarchyGroup.Name), "renamed in place")

	env.updateStack(t, "ch-stack", connectHierarchyTemplate(t, "", false))

	_, err = cn.DescribeUserHierarchyGroup(t.Context(), &connectsdk.DescribeUserHierarchyGroupInput{
		InstanceId: inst, HierarchyGroupId: aws.String(lastARNSegment(got["GroupRef"])),
	})
	require.Error(t, err)
}
