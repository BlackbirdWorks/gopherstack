package ssm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func newDroppedMembersClient(t *testing.T) *ssmsdk.Client {
	t.Helper()

	return newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
}

func TestUpdateOpsItem_OperationalDataToDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		toDelete []string
		wantKeys []string
	}{
		{name: "delete_one", toDelete: []string{"a"}, wantKeys: []string{"b"}},
		{name: "delete_none", toDelete: nil, wantKeys: []string{"a", "b"}},
		{name: "delete_unknown_ignored", toDelete: []string{"zzz"}, wantKeys: []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newDroppedMembersClient(t)

			created, err := client.CreateOpsItem(ctx, &ssmsdk.CreateOpsItemInput{
				Title: aws.String("t"), Description: aws.String("d"), Source: aws.String("s"),
				OperationalData: map[string]ssmtypes.OpsItemDataValue{
					"a": {Value: aws.String("1")}, "b": {Value: aws.String("2")},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateOpsItem(ctx, &ssmsdk.UpdateOpsItemInput{
				OpsItemId: created.OpsItemId, OperationalDataToDelete: tt.toDelete,
			})
			require.NoError(t, err)

			got, err := client.GetOpsItem(ctx, &ssmsdk.GetOpsItemInput{OpsItemId: created.OpsItemId})
			require.NoError(t, err)

			keys := make([]string, 0, len(got.OpsItem.OperationalData))
			for k := range got.OpsItem.OperationalData {
				keys = append(keys, k)
			}

			assert.ElementsMatch(t, tt.wantKeys, keys)
		})
	}
}

func TestDescribeDocument_ParametersAndRequires(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  ssmtypes.DocumentFormat
		content string
	}{
		{
			name:   "json",
			format: ssmtypes.DocumentFormatJson,
			content: `{"schemaVersion":"2.2","parameters":{"Zeta":{"type":"StringList",` +
				`"description":"z","default":["a","b"]},` +
				`"Alpha":{"type":"Boolean","description":"flag","default":true}},` +
				`"mainSteps":[{"action":"aws:runShellScript","name":"s","inputs":{"runCommand":["echo"]}}]}`,
		},
		{
			name:   "yaml",
			format: ssmtypes.DocumentFormatYaml,
			content: "schemaVersion: '2.2'\nparameters:\n  Zeta:\n    type: StringList\n    description: z\n" +
				"    default: [a, b]\n  Alpha:\n    type: Boolean\n    description: flag\n    default: true\n" +
				"mainSteps:\n- action: aws:runShellScript\n  name: s\n  inputs:\n    runCommand: [echo]\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newDroppedMembersClient(t)

			_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
				Name: aws.String("base-doc"), Content: aws.String(tt.content),
				DocumentType: ssmtypes.DocumentTypeCommand, DocumentFormat: tt.format,
			})
			require.NoError(t, err)

			_, err = client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
				Name: aws.String("dep-doc"), Content: aws.String(tt.content),
				DocumentType: ssmtypes.DocumentTypeCommand, DocumentFormat: tt.format,
				Requires: []ssmtypes.DocumentRequires{{
					Name: aws.String(
						"base-doc",
					), RequireType: aws.String("Mandatory"), VersionName: aws.String("v-one"),
				}},
			})
			require.NoError(t, err)

			got, err := client.DescribeDocument(ctx, &ssmsdk.DescribeDocumentInput{Name: aws.String("dep-doc")})
			require.NoError(t, err)
			require.Len(t, got.Document.Parameters, 2)

			assert.Equal(t, "Alpha", aws.ToString(got.Document.Parameters[0].Name))
			assert.Equal(t, ssmtypes.DocumentParameterTypeString, got.Document.Parameters[0].Type)
			assert.Equal(t, "true", aws.ToString(got.Document.Parameters[0].DefaultValue))
			assert.Equal(t, "flag", aws.ToString(got.Document.Parameters[0].Description))
			assert.Equal(t, "Zeta", aws.ToString(got.Document.Parameters[1].Name))
			assert.Equal(t, ssmtypes.DocumentParameterTypeStringList, got.Document.Parameters[1].Type)

			require.Len(t, got.Document.Requires, 1)
			assert.Equal(t, "Mandatory", aws.ToString(got.Document.Requires[0].RequireType))
			assert.Equal(t, "v-one", aws.ToString(got.Document.Requires[0].VersionName))
		})
	}
}

func TestDescribePatchBaselines_DescriptionAndDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		registerDefault bool
	}{
		{name: "not_default"},
		{name: "registered_default", registerDefault: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newDroppedMembersClient(t)

			created, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
				Name: aws.String("custom"), Description: aws.String("my desc"),
				OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2,
			})
			require.NoError(t, err)

			if tt.registerDefault {
				_, err = client.RegisterDefaultPatchBaseline(ctx, &ssmsdk.RegisterDefaultPatchBaselineInput{
					BaselineId: created.BaselineId,
				})
				require.NoError(t, err)
			}

			list, err := client.DescribePatchBaselines(ctx, &ssmsdk.DescribePatchBaselinesInput{
				Filters: []ssmtypes.PatchOrchestratorFilter{
					{Key: aws.String("NAME_PREFIX"), Values: []string{"custom"}},
				},
			})
			require.NoError(t, err)
			require.Len(t, list.BaselineIdentities, 1)
			assert.Equal(t, "my desc", aws.ToString(list.BaselineIdentities[0].BaselineDescription))
			assert.Equal(t, tt.registerDefault, list.BaselineIdentities[0].DefaultBaseline)
		})
	}
}

func TestChangeRequestAndAutomationExecutionMembers(t *testing.T) {
	t.Parallel()

	sched := time.Now().Add(time.Hour).Truncate(time.Second)

	tests := []struct {
		name string
	}{
		{name: "change_request_members_echoed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newDroppedMembersClient(t)

			_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
				Name: aws.String("runbook"), DocumentType: ssmtypes.DocumentTypeAutomation,
				DocumentFormat: ssmtypes.DocumentFormatJson,
				Content: aws.String(`{"schemaVersion":"0.3","mainSteps":[` +
					`{"name":"one","action":"aws:sleep","maxAttempts":3,"isCritical":false,` +
					`"onFailure":"Continue","nextStep":"two","inputs":{"Duration":"PT1S"}},` +
					`{"name":"two","action":"aws:sleep","isEnd":true,"inputs":{"Duration":"PT1S"}}]}`),
			})
			require.NoError(t, err)

			started, err := client.StartChangeRequestExecution(ctx, &ssmsdk.StartChangeRequestExecutionInput{
				DocumentName:      aws.String("AWS-ChangeTemplate"),
				ChangeRequestName: aws.String("cr-1"),
				ScheduledTime:     aws.Time(sched),
				Parameters:        map[string][]string{"p": {"v"}},
				Tags:              []ssmtypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
				Runbooks:          []ssmtypes.Runbook{{DocumentName: aws.String("runbook")}},
			})
			require.NoError(t, err)

			got, err := client.GetAutomationExecution(ctx, &ssmsdk.GetAutomationExecutionInput{
				AutomationExecutionId: started.AutomationExecutionId,
			})
			require.NoError(t, err)

			exec := got.AutomationExecution
			assert.Equal(t, "cr-1", aws.ToString(exec.ChangeRequestName))
			require.NotNil(t, exec.ScheduledTime)
			assert.WithinDuration(t, sched, *exec.ScheduledTime, time.Second)
			assert.Equal(t, []string{"v"}, exec.Parameters["p"])

			require.NotNil(t, exec.ProgressCounters)
			assert.EqualValues(t, 2, exec.ProgressCounters.TotalSteps)

			require.Len(t, exec.StepExecutions, 2)
			one, two := exec.StepExecutions[0], exec.StepExecutions[1]
			assert.EqualValues(t, 3, aws.ToInt32(one.MaxAttempts))
			assert.False(t, aws.ToBool(one.IsCritical))
			assert.Equal(t, "Continue", aws.ToString(one.OnFailure))
			assert.Equal(t, "two", aws.ToString(one.NextStep))
			assert.EqualValues(t, 1, aws.ToInt32(two.MaxAttempts))
			assert.True(t, aws.ToBool(two.IsCritical))
			assert.True(t, aws.ToBool(two.IsEnd))
			assert.Equal(t, "Abort", aws.ToString(two.OnFailure))

			list, err := client.DescribeAutomationExecutions(ctx, &ssmsdk.DescribeAutomationExecutionsInput{})
			require.NoError(t, err)
			require.Len(t, list.AutomationExecutionMetadataList, 1)
			assert.Equal(t, ssmtypes.AutomationTypeLocal, list.AutomationExecutionMetadataList[0].AutomationType)
			assert.Equal(t, "cr-1", aws.ToString(list.AutomationExecutionMetadataList[0].ChangeRequestName))

			tags, err := client.ListTagsForResource(ctx, &ssmsdk.ListTagsForResourceInput{
				ResourceType: ssmtypes.ResourceTypeForTaggingAutomation, ResourceId: started.AutomationExecutionId,
			})
			require.NoError(t, err)
			require.Len(t, tags.TagList, 1)
		})
	}
}

func TestDeleteInventory_LastStatusUpdateTime(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newDroppedMembersClient(t)

	out, err := client.DeleteInventory(ctx, &ssmsdk.DeleteInventoryInput{TypeName: aws.String("Custom:Thing")})
	require.NoError(t, err)

	got, err := client.DescribeInventoryDeletions(
		ctx,
		&ssmsdk.DescribeInventoryDeletionsInput{DeletionId: out.DeletionId},
	)
	require.NoError(t, err)
	require.Len(t, got.InventoryDeletions, 1)
	require.NotNil(t, got.InventoryDeletions[0].LastStatusUpdateTime)
	assert.WithinDuration(t, time.Now(), *got.InventoryDeletions[0].LastStatusUpdateTime, time.Minute)
}
