package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_QuickSight_FolderMembershipLifecycle drives the full Folder lifecycle through
// the real SDK: create->describe->list->create-membership->list-members->delete, asserting real
// (non-empty) fields round-trip.
func TestIntegration_QuickSight_FolderMembershipLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createQuickSightClient(t)
	ctx := t.Context()

	folderID := "integ-folder-" + uuid.NewString()[:8]
	folderName := "Integration Test Folder"

	createOut, err := client.CreateFolder(ctx, &quicksight.CreateFolderInput{
		AwsAccountId: aws.String(quicksightAccountID),
		FolderId:     aws.String(folderID),
		Name:         aws.String(folderName),
		FolderType:   qstypes.FolderTypeShared,
	})
	require.NoError(t, err, "CreateFolder should succeed")
	require.NotEmpty(t, aws.ToString(createOut.Arn), "Arn should be a real, non-empty value")
	assert.Equal(t, folderID, aws.ToString(createOut.FolderId))

	t.Cleanup(func() {
		_, _ = client.DeleteFolder(ctx, &quicksight.DeleteFolderInput{
			AwsAccountId: aws.String(quicksightAccountID),
			FolderId:     aws.String(folderID),
		})
	})

	descOut, err := client.DescribeFolder(ctx, &quicksight.DescribeFolderInput{
		AwsAccountId: aws.String(quicksightAccountID),
		FolderId:     aws.String(folderID),
	})
	require.NoError(t, err, "DescribeFolder should succeed")
	require.NotNil(t, descOut.Folder)
	assert.Equal(t, folderName, aws.ToString(descOut.Folder.Name))
	assert.Equal(t, folderID, aws.ToString(descOut.Folder.FolderId))
	require.NotNil(t, descOut.Folder.CreatedTime, "CreatedTime should be a real timestamp")

	listOut, err := client.ListFolders(ctx, &quicksight.ListFoldersInput{
		AwsAccountId: aws.String(quicksightAccountID),
	})
	require.NoError(t, err, "ListFolders should succeed")

	found := false
	for _, f := range listOut.FolderSummaryList {
		if aws.ToString(f.FolderId) == folderID {
			found = true
			assert.Equal(t, folderName, aws.ToString(f.Name))
		}
	}
	assert.True(t, found, "created folder should appear in ListFolders")

	memberID := "integ-dataset-" + uuid.NewString()[:8]

	membershipOut, err := client.CreateFolderMembership(ctx, &quicksight.CreateFolderMembershipInput{
		AwsAccountId: aws.String(quicksightAccountID),
		FolderId:     aws.String(folderID),
		MemberId:     aws.String(memberID),
		MemberType:   qstypes.MemberTypeDataset,
	})
	require.NoError(t, err, "CreateFolderMembership should succeed")
	require.NotNil(t, membershipOut.FolderMember)
	assert.Equal(t, memberID, aws.ToString(membershipOut.FolderMember.MemberId))

	membersOut, err := client.ListFolderMembers(ctx, &quicksight.ListFolderMembersInput{
		AwsAccountId: aws.String(quicksightAccountID),
		FolderId:     aws.String(folderID),
	})
	require.NoError(t, err, "ListFolderMembers should succeed")
	require.NotEmpty(t, membersOut.FolderMemberList, "expected the created member to appear")

	memberFound := false
	for _, m := range membersOut.FolderMemberList {
		if aws.ToString(m.MemberId) == memberID {
			memberFound = true
		}
	}
	assert.True(t, memberFound, "created folder member should appear in ListFolderMembers")

	_, err = client.DeleteFolder(ctx, &quicksight.DeleteFolderInput{
		AwsAccountId: aws.String(quicksightAccountID),
		FolderId:     aws.String(folderID),
	})
	require.NoError(t, err, "DeleteFolder should succeed")

	_, err = client.DescribeFolder(ctx, &quicksight.DescribeFolderInput{
		AwsAccountId: aws.String(quicksightAccountID),
		FolderId:     aws.String(folderID),
	})
	require.Error(t, err, "folder should be gone after delete")
}

// TestIntegration_QuickSight_TemplateVersionLifecycle drives Template create->describe->update
// (which creates a new version)->list-versions through the real SDK, asserting the version
// number and source ARN round-trip.
func TestIntegration_QuickSight_TemplateVersionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createQuickSightClient(t)
	ctx := t.Context()

	templateID := "integ-template-" + uuid.NewString()[:8]
	templateName := "Integration Test Template"

	definition := &qstypes.TemplateVersionDefinition{
		DataSetConfigurations: []qstypes.DataSetConfiguration{
			{Placeholder: aws.String("primary")},
		},
	}

	createOut, err := client.CreateTemplate(ctx, &quicksight.CreateTemplateInput{
		AwsAccountId: aws.String(quicksightAccountID),
		TemplateId:   aws.String(templateID),
		Name:         aws.String(templateName),
		Definition:   definition,
	})
	require.NoError(t, err, "CreateTemplate should succeed")
	require.NotEmpty(t, aws.ToString(createOut.Arn))
	assert.Equal(t, templateID, aws.ToString(createOut.TemplateId))
	assert.NotEmpty(t, string(createOut.CreationStatus), "CreationStatus should be a real value")

	t.Cleanup(func() {
		_, _ = client.DeleteTemplate(ctx, &quicksight.DeleteTemplateInput{
			AwsAccountId: aws.String(quicksightAccountID),
			TemplateId:   aws.String(templateID),
		})
	})

	descOut, err := client.DescribeTemplate(ctx, &quicksight.DescribeTemplateInput{
		AwsAccountId: aws.String(quicksightAccountID),
		TemplateId:   aws.String(templateID),
	})
	require.NoError(t, err, "DescribeTemplate should succeed")
	require.NotNil(t, descOut.Template)
	require.NotNil(t, descOut.Template.Version)
	assert.Equal(t, templateName, aws.ToString(descOut.Template.Name))
	assert.Equal(t, int64(1), aws.ToInt64(descOut.Template.Version.VersionNumber))

	// UpdateTemplate creates a new (second) version.
	updatedName := "Integration Test Template Updated"
	updateOut, err := client.UpdateTemplate(ctx, &quicksight.UpdateTemplateInput{
		AwsAccountId: aws.String(quicksightAccountID),
		TemplateId:   aws.String(templateID),
		Name:         aws.String(updatedName),
		Definition:   definition,
	})
	require.NoError(t, err, "UpdateTemplate should succeed")
	assert.Equal(t, templateID, aws.ToString(updateOut.TemplateId))
	assert.NotEmpty(t, aws.ToString(updateOut.VersionArn), "VersionArn should be a real value for the new version")

	versionsOut, err := client.ListTemplateVersions(ctx, &quicksight.ListTemplateVersionsInput{
		AwsAccountId: aws.String(quicksightAccountID),
		TemplateId:   aws.String(templateID),
	})
	require.NoError(t, err, "ListTemplateVersions should succeed")
	require.Len(t, versionsOut.TemplateVersionSummaryList, 2, "expected two real template versions (create + update)")

	seenVersions := map[int64]bool{}
	for _, v := range versionsOut.TemplateVersionSummaryList {
		seenVersions[aws.ToInt64(v.VersionNumber)] = true
	}
	assert.True(t, seenVersions[1], "version 1 (from create) should be listed")
	assert.True(t, seenVersions[2], "version 2 (from update) should be listed")

	// DescribeTemplate again should now reflect the latest (updated) version by default.
	descOut2, err := client.DescribeTemplate(ctx, &quicksight.DescribeTemplateInput{
		AwsAccountId: aws.String(quicksightAccountID),
		TemplateId:   aws.String(templateID),
	})
	require.NoError(t, err, "DescribeTemplate (post-update) should succeed")
	assert.Equal(t, updatedName, aws.ToString(descOut2.Template.Name))
	assert.Equal(t, int64(2), aws.ToInt64(descOut2.Template.Version.VersionNumber))
}

// TestIntegration_QuickSight_AccountSettingsRoundTrip drives UpdateAccountSettings followed by
// DescribeAccountSettings through the real SDK, asserting the submitted values round-trip.
func TestIntegration_QuickSight_AccountSettingsRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createQuickSightClient(t)
	ctx := t.Context()

	notificationEmail := "integration-" + uuid.NewString()[:8] + "@example.com"

	_, err := client.UpdateAccountSettings(ctx, &quicksight.UpdateAccountSettingsInput{
		AwsAccountId:                 aws.String(quicksightAccountID),
		DefaultNamespace:             aws.String("default"),
		NotificationEmail:            aws.String(notificationEmail),
		TerminationProtectionEnabled: false,
	})
	require.NoError(t, err, "UpdateAccountSettings should succeed")

	out, err := client.DescribeAccountSettings(ctx, &quicksight.DescribeAccountSettingsInput{
		AwsAccountId: aws.String(quicksightAccountID),
	})
	require.NoError(t, err, "DescribeAccountSettings should succeed")
	require.NotNil(t, out.AccountSettings)
	assert.Equal(t, notificationEmail, aws.ToString(out.AccountSettings.NotificationEmail))
	assert.Equal(t, "default", aws.ToString(out.AccountSettings.DefaultNamespace))
	assert.False(t, out.AccountSettings.TerminationProtectionEnabled)
}
