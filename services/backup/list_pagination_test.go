package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestListFrameworks_Pagination covers gopherstack-tscj: ListFrameworks
// previously accepted MaxResults/NextToken (real query params, serializers.go
// awsRestjson1_serializeOpHttpBindingsListFrameworksInput:5895-5901,
// capitalized "MaxResults"/"NextToken") on the wire and applied neither.
func TestListFrameworks_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	names := make([]string, 0, 3)

	for _, n := range []string{"fw-a", "fw-b", "fw-c"} {
		_, err := client.CreateFramework(ctx, &backupsdk.CreateFrameworkInput{
			FrameworkName:        aws.String(n),
			FrameworkControls:    []types.FrameworkControl{},
			FrameworkTags:        map[string]string{},
			IdempotencyToken:     aws.String(n),
			FrameworkDescription: aws.String(""),
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListFrameworks(ctx, &backupsdk.ListFrameworksInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Frameworks, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListFrameworks(ctx, &backupsdk.ListFrameworksInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Frameworks, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListFrameworks(ctx, &backupsdk.ListFrameworksInput{
		MaxResults: aws.Int32(1),
		NextToken:  page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Frameworks, 1)
	assert.Nil(t, page3.NextToken)

	var got []string
	for _, page := range [][]types.Framework{page1.Frameworks, page2.Frameworks, page3.Frameworks} {
		for _, f := range page {
			got = append(got, aws.ToString(f.FrameworkName))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListReportPlans_Pagination covers gopherstack-tscj: ListReportPlans
// previously accepted MaxResults/NextToken (real query params, serializers.go
// awsRestjson1_serializeOpHttpBindingsListReportPlansInput:6633-6639,
// capitalized "MaxResults"/"NextToken") on the wire and applied neither.
func TestListReportPlans_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	names := make([]string, 0, 3)

	for _, n := range []string{"rp-a", "rp-b", "rp-c"} {
		_, err := client.CreateReportPlan(ctx, &backupsdk.CreateReportPlanInput{
			ReportPlanName:        aws.String(n),
			ReportDeliveryChannel: &types.ReportDeliveryChannel{S3BucketName: aws.String("rp-bucket")},
			ReportSetting:         &types.ReportSetting{ReportTemplate: aws.String("BACKUP_JOB_REPORT")},
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListReportPlans(ctx, &backupsdk.ListReportPlansInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.ReportPlans, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListReportPlans(ctx, &backupsdk.ListReportPlansInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ReportPlans, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListReportPlans(ctx, &backupsdk.ListReportPlansInput{
		MaxResults: aws.Int32(1),
		NextToken:  page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.ReportPlans, 1)
	assert.Nil(t, page3.NextToken)

	var got []string
	for _, page := range [][]types.ReportPlan{page1.ReportPlans, page2.ReportPlans, page3.ReportPlans} {
		for _, rp := range page {
			got = append(got, aws.ToString(rp.ReportPlanName))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListRestoreTestingPlans_Pagination covers gopherstack-tscj:
// ListRestoreTestingPlans previously accepted MaxResults/NextToken (real
// query params, serializers.go
// awsRestjson1_serializeOpHttpBindingsListRestoreTestingPlansInput:7065-7071,
// capitalized "MaxResults"/"NextToken") on the wire and applied neither.
func TestListRestoreTestingPlans_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	names := make([]string, 0, 3)

	for _, n := range []string{"rtp-a", "rtp-b", "rtp-c"} {
		_, err := client.CreateRestoreTestingPlan(ctx, &backupsdk.CreateRestoreTestingPlanInput{
			RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
				RestoreTestingPlanName: aws.String(n),
				ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
				RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
					Algorithm: types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
					RecoveryPointTypes: []types.RestoreTestingRecoveryPointType{
						types.RestoreTestingRecoveryPointTypeSnapshot,
					},
					IncludeVaults: []string{"*"},
				},
			},
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListRestoreTestingPlans(ctx, &backupsdk.ListRestoreTestingPlansInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.RestoreTestingPlans, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListRestoreTestingPlans(ctx, &backupsdk.ListRestoreTestingPlansInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.RestoreTestingPlans, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListRestoreTestingPlans(ctx, &backupsdk.ListRestoreTestingPlansInput{
		MaxResults: aws.Int32(1),
		NextToken:  page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.RestoreTestingPlans, 1)
	assert.Nil(t, page3.NextToken)

	var got []string
	for _, page := range [][]types.RestoreTestingPlanForList{
		page1.RestoreTestingPlans, page2.RestoreTestingPlans, page3.RestoreTestingPlans,
	} {
		for _, rtp := range page {
			got = append(got, aws.ToString(rtp.RestoreTestingPlanName))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListRestoreTestingSelections_Pagination covers gopherstack-tscj:
// ListRestoreTestingSelections previously accepted MaxResults/NextToken
// (real query params, serializers.go
// awsRestjson1_serializeOpHttpBindingsListRestoreTestingSelectionsInput:7135-7141,
// capitalized "MaxResults"/"NextToken") on the wire and applied neither.
func TestListRestoreTestingSelections_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	_, err := client.CreateRestoreTestingPlan(ctx, &backupsdk.CreateRestoreTestingPlanInput{
		RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
			RestoreTestingPlanName: aws.String("rts-plan"),
			ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
			RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
				Algorithm: types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
				RecoveryPointTypes: []types.RestoreTestingRecoveryPointType{
					types.RestoreTestingRecoveryPointTypeSnapshot,
				},
				IncludeVaults: []string{"*"},
			},
		},
	})
	require.NoError(t, err)

	names := make([]string, 0, 3)

	for _, n := range []string{"sel-a", "sel-b", "sel-c"} {
		_, err = client.CreateRestoreTestingSelection(ctx, &backupsdk.CreateRestoreTestingSelectionInput{
			RestoreTestingPlanName: aws.String("rts-plan"),
			RestoreTestingSelection: &types.RestoreTestingSelectionForCreate{
				RestoreTestingSelectionName: aws.String(n),
				ProtectedResourceType:       aws.String("EC2"),
				IamRoleArn:                  aws.String("arn:aws:iam::000000000000:role/RestoreRole"),
				ProtectedResourceArns:       []string{"*"},
			},
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListRestoreTestingSelections(ctx, &backupsdk.ListRestoreTestingSelectionsInput{
		RestoreTestingPlanName: aws.String("rts-plan"),
		MaxResults:             aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.RestoreTestingSelections, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListRestoreTestingSelections(ctx, &backupsdk.ListRestoreTestingSelectionsInput{
		RestoreTestingPlanName: aws.String("rts-plan"),
		MaxResults:             aws.Int32(1),
		NextToken:              page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.RestoreTestingSelections, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListRestoreTestingSelections(ctx, &backupsdk.ListRestoreTestingSelectionsInput{
		RestoreTestingPlanName: aws.String("rts-plan"),
		MaxResults:             aws.Int32(1),
		NextToken:              page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.RestoreTestingSelections, 1)
	assert.Nil(t, page3.NextToken)

	var got []string
	for _, page := range [][]types.RestoreTestingSelectionForList{
		page1.RestoreTestingSelections, page2.RestoreTestingSelections, page3.RestoreTestingSelections,
	} {
		for _, sel := range page {
			got = append(got, aws.ToString(sel.RestoreTestingSelectionName))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListBackupSelections_Pagination covers gopherstack-tscj:
// ListBackupSelections previously accepted MaxResults/NextToken (real query
// params, serializers.go
// awsRestjson1_serializeOpHttpBindingsListBackupSelectionsInput:5539-5545,
// lowercase "maxResults"/"nextToken") on the wire and applied neither.
func TestListBackupSelections_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	vault := mustVault(t, backend, "bs-vault")
	plan := mustPlan(t, backend, "bs-plan", vault.BackupVaultName)

	names := make([]string, 0, 3)

	for _, n := range []string{"bs-a", "bs-b", "bs-c"} {
		_, err := client.CreateBackupSelection(ctx, &backupsdk.CreateBackupSelectionInput{
			BackupPlanId: aws.String(plan.BackupPlanID),
			BackupSelection: &types.BackupSelection{
				SelectionName: aws.String(n),
				IamRoleArn:    aws.String("arn:aws:iam::000000000000:role/BackupRole"),
			},
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListBackupSelections(ctx, &backupsdk.ListBackupSelectionsInput{
		BackupPlanId: aws.String(plan.BackupPlanID),
		MaxResults:   aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.BackupSelectionsList, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListBackupSelections(ctx, &backupsdk.ListBackupSelectionsInput{
		BackupPlanId: aws.String(plan.BackupPlanID),
		MaxResults:   aws.Int32(1),
		NextToken:    page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.BackupSelectionsList, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListBackupSelections(ctx, &backupsdk.ListBackupSelectionsInput{
		BackupPlanId: aws.String(plan.BackupPlanID),
		MaxResults:   aws.Int32(1),
		NextToken:    page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.BackupSelectionsList, 1)
	assert.Nil(t, page3.NextToken)

	var got []string
	for _, page := range [][]types.BackupSelectionsListMember{
		page1.BackupSelectionsList, page2.BackupSelectionsList, page3.BackupSelectionsList,
	} {
		for _, sel := range page {
			got = append(got, aws.ToString(sel.SelectionName))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListLegalHolds_Pagination covers gopherstack-tscj: ListLegalHolds
// previously accepted MaxResults/NextToken (real query params, serializers.go
// awsRestjson1_serializeOpHttpBindingsListLegalHoldsInput:6055-6061,
// lowercase "maxResults"/"nextToken") on the wire and applied neither.
func TestListLegalHolds_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	ids := make([]string, 0, 3)

	for _, title := range []string{"lh-a", "lh-b", "lh-c"} {
		out, err := client.CreateLegalHold(ctx, &backupsdk.CreateLegalHoldInput{
			Title:       aws.String(title),
			Description: aws.String("desc"),
		})
		require.NoError(t, err)
		ids = append(ids, aws.ToString(out.LegalHoldId))
	}

	page1, err := client.ListLegalHolds(ctx, &backupsdk.ListLegalHoldsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.LegalHolds, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListLegalHolds(ctx, &backupsdk.ListLegalHoldsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.LegalHolds, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListLegalHolds(ctx, &backupsdk.ListLegalHoldsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.LegalHolds, 1)
	assert.Nil(t, page3.NextToken)

	var got []string
	for _, page := range [][]types.LegalHold{page1.LegalHolds, page2.LegalHolds, page3.LegalHolds} {
		for _, lh := range page {
			got = append(got, aws.ToString(lh.LegalHoldId))
		}
	}

	assert.ElementsMatch(t, ids, got)
}
