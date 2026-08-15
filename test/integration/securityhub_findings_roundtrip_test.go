package integration_test

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomAccountID builds a syntactically valid 12-digit AWS account ID that
// is unique per call, for tests that must not collide with parallel siblings
// or shared-account fixtures.
func randomAccountID() string {
	u := uuid.New()

	const twelveDigits = 1_000_000_000_000

	return fmt.Sprintf("%012d", binary.BigEndian.Uint64(u[:8])%twelveDigits)
}

// TestIntegration_SecurityHub_FindingsRoundTrip drives BatchImportFindings,
// GetFindings, BatchUpdateFindings and GetFindingHistory through a real
// client -- none of the four had ever been called by a typed client before
// this test. It also exercises GetFindings' SeverityLabel/WorkflowStatus
// filters, which read a finding's nested Severity.Label/Workflow.Status
// (securityhub@v1.75.4 types/types.go AwsSecurityFinding), not a flat
// top-level field.
func TestIntegration_SecurityHub_FindingsRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSecurityHubClient(t)
	ctx := t.Context()

	_, _ = client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{
		EnableDefaultStandards: aws.Bool(false),
	})

	findingID := "roundtrip-finding-" + uuid.NewString()
	productARN := "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
	resourceID := "arn:aws:s3:::roundtrip-bucket-" + uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)

	finding := securityhubtypes.AwsSecurityFinding{
		SchemaVersion: aws.String("2018-10-08"),
		Id:            aws.String(findingID),
		ProductArn:    aws.String(productARN),
		GeneratorId:   aws.String("test-generator"),
		AwsAccountId:  aws.String("123456789012"),
		CreatedAt:     aws.String(now),
		UpdatedAt:     aws.String(now),
		Title:         aws.String("Roundtrip Test Finding"),
		Description:   aws.String("Exercises BatchImportFindings via a real client"),
		Severity:      &securityhubtypes.Severity{Label: securityhubtypes.SeverityLabelHigh},
		RecordState:   securityhubtypes.RecordStateActive,
		Resources: []securityhubtypes.Resource{
			{Type: aws.String("AwsS3Bucket"), Id: aws.String(resourceID)},
		},
	}

	importOut, err := client.BatchImportFindings(ctx, &securityhubsdk.BatchImportFindingsInput{
		Findings: []securityhubtypes.AwsSecurityFinding{finding},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(importOut.SuccessCount))
	assert.Equal(t, int32(0), aws.ToInt32(importOut.FailedCount))
	assert.Empty(t, importOut.FailedFindings)

	getOut, err := client.GetFindings(ctx, &securityhubsdk.GetFindingsInput{
		Filters: &securityhubtypes.AwsSecurityFindingFilters{
			Id: []securityhubtypes.StringFilter{
				{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String(findingID)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, getOut.Findings, 1, "GetFindings should read back the finding BatchImportFindings just wrote")

	got := getOut.Findings[0]
	assert.Equal(t, "Roundtrip Test Finding", aws.ToString(got.Title))
	assert.Equal(t, "Exercises BatchImportFindings via a real client", aws.ToString(got.Description))
	assert.Equal(t, "test-generator", aws.ToString(got.GeneratorId))
	require.NotNil(t, got.Severity)
	assert.Equal(t, securityhubtypes.SeverityLabelHigh, got.Severity.Label)
	require.Len(t, got.Resources, 1)
	assert.Equal(t, resourceID, aws.ToString(got.Resources[0].Id))

	// SeverityLabel filters against the nested Severity.Label, not a flat field.
	sevOut, err := client.GetFindings(ctx, &securityhubsdk.GetFindingsInput{
		Filters: &securityhubtypes.AwsSecurityFindingFilters{
			Id: []securityhubtypes.StringFilter{
				{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String(findingID)},
			},
			SeverityLabel: []securityhubtypes.StringFilter{
				{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String("HIGH")},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, sevOut.Findings, 1, "SeverityLabel filter should match the finding's nested Severity.Label")

	_, err = client.BatchUpdateFindings(ctx, &securityhubsdk.BatchUpdateFindingsInput{
		FindingIdentifiers: []securityhubtypes.AwsSecurityFindingIdentifier{
			{Id: aws.String(findingID), ProductArn: aws.String(productARN)},
		},
		Note: &securityhubtypes.NoteUpdate{
			Text:      aws.String("investigated, no action needed"),
			UpdatedBy: aws.String("integration-test"),
		},
		Workflow: &securityhubtypes.WorkflowUpdate{Status: securityhubtypes.WorkflowStatusResolved},
	})
	require.NoError(t, err)

	afterUpdate, err := client.GetFindings(ctx, &securityhubsdk.GetFindingsInput{
		Filters: &securityhubtypes.AwsSecurityFindingFilters{
			Id: []securityhubtypes.StringFilter{
				{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String(findingID)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, afterUpdate.Findings, 1)
	require.NotNil(t, afterUpdate.Findings[0].Note)
	assert.Equal(t, "investigated, no action needed", aws.ToString(afterUpdate.Findings[0].Note.Text))
	require.NotNil(t, afterUpdate.Findings[0].Workflow)
	assert.Equal(t, securityhubtypes.WorkflowStatusResolved, afterUpdate.Findings[0].Workflow.Status)

	// WorkflowStatus filters against the nested Workflow.Status, not a flat field.
	workflowOut, err := client.GetFindings(ctx, &securityhubsdk.GetFindingsInput{
		Filters: &securityhubtypes.AwsSecurityFindingFilters{
			Id: []securityhubtypes.StringFilter{
				{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String(findingID)},
			},
			WorkflowStatus: []securityhubtypes.StringFilter{
				{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String("RESOLVED")},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, workflowOut.Findings, 1, "WorkflowStatus filter should match the finding's nested Workflow.Status")

	histOut, err := client.GetFindingHistory(ctx, &securityhubsdk.GetFindingHistoryInput{
		FindingIdentifier: &securityhubtypes.AwsSecurityFindingIdentifier{
			Id: aws.String(findingID), ProductArn: aws.String(productARN),
		},
	})
	require.NoError(t, err)
	require.Len(t, histOut.Records, 2, "one record for the import, one for the update")
	assert.True(t, aws.ToBool(histOut.Records[0].FindingCreated))
	assert.Equal(t,
		securityhubtypes.FindingHistoryUpdateSourceTypeBatchImportFindings,
		histOut.Records[0].UpdateSource.Type,
	)
	assert.False(t, aws.ToBool(histOut.Records[1].FindingCreated))
	assert.Equal(t,
		securityhubtypes.FindingHistoryUpdateSourceTypeBatchUpdateFindings,
		histOut.Records[1].UpdateSource.Type,
	)
}

// TestIntegration_SecurityHub_ActionTargetRoundTrip drives
// CreateActionTarget, DescribeActionTargets, UpdateActionTarget and
// DeleteActionTarget through a real client -- none had ever been called by a
// typed client before this test.
func TestIntegration_SecurityHub_ActionTargetRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSecurityHubClient(t)
	ctx := t.Context()

	_, _ = client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{
		EnableDefaultStandards: aws.Bool(false),
	})

	id := "roundtrip-action-" + uuid.NewString()[:8]

	createOut, err := client.CreateActionTarget(ctx, &securityhubsdk.CreateActionTargetInput{
		Id:          aws.String(id),
		Name:        aws.String("Roundtrip Action"),
		Description: aws.String("created by integration test"),
	})
	require.NoError(t, err)
	arn := aws.ToString(createOut.ActionTargetArn)
	require.NotEmpty(t, arn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteActionTarget(cleanupCtx, &securityhubsdk.DeleteActionTargetInput{
			ActionTargetArn: aws.String(arn),
		})
	})

	descOut, err := client.DescribeActionTargets(ctx, &securityhubsdk.DescribeActionTargetsInput{
		ActionTargetArns: []string{arn},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ActionTargets, 1)
	assert.Equal(t, "Roundtrip Action", aws.ToString(descOut.ActionTargets[0].Name))
	assert.Equal(t, "created by integration test", aws.ToString(descOut.ActionTargets[0].Description))

	_, err = client.UpdateActionTarget(ctx, &securityhubsdk.UpdateActionTargetInput{
		ActionTargetArn: aws.String(arn),
		Description:     aws.String("updated by integration test"),
	})
	require.NoError(t, err)

	descAfterUpdate, err := client.DescribeActionTargets(ctx, &securityhubsdk.DescribeActionTargetsInput{
		ActionTargetArns: []string{arn},
	})
	require.NoError(t, err)
	require.Len(t, descAfterUpdate.ActionTargets, 1)
	assert.Equal(t, "updated by integration test", aws.ToString(descAfterUpdate.ActionTargets[0].Description))
	assert.Equal(t,
		"Roundtrip Action", aws.ToString(descAfterUpdate.ActionTargets[0].Name),
		"UpdateActionTarget without Name should leave it unchanged",
	)

	deleteOut, err := client.DeleteActionTarget(ctx, &securityhubsdk.DeleteActionTargetInput{
		ActionTargetArn: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Equal(t, arn, aws.ToString(deleteOut.ActionTargetArn))

	descAfterDelete, err := client.DescribeActionTargets(ctx, &securityhubsdk.DescribeActionTargetsInput{})
	require.NoError(t, err)

	for _, at := range descAfterDelete.ActionTargets {
		assert.NotEqual(t,
			arn, aws.ToString(at.ActionTargetArn),
			"DeleteActionTarget should remove the target from DescribeActionTargets",
		)
	}
}

// TestIntegration_SecurityHub_MembersRoundTrip drives CreateMembers,
// GetMembers, ListMembers and DeleteMembers through a real client and
// asserts the UnprocessedAccounts shape a real caller decodes:
// types.Result{AccountId, ProcessingResult} (securityhub@v1.75.4
// deserializers.go's awsRestjson1_deserializeDocumentResult), not the
// ErrorCode/ErrorMessage shape this backend previously emitted.
func TestIntegration_SecurityHub_MembersRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSecurityHubClient(t)
	ctx := t.Context()

	_, _ = client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{
		EnableDefaultStandards: aws.Bool(false),
	})

	accountID := randomAccountID()

	createOut, err := client.CreateMembers(ctx, &securityhubsdk.CreateMembersInput{
		AccountDetails: []securityhubtypes.AccountDetails{
			{AccountId: aws.String(accountID), Email: aws.String("member@example.com")},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, createOut.UnprocessedAccounts)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteMembers(cleanupCtx, &securityhubsdk.DeleteMembersInput{AccountIds: []string{accountID}})
	})

	getOut, err := client.GetMembers(ctx, &securityhubsdk.GetMembersInput{AccountIds: []string{accountID}})
	require.NoError(t, err)
	require.Len(t, getOut.Members, 1, "GetMembers should read back the member CreateMembers just wrote")
	assert.Equal(t, accountID, aws.ToString(getOut.Members[0].AccountId))
	assert.Equal(t, "member@example.com", aws.ToString(getOut.Members[0].Email))
	assert.Equal(t, "Created", aws.ToString(getOut.Members[0].MemberStatus))

	listOut, err := client.ListMembers(ctx, &securityhubsdk.ListMembersInput{OnlyAssociated: aws.Bool(false)})
	require.NoError(t, err)

	var found bool

	for _, m := range listOut.Members {
		if aws.ToString(m.AccountId) == accountID {
			found = true
		}
	}

	assert.True(t, found, "ListMembers(OnlyAssociated=false) should include the member CreateMembers just wrote")

	deleteOut, err := client.DeleteMembers(ctx, &securityhubsdk.DeleteMembersInput{AccountIds: []string{accountID}})
	require.NoError(t, err)
	assert.Empty(t, deleteOut.UnprocessedAccounts)

	getAfterDelete, err := client.GetMembers(ctx, &securityhubsdk.GetMembersInput{AccountIds: []string{accountID}})
	require.NoError(t, err)
	assert.Empty(t, getAfterDelete.Members)
	require.Len(t,
		getAfterDelete.UnprocessedAccounts, 1,
		"GetMembers on a deleted account should report it as unprocessed",
	)
	assert.Equal(t, accountID, aws.ToString(getAfterDelete.UnprocessedAccounts[0].AccountId))
	assert.NotEmpty(
		t,
		aws.ToString(getAfterDelete.UnprocessedAccounts[0].ProcessingResult),
		"a real client can only see the not-found reason via ProcessingResult, not a nonstandard ErrorCode/ErrorMessage pair",
	)
}
