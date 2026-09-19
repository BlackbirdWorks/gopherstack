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

// TestListSummaries_MemberRoundTrips proves this pass's over-wide-response
// fixes (gopherstack list-summary-shapes sweep, 2026-09-18): ListOpsMetadata
// leaked the Get-only Metadata map onto its list item; ListComplianceSummaries/
// ListResourceComplianceSummaries were missing the real SeveritySummary
// breakdown; ListOpsItemRelatedItems was missing the real OpsItemId member.
// Each subtest drives the real aws-sdk-go-v2 client end to end.
func TestListSummaries_MemberRoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("ops metadata list omits metadata map", func(t *testing.T) {
		t.Parallel()

		backend := ssm.NewInMemoryBackend()
		handler := ssm.NewHandler(backend)
		client := newTestSSMClient(t, handler)

		created, err := client.CreateOpsMetadata(t.Context(), &ssmsdk.CreateOpsMetadataInput{
			ResourceId: aws.String("arn:aws:ssm:us-east-1:000000000000:document/list-summary-doc"),
			Metadata: map[string]ssmtypes.MetadataValue{
				"key1": {Value: aws.String("value1")},
			},
		})
		require.NoError(t, err)

		listed, err := client.ListOpsMetadata(t.Context(), &ssmsdk.ListOpsMetadataInput{})
		require.NoError(t, err)
		require.Len(t, listed.OpsMetadataList, 1)
		assert.Equal(t, aws.ToString(created.OpsMetadataArn), aws.ToString(listed.OpsMetadataList[0].OpsMetadataArn))

		rec := doRequest(t, handler, "ListOpsMetadata", `{}`)
		assert.NotContains(
			t, rec.Body.String(), `"Metadata":`,
			"ListOpsMetadata must not leak the Get-only Metadata map",
		)
		assert.Contains(t, rec.Body.String(), "OpsMetadataArn")
	})

	t.Run("compliance summaries carry severity breakdown", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)

		_, err := client.PutComplianceItems(t.Context(), &ssmsdk.PutComplianceItemsInput{
			ResourceId:     aws.String("i-severitysummary"),
			ResourceType:   aws.String("ManagedInstance"),
			ComplianceType: aws.String("Custom:Severity"),
			ExecutionSummary: &ssmtypes.ComplianceExecutionSummary{
				ExecutionTime: aws.Time(time.Now()),
			},
			Items: []ssmtypes.ComplianceItemEntry{
				{
					Id:       aws.String("1"),
					Severity: ssmtypes.ComplianceSeverityCritical,
					Status:   ssmtypes.ComplianceStatusNonCompliant,
				},
				{
					Id:       aws.String("2"),
					Severity: ssmtypes.ComplianceSeverityLow,
					Status:   ssmtypes.ComplianceStatusCompliant,
				},
			},
		})
		require.NoError(t, err)

		summaries, err := client.ListComplianceSummaries(t.Context(), &ssmsdk.ListComplianceSummariesInput{})
		require.NoError(t, err)
		require.Len(t, summaries.ComplianceSummaryItems, 1)

		item := summaries.ComplianceSummaryItems[0]
		require.NotNil(t, item.NonCompliantSummary.SeveritySummary)
		assert.EqualValues(t, 1, item.NonCompliantSummary.SeveritySummary.CriticalCount)
		require.NotNil(t, item.CompliantSummary.SeveritySummary)
		assert.EqualValues(t, 1, item.CompliantSummary.SeveritySummary.LowCount)

		resourceSummaries, err := client.ListResourceComplianceSummaries(
			t.Context(),
			&ssmsdk.ListResourceComplianceSummariesInput{},
		)
		require.NoError(t, err)
		require.Len(t, resourceSummaries.ResourceComplianceSummaryItems, 1)

		resItem := resourceSummaries.ResourceComplianceSummaryItems[0]
		assert.Equal(t, ssmtypes.ComplianceSeverityCritical, resItem.OverallSeverity)
		require.NotNil(t, resItem.NonCompliantSummary.SeveritySummary)
		assert.EqualValues(t, 1, resItem.NonCompliantSummary.SeveritySummary.CriticalCount)
	})

	t.Run("ops item related items carry ops item id", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)

		item, err := client.CreateOpsItem(t.Context(), &ssmsdk.CreateOpsItemInput{
			Title:       aws.String("related-item-test"),
			Source:      aws.String("EC2"),
			Description: aws.String("desc"),
		})
		require.NoError(t, err)

		_, err = client.AssociateOpsItemRelatedItem(t.Context(), &ssmsdk.AssociateOpsItemRelatedItemInput{
			OpsItemId:       item.OpsItemId,
			AssociationType: aws.String("RelatesTo"),
			ResourceType:    aws.String("AWS::SSMIncidents::IncidentRecord"),
			ResourceUri:     aws.String("arn:aws:ssm-incidents::000000000000:incident-record/example"),
		})
		require.NoError(t, err)

		related, err := client.ListOpsItemRelatedItems(t.Context(), &ssmsdk.ListOpsItemRelatedItemsInput{
			OpsItemId: item.OpsItemId,
		})
		require.NoError(t, err)
		require.Len(t, related.Summaries, 1)
		assert.Equal(t, aws.ToString(item.OpsItemId), aws.ToString(related.Summaries[0].OpsItemId))
		assert.NotZero(t, aws.ToTime(related.Summaries[0].CreatedTime))
	})
}
