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

func TestInventoryFilters_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	ctx := t.Context()

	for id, ver := range map[string]string{"i-aaa": "2023.1", "i-bbb": "2024.2"} {
		_, err := client.PutInventory(ctx, &ssmsdk.PutInventoryInput{
			InstanceId: aws.String(id),
			Items: []ssmtypes.InventoryItem{{
				TypeName:      aws.String("AWS:InstanceInformation"),
				SchemaVersion: aws.String("1.0"),
				CaptureTime:   aws.String("2026-01-01T00:00:00Z"),
				Content: []map[string]string{
					{"PlatformVersion": ver, "PlatformType": "Linux"},
					{"PlatformVersion": ver + "-x", "PlatformType": "Windows"},
				},
			}},
		})
		require.NoError(t, err)
	}

	f := func(typ ssmtypes.InventoryQueryOperatorType, vals ...string) []ssmtypes.InventoryFilter {
		return []ssmtypes.InventoryFilter{{
			Key:    aws.String("AWS:InstanceInformation.PlatformVersion"),
			Type:   typ,
			Values: vals,
		}}
	}

	tests := []struct {
		name         string
		filters      []ssmtypes.InventoryFilter
		wantEntities []string
		wantEntries  int
	}{
		{"none", nil, []string{"i-aaa", "i-bbb"}, 4},
		{"equal", f(ssmtypes.InventoryQueryOperatorTypeEqual, "2023.1"), []string{"i-aaa"}, 1},
		{"begin with", f(ssmtypes.InventoryQueryOperatorTypeBeginWith, "2024"), []string{"i-bbb"}, 2},
		{"not equal", f(ssmtypes.InventoryQueryOperatorTypeNotEqual, "2023.1"), []string{"i-aaa", "i-bbb"}, 3},
		{"greater than", f(ssmtypes.InventoryQueryOperatorTypeGreaterThan, "2024"), []string{"i-bbb"}, 2},
		{"no match", f(ssmtypes.InventoryQueryOperatorTypeEqual, "nope"), nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := client.GetInventory(ctx, &ssmsdk.GetInventoryInput{Filters: tt.filters})
			require.NoError(t, err)

			var ids []string
			for _, e := range got.Entities {
				ids = append(ids, aws.ToString(e.Id))
			}

			assert.Equal(t, tt.wantEntities, ids)

			entries := 0

			for _, id := range []string{"i-aaa", "i-bbb"} {
				out, listErr := client.ListInventoryEntries(ctx, &ssmsdk.ListInventoryEntriesInput{
					InstanceId: aws.String(id),
					TypeName:   aws.String("AWS:InstanceInformation"),
					Filters:    tt.filters,
				})
				require.NoError(t, listErr)

				entries += len(out.Entries)
			}

			assert.Equal(t, tt.wantEntries, entries)
		})
	}

	_, err := client.GetInventory(ctx, &ssmsdk.GetInventoryInput{
		Filters: []ssmtypes.InventoryFilter{{
			Key: aws.String("AWS:InstanceInformation.PlatformType"), Type: "Bogus", Values: []string{"x"},
		}},
	})
	require.Error(t, err)
}

func TestComplianceFilters_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	ctx := t.Context()

	puts := []struct{ res, ctype, status string }{
		{"i-1", "Association", "COMPLIANT"},
		{"i-1", "Patch", "NON_COMPLIANT"},
		{"i-2", "Patch", "COMPLIANT"},
	}

	for _, p := range puts {
		_, err := client.PutComplianceItems(ctx, &ssmsdk.PutComplianceItemsInput{
			ResourceId:     aws.String(p.res),
			ResourceType:   aws.String("ManagedInstance"),
			ComplianceType: aws.String(p.ctype),
			ExecutionSummary: &ssmtypes.ComplianceExecutionSummary{
				ExecutionTime: aws.Time(time.Unix(1_700_000_000, 0)),
			},
			Items: []ssmtypes.ComplianceItemEntry{{
				Id:       aws.String("id-" + p.res + p.ctype),
				Severity: ssmtypes.ComplianceSeverityHigh,
				Status:   ssmtypes.ComplianceStatus(p.status),
			}},
		})
		require.NoError(t, err)
	}

	patch := ssmtypes.ComplianceStringFilter{
		Key: aws.String("ComplianceType"), Type: ssmtypes.ComplianceQueryOperatorTypeEqual, Values: []string{"Patch"},
	}
	nonCompliant := ssmtypes.ComplianceStringFilter{
		Key: aws.String("Status"), Type: ssmtypes.ComplianceQueryOperatorTypeEqual, Values: []string{"NON_COMPLIANT"},
	}
	notPatch := ssmtypes.ComplianceStringFilter{
		Key: aws.String(
			"ComplianceType",
		), Type: ssmtypes.ComplianceQueryOperatorTypeNotEqual, Values: []string{"Patch"},
	}

	tests := []struct {
		name         string
		filters      []ssmtypes.ComplianceStringFilter
		wantItems    int
		wantSummary  int
		wantResource int
	}{
		{"none", nil, 3, 2, 2},
		{"patch only", []ssmtypes.ComplianceStringFilter{patch}, 2, 1, 2},
		{"patch and noncompliant", []ssmtypes.ComplianceStringFilter{patch, nonCompliant}, 1, 1, 1},
		{"not patch", []ssmtypes.ComplianceStringFilter{notPatch}, 1, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items, err := client.ListComplianceItems(ctx, &ssmsdk.ListComplianceItemsInput{Filters: tt.filters})
			require.NoError(t, err)
			assert.Len(t, items.ComplianceItems, tt.wantItems)

			sums, err := client.ListComplianceSummaries(ctx, &ssmsdk.ListComplianceSummariesInput{Filters: tt.filters})
			require.NoError(t, err)
			assert.Len(t, sums.ComplianceSummaryItems, tt.wantSummary)

			res, err := client.ListResourceComplianceSummaries(
				ctx, &ssmsdk.ListResourceComplianceSummariesInput{Filters: tt.filters},
			)
			require.NoError(t, err)
			assert.Len(t, res.ResourceComplianceSummaryItems, tt.wantResource)
		})
	}
}
