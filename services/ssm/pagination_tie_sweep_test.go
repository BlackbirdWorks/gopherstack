package ssm_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func int64p(n int64) *int64 { return new(n) }
func int32p(n int32) *int32 { return new(n) }

// pageWalkTestPageSize is the MaxResults every test in this file requests --
// small enough (relative to each test's total of 12) to force several page
// boundaries within the tie group.
const pageWalkTestPageSize = 5

// assertPageWalkReproducesSet repeatedly (30x, since map iteration is
// randomized per-call, not per-process) walks fetch page-by-page and asserts
// the concatenation of every page reproduces want exactly -- no drops, no
// duplicates across the page boundary.
func assertPageWalkReproducesSet(
	t *testing.T,
	want map[string]bool,
	fetch func(nextToken string) (ids []string, next string),
) {
	t.Helper()

	total := len(want)

	for iter := range 30 {
		got := make(map[string]int, total)

		var token string
		for range total/pageWalkTestPageSize + 2 {
			ids, next := fetch(token)
			for _, id := range ids {
				got[id]++
			}

			if next == "" {
				break
			}

			token = next
		}

		require.Lenf(
			t, got, total,
			"iteration %d: page walk produced %d distinct items, want %d", iter, len(got), total,
		)

		for id := range want {
			require.Equalf(
				t, 1, got[id],
				"iteration %d: item %s appeared %d times across the page walk", iter, id, got[id],
			)
		}
	}
}

// TestListAssociations_PageWalkReproducesFullSet proves ListAssociations
// sorts nothing before paginating: it builds its list from
// associationsStore.All() (a store.Table map walk, unstable between calls)
// and hands it straight to paginateSlice's offset scheme. Looped: a single
// walk can pass by luck.
func TestListAssociations_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		out, err := b.CreateAssociation(ctx, &ssm.CreateAssociationInput{
			Name:       "AWS-RunShellScript",
			InstanceID: "i-" + strconv.Itoa(i),
		})
		require.NoError(t, err)
		want[out.AssociationDescription.AssociationID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.ListAssociations(
			ctx,
			&ssm.ListAssociationsInput{MaxResults: int32p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.Associations))
		for i, a := range out.Associations {
			ids[i] = a.AssociationID
		}

		return ids, out.NextToken
	})
}

// TestDescribeMaintenanceWindows_PageWalkReproducesFullSet proves
// DescribeMaintenanceWindows builds its list from
// maintenanceWindowsStore.All() (a store.Table map walk) and paginates it
// unsorted.
func TestDescribeMaintenanceWindows_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		out, err := b.CreateMaintenanceWindow(ctx, &ssm.CreateMaintenanceWindowInput{
			Name:     "mw-" + strconv.Itoa(i),
			Schedule: "cron(0 0 * * ? *)",
			Duration: 1,
			Cutoff:   0,
		})
		require.NoError(t, err)
		want[out.WindowID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.DescribeMaintenanceWindows(
			ctx,
			&ssm.DescribeMaintenanceWindowsInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.WindowIdentities))
		for i, w := range out.WindowIdentities {
			ids[i] = w.WindowID
		}

		return ids, out.NextToken
	})
}

// TestDescribeMaintenanceWindowsForTarget_PageWalkReproducesFullSet proves
// DescribeMaintenanceWindowsForTarget builds its matched-window ID set from
// maintenanceWindowTargetsStore.All() and then ranges a local
// map[string]struct{} to build identities -- two layers of unspecified Go
// map order -- before paginating unsorted.
func TestDescribeMaintenanceWindowsForTarget_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		mw, err := b.CreateMaintenanceWindow(ctx, &ssm.CreateMaintenanceWindowInput{
			Name:     "mw-target-" + strconv.Itoa(i),
			Schedule: "cron(0 0 * * ? *)",
			Duration: 1,
			Cutoff:   0,
		})
		require.NoError(t, err)

		_, err = b.RegisterTargetWithMaintenanceWindow(ctx, &ssm.RegisterTargetWithMaintenanceWindowInput{
			WindowID:     mw.WindowID,
			ResourceType: "INSTANCE",
			Targets: []ssm.WindowTarget{
				{Key: "tag:Environment", Values: []string{"prod"}},
			},
		})
		require.NoError(t, err)

		want[mw.WindowID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.DescribeMaintenanceWindowsForTarget(ctx, &ssm.DescribeMaintenanceWindowsForTargetInput{
			ResourceType: "INSTANCE",
			Targets: []ssm.WindowTarget{
				{Key: "tag:Environment", Values: []string{"prod"}},
			},
			MaxResults: int64p(pageWalkTestPageSize),
			NextToken:  token,
		})
		require.NoError(t, err)

		ids := make([]string, len(out.WindowIdentities))
		for i, w := range out.WindowIdentities {
			ids[i] = w.WindowID
		}

		return ids, out.NextToken
	})
}

// TestGetInventory_PageWalkReproducesFullSet proves GetInventory builds its
// entity list from a raw `map[string][]InventoryItem` walk (b.inventory,
// keyed by instance ID) and paginates it unsorted.
func TestGetInventory_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		instanceID := "i-inv-" + strconv.Itoa(i)
		_, err := b.PutInventory(ctx, &ssm.PutInventoryInput{
			InstanceID: instanceID,
			Items: []ssm.InventoryItem{
				{TypeName: "Custom:App", SchemaVersion: "1.0", CaptureTime: "2024-01-01T00:00:00Z"},
			},
		})
		require.NoError(t, err)
		want[instanceID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.GetInventory(
			ctx,
			&ssm.GetInventoryInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.Entities))
		for i, e := range out.Entities {
			ids[i] = e.ID
		}

		return ids, out.NextToken
	})
}

// TestListComplianceItems_PageWalkReproducesFullSet proves ListComplianceItems
// builds its list from a raw `map[string][]ComplianceItem` walk (b.compliance,
// keyed by resource ID) and paginates it unsorted.
func TestListComplianceItems_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		resourceID := "res-" + strconv.Itoa(i)
		_, err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
			ResourceID:       resourceID,
			ResourceType:     "ManagedInstance",
			ComplianceType:   "Custom",
			ExecutionSummary: &ssm.ComplianceExecutionSummary{ExecutionTime: 1_700_000_000},
			Items: []ssm.ComplianceItem{
				{
					ResourceID:   resourceID,
					ResourceType: "ManagedInstance",
					Severity:     "INFORMATIONAL",
					Status:       "COMPLIANT",
				},
			},
		})
		require.NoError(t, err)
		want[resourceID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.ListComplianceItems(
			ctx,
			&ssm.ListComplianceItemsInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.ComplianceItems))
		for i, it := range out.ComplianceItems {
			ids[i] = it.ResourceID
		}

		return ids, out.NextToken
	})
}

// TestListComplianceSummaries_PageWalkReproducesFullSet proves
// ListComplianceSummaries builds its list from a `map[string]*complianceTally`
// keyed by ComplianceType and ranges it unsorted before paginating.
func TestListComplianceSummaries_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		complianceType := "Type-" + strconv.Itoa(i)
		_, err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
			ResourceID:       "res-summary-" + strconv.Itoa(i),
			ResourceType:     "ManagedInstance",
			ComplianceType:   complianceType,
			ExecutionSummary: &ssm.ComplianceExecutionSummary{ExecutionTime: 1_700_000_000},
			Items: []ssm.ComplianceItem{
				{
					ResourceID:     "res-summary-" + strconv.Itoa(i),
					ResourceType:   "ManagedInstance",
					ComplianceType: complianceType,
					Severity:       "INFORMATIONAL",
					Status:         "COMPLIANT",
				},
			},
		})
		require.NoError(t, err)
		want[complianceType] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.ListComplianceSummaries(
			ctx,
			&ssm.ListComplianceSummariesInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.ComplianceSummaryItems))
		for i, s := range out.ComplianceSummaryItems {
			item, ok := s.(ssm.ComplianceSummaryItem)
			require.True(t, ok, "unexpected element type %T", s)
			ids[i] = item.ComplianceType
		}

		return ids, out.NextToken
	})
}

// TestListResourceComplianceSummaries_PageWalkReproducesFullSet proves
// ListResourceComplianceSummaries builds its list from a raw
// `map[string][]ComplianceItem` walk (b.compliance, keyed by resource ID) and
// paginates it unsorted.
func TestListResourceComplianceSummaries_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		resourceID := "res-rcs-" + strconv.Itoa(i)
		_, err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
			ResourceID:       resourceID,
			ResourceType:     "ManagedInstance",
			ComplianceType:   "Custom",
			ExecutionSummary: &ssm.ComplianceExecutionSummary{ExecutionTime: 1_700_000_000},
			Items: []ssm.ComplianceItem{
				{
					ResourceID:   resourceID,
					ResourceType: "ManagedInstance",
					Severity:     "INFORMATIONAL",
					Status:       "COMPLIANT",
				},
			},
		})
		require.NoError(t, err)
		want[resourceID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.ListResourceComplianceSummaries(
			ctx,
			&ssm.ListResourceComplianceSummariesInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.ResourceComplianceSummaryItems))
		for i, s := range out.ResourceComplianceSummaryItems {
			item, ok := s.(ssm.ResourceComplianceSummaryItem)
			require.True(t, ok, "unexpected element type %T", s)
			ids[i] = item.ResourceID
		}

		return ids, out.NextToken
	})
}

// TestDescribeOpsItems_PageWalkReproducesFullSet proves DescribeOpsItems
// builds its list from opsItemsStore.All() (a store.Table map walk) and
// paginates it unsorted.
func TestDescribeOpsItems_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		out, err := b.CreateOpsItem(ctx, &ssm.CreateOpsItemInput{
			Title:       "title-" + strconv.Itoa(i),
			Source:      "EC2",
			Description: "desc",
		})
		require.NoError(t, err)
		want[out.OpsItemID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.DescribeOpsItems(
			ctx,
			&ssm.DescribeOpsItemsInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.OpsItemSummaries))
		for i, it := range out.OpsItemSummaries {
			ids[i] = it.OpsItemID
		}

		return ids, out.NextToken
	})
}

// TestListOpsItemRelatedItems_PageWalkReproducesFullSet proves that, when
// OpsItemId is omitted, ListOpsItemRelatedItems flattens
// opsItemRelatedItemsStore (a raw `map[string][]OpsItemRelatedItem` keyed by
// OpsItem ID) by ranging it directly -- unspecified Go map order -- and
// paginates the result unsorted.
func TestListOpsItemRelatedItems_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		item, err := b.CreateOpsItem(ctx, &ssm.CreateOpsItemInput{
			Title:       "related-" + strconv.Itoa(i),
			Source:      "EC2",
			Description: "desc",
		})
		require.NoError(t, err)

		rel, err := b.AssociateOpsItemRelatedItem(ctx, &ssm.AssociateOpsItemRelatedItemInput{
			OpsItemID:       item.OpsItemID,
			AssociationType: "RelatesTo",
			ResourceType:    "AWS::SSMIncidents::IncidentRecord",
			ResourceURI:     "arn:aws:ssm-incidents::123456789012:incident-record/inc-" + strconv.Itoa(i),
		})
		require.NoError(t, err)
		want[rel.AssociationID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.ListOpsItemRelatedItems(
			ctx,
			&ssm.ListOpsItemRelatedItemsInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.Summaries))
		for i, s := range out.Summaries {
			ids[i] = s.AssociationID
		}

		return ids, out.NextToken
	})
}

// TestDescribePatchBaselines_PageWalkReproducesFullSet proves
// DescribePatchBaselines builds its list from patchBaselinesStore.All() (a
// store.Table map walk) and paginates it unsorted.
func TestDescribePatchBaselines_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		out, err := b.CreatePatchBaseline(ctx, &ssm.CreatePatchBaselineInput{
			Name:            "baseline-" + strconv.Itoa(i),
			OperatingSystem: "WINDOWS",
		})
		require.NoError(t, err)
		want[out.BaselineID] = true
	}

	assertPageWalkReproducesSet(t, want, func(token string) ([]string, string) {
		out, err := b.DescribePatchBaselines(
			ctx,
			&ssm.DescribePatchBaselinesInput{MaxResults: int64p(pageWalkTestPageSize), NextToken: token},
		)
		require.NoError(t, err)

		ids := make([]string, len(out.BaselineIdentities))
		for i, bl := range out.BaselineIdentities {
			ids[i] = bl.BaselineID
		}

		return ids, out.NextToken
	})
}
