package ssm_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ptr64 returns a pointer to the given int64.
func ptr64(v int64) *int64 {
	p := new(int64)
	*p = v

	return p
}

// ---------------------------------------------------------------------------
// GetParameterHistory — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_GetParameterHistory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/p", Value: "v1", Type: "String"})
	require.NoError(t, putErr)

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
		{ptr64(-1), "-1 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
				Name:       "/hist/p",
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				require.Error(t, err)
				assert.Nil(t, out)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_GetParameterHistory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/multi", Value: "v1", Type: "String"})
	require.NoError(t, putErr)

	for i := 2; i <= 5; i++ {
		_, putErr = b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      "/hist/multi",
			Value:     fmt.Sprintf("v%d", i),
			Type:      "String",
			Overwrite: true,
		})
		require.NoError(t, putErr)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
			Name:       "/hist/multi",
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total, "all 5 versions seen")
	assert.Equal(t, 3, pages, "ceil(5/2)=3 pages")
}

// ---------------------------------------------------------------------------
// GetParametersByPath — bounds (1-10) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_GetParametersByPath(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/path/a", Value: "v", Type: "String"})
	require.NoError(t, putErr)

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default 10", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(10), "10 is valid cap", false},
		{ptr64(11), "11 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
				Path:       "/path/",
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_GetParametersByPath(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 5 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/bypath/param-%02d", i),
			Value: "v",
			Type:  "String",
		})
		require.NoError(t, err)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
			Path:       "/bypath/",
			Recursive:  true,
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// DescribeParameters — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_DescribeParameters(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/desc/p", Value: "v", Type: "String"})
	require.NoError(t, putErr)

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.DescribeParameters(ctx, &ssm.DescribeParametersInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_DescribeParameters(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 7 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/describe/param-%02d", i),
			Value: "v",
			Type:  "String",
		})
		require.NoError(t, err)
	}

	maxR := ptr64(3)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.DescribeParameters(ctx, &ssm.DescribeParametersInput{
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 7, total)
	assert.Equal(t, 3, pages) // 3+3+1
}

// ---------------------------------------------------------------------------
// GetInventory — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_GetInventory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetInventory(ctx, &ssm.GetInventoryInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_GetInventory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 5 {
		instanceID := fmt.Sprintf("i-%012d", i+1)
		_, err := b.PutInventory(ctx, &ssm.PutInventoryInput{
			InstanceID: instanceID,
			Items: []ssm.InventoryItem{
				{
					TypeName:      "AWS:Application",
					SchemaVersion: "1.1",
					CaptureTime:   "2024-01-01T00:00:00Z",
				},
			},
		})
		require.NoError(t, err)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.GetInventory(ctx, &ssm.GetInventoryInput{
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Entities)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListInventoryEntries — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_ListInventoryEntries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListInventoryEntries(ctx, &ssm.ListInventoryEntriesInput{
				InstanceID: "i-000000000001",
				TypeName:   "AWS:Application",
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_ListInventoryEntries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// One InventoryItem with 5 Content rows.
	content := make([]map[string]string, 5)
	for i := range 5 {
		content[i] = map[string]string{
			"Name":    fmt.Sprintf("app-%02d", i),
			"Version": "1.0",
		}
	}

	_, putErr := b.PutInventory(ctx, &ssm.PutInventoryInput{
		InstanceID: "i-entries-0001",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-01T00:00:00Z",
				Content:       content,
			},
		},
	})
	require.NoError(t, putErr)

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.ListInventoryEntries(ctx, &ssm.ListInventoryEntriesInput{
			InstanceID: "i-entries-0001",
			TypeName:   "AWS:Application",
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Entries)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListComplianceItems — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_ListComplianceItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListComplianceItems(ctx, &ssm.ListComplianceItemsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_ListComplianceItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// 3 items for res-1, 2 items for res-2 → 5 total.
	items1 := make([]ssm.ComplianceItem, 3)
	for i := range 3 {
		items1[i] = ssm.ComplianceItem{
			Title:    fmt.Sprintf("item-%02d", i),
			Status:   "COMPLIANT",
			Severity: "INFORMATIONAL",
		}
	}

	_, putErr := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
		ResourceID:     "res-ci-1",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Custom",
		Items:          items1,
	})
	require.NoError(t, putErr)

	items2 := make([]ssm.ComplianceItem, 2)
	for i := range 2 {
		items2[i] = ssm.ComplianceItem{
			Title:    fmt.Sprintf("item-%02d", i+10),
			Status:   "NON_COMPLIANT",
			Severity: "HIGH",
		}
	}

	_, putErr = b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
		ResourceID:     "res-ci-2",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Custom",
		Items:          items2,
	})
	require.NoError(t, putErr)

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.ListComplianceItems(ctx, &ssm.ListComplianceItemsInput{
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.ComplianceItems)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListComplianceSummaries — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_ListComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListComplianceSummaries(ctx, &ssm.ListComplianceSummariesInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_ListComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// 5 distinct ComplianceTypes → 5 separate summary entries.
	for i := range 5 {
		ct := fmt.Sprintf("Type%02d", i)
		_, err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
			ResourceID:     fmt.Sprintf("res-cs-%02d", i),
			ResourceType:   "ManagedInstance",
			ComplianceType: ct,
			Items: []ssm.ComplianceItem{
				{Title: "item-1", Status: "COMPLIANT", Severity: "LOW"},
			},
		})
		require.NoError(t, err)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.ListComplianceSummaries(ctx, &ssm.ListComplianceSummariesInput{
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.ComplianceSummaryItems)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListResourceComplianceSummaries — bounds (1-50) + multi-page
// ---------------------------------------------------------------------------

func TestSSMBounds_ListResourceComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListResourceComplianceSummaries(ctx, &ssm.ListResourceComplianceSummariesInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

func TestSSMPagination_ListResourceComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// 5 distinct resources → 5 per-resource summaries.
	for i := range 5 {
		_, err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
			ResourceID:     fmt.Sprintf("res-rcs-%02d", i),
			ResourceType:   "ManagedInstance",
			ComplianceType: "Patch",
			Items: []ssm.ComplianceItem{
				{Title: "item-1", Status: "COMPLIANT", Severity: "LOW"},
			},
		})
		require.NoError(t, err)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.ListResourceComplianceSummaries(ctx, &ssm.ListResourceComplianceSummariesInput{
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.ResourceComplianceSummaryItems)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// DescribePatchGroups — bounds (1-100)
// ---------------------------------------------------------------------------

func TestSSMBounds_DescribePatchGroups(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(100), "100 is valid cap", false},
		{ptr64(101), "101 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.DescribePatchGroups(ctx, &ssm.DescribePatchGroupsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DescribeMaintenanceWindowsForTarget — bounds (1-100)
// ---------------------------------------------------------------------------

func TestSSMBounds_DescribeMaintenanceWindowsForTarget(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(100), "100 is valid cap", false},
		{ptr64(101), "101 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.DescribeMaintenanceWindowsForTarget(ctx, &ssm.DescribeMaintenanceWindowsForTargetInput{
				ResourceType: "INSTANCE",
				MaxResults:   tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ListOpsItemRelatedItems — bounds (1-50)
// ---------------------------------------------------------------------------

func TestSSMBounds_ListOpsItemRelatedItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListOpsItemRelatedItems(ctx, &ssm.ListOpsItemRelatedItemsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ListOpsItemEvents — bounds (1-50)
// ---------------------------------------------------------------------------

func TestSSMBounds_ListOpsItemEvents(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListOpsItemEvents(ctx, &ssm.ListOpsItemEventsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
