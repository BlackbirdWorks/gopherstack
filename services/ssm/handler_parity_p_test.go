package ssm_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ptr64 returns a pointer to the given int64.
func ptr64(v int64) *int64 { return &v }

// putNParams seeds n parameters with consecutive names and returns their names.
func putNParams(t *testing.T, b *ssm.InMemoryBackend, n int) []string {
	t.Helper()

	ctx := context.Background()
	names := make([]string, n)

	for i := range n {
		name := "/parity/param-%04d"
		names[i] = "/parity/param-" + func() string {
			if i < 10 {
				return "000" + string(rune('0'+i))
			}
			return "many"
		}()

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  "/parity/param/" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Value: "value",
			Type:  "String",
		})
		require.NoError(t, err)

		names[i] = "/parity/param/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}

	return names
}

// TestSSMBounds_GetParameterHistory verifies MaxResults bounds enforcement.
func TestSSMBounds_GetParameterHistory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, err := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/p", Value: "v1", Type: "String"})
	require.NoError(t, err)

	_, err = b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/p", Value: "v2", Type: "String", Overwrite: true})
	require.NoError(t, err)

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default (50)", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid (cap)", ptr64(50), false},
		{"51 is invalid", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
		{"-1 is invalid", ptr64(-1), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
				Name:       "/hist/p",
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
				assert.Nil(t, out)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}

// TestSSMPagination_GetParameterHistory verifies multi-page round-trips.
func TestSSMPagination_GetParameterHistory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// Create 5 versions by overwriting the parameter.
	_, err := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/multi", Value: "v1", Type: "String"})
	require.NoError(t, err)

	for i := 2; i <= 5; i++ {
		_, err = b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      "/hist/multi",
			Value:     "v" + string(rune('0'+i)),
			Type:      "String",
			Overwrite: true,
		})
		require.NoError(t, err)
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

	assert.Equal(t, 5, total, "all versions seen")
	assert.Equal(t, 3, pages, "5 items / 2 per page = 3 pages (ceil)")
}

// TestSSMBounds_GetParametersByPath verifies MaxResults bounds (1-10).
func TestSSMBounds_GetParametersByPath(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, err := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/path/a", Value: "v", Type: "String"})
	require.NoError(t, err)

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default (10)", nil, false},
		{"1 is valid", ptr64(1), false},
		{"10 is valid (cap)", ptr64(10), false},
		{"11 exceeds cap", ptr64(11), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMPagination_GetParametersByPath verifies multi-page round-trips.
func TestSSMPagination_GetParametersByPath(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 5 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  "/bypath/param-" + string(rune('a'+i)),
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

// TestSSMBounds_DescribeParameters verifies MaxResults bounds (1-50).
func TestSSMBounds_DescribeParameters(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, err := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/desc/p", Value: "v", Type: "String"})
	require.NoError(t, err)

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid (cap)", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMPagination_DescribeParameters verifies multi-page round-trips.
func TestSSMPagination_DescribeParameters(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 7 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  "/describe/param-" + string(rune('a'+i)),
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

// TestSSMBounds_GetInventory verifies MaxResults bounds (1-50).
func TestSSMBounds_GetInventory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid (cap)", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMPagination_GetInventory verifies multi-page round-trips.
func TestSSMPagination_GetInventory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// Seed inventory for 5 distinct instances.
	for i := range 5 {
		instanceID := "i-" + string(rune('a'+i)) + "000000000000"
		err := b.PutInventory(ctx, &ssm.PutInventoryInput{
			InstanceID: instanceID,
			Items: []ssm.InventoryItem{
				{TypeName: "AWS:Application", SchemaVersion: "1.1", CaptureTime: "2024-01-01T00:00:00Z"},
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

// TestSSMBounds_ListComplianceItems verifies MaxResults bounds (1-50).
func TestSSMBounds_ListComplianceItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid (cap)", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMPagination_ListComplianceItems verifies multi-page round-trips.
func TestSSMPagination_ListComplianceItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// Seed 5 compliance items across two resources.
	items := make([]ssm.ComplianceItem, 3)
	for i := range 3 {
		items[i] = ssm.ComplianceItem{
			ComplianceType: "Custom",
			ID:             string(rune('a' + i)),
			Title:          "item-" + string(rune('a'+i)),
			Status:         "COMPLIANT",
			Severity:       "INFORMATIONAL",
		}
	}

	err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
		ResourceID:     "res-1",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Custom",
		Items:          items,
	})
	require.NoError(t, err)

	items2 := make([]ssm.ComplianceItem, 2)
	for i := range 2 {
		items2[i] = ssm.ComplianceItem{
			ComplianceType: "Custom",
			ID:             string(rune('x' + i)),
			Title:          "item-" + string(rune('x'+i)),
			Status:         "NON_COMPLIANT",
			Severity:       "HIGH",
		}
	}

	err = b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
		ResourceID:     "res-2",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Custom",
		Items:          items2,
	})
	require.NoError(t, err)

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

// TestSSMBounds_ListComplianceSummaries verifies MaxResults bounds (1-50).
func TestSSMBounds_ListComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid (cap)", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMBounds_DescribePatchGroups verifies MaxResults bounds (1-100).
func TestSSMBounds_DescribePatchGroups(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"100 is valid (cap)", ptr64(100), false},
		{"101 exceeds cap", ptr64(101), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMBounds_ListOpsItemRelatedItems verifies MaxResults bounds (1-50).
func TestSSMBounds_ListOpsItemRelatedItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMBounds_ListOpsItemEvents verifies MaxResults bounds (1-50).
func TestSSMBounds_ListOpsItemEvents(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMBounds_ListInventoryEntries verifies MaxResults bounds (1-50).
func TestSSMBounds_ListInventoryEntries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListInventoryEntries(ctx, &ssm.ListInventoryEntriesInput{
				InstanceID: "i-0001",
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

// TestSSMPagination_ListInventoryEntries verifies multi-page round-trips.
func TestSSMPagination_ListInventoryEntries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// Seed 5 inventory entries for one instance.
	entries := make([]map[string]string, 5)
	for i := range 5 {
		entries[i] = map[string]string{
			"Name":    "app-" + string(rune('a'+i)),
			"Version": "1.0",
		}
	}

	err := b.PutInventory(ctx, &ssm.PutInventoryInput{
		InstanceID: "i-parity-0001",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-01T00:00:00Z",
				Content:       entries,
			},
		},
	})
	require.NoError(t, err)

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.ListInventoryEntries(ctx, &ssm.ListInventoryEntriesInput{
			InstanceID: "i-parity-0001",
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

// TestSSMBounds_DescribeMaintenanceWindowsForTarget verifies MaxResults bounds (1-100).
func TestSSMBounds_DescribeMaintenanceWindowsForTarget(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"100 is valid (cap)", ptr64(100), false},
		{"101 exceeds cap", ptr64(101), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMBounds_ListResourceComplianceSummaries verifies MaxResults bounds (1-50).
func TestSSMBounds_ListResourceComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		maxResults *int64
		wantError  bool
	}{
		{"nil uses default", nil, false},
		{"1 is valid", ptr64(1), false},
		{"50 is valid", ptr64(50), false},
		{"51 exceeds cap", ptr64(51), true},
		{"0 is invalid", ptr64(0), true},
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

// TestSSMPagination_ListResourceComplianceSummaries verifies multi-page round-trips.
func TestSSMPagination_ListResourceComplianceSummaries(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	// Seed compliance for 5 resources.
	for i := range 5 {
		err := b.PutComplianceItems(ctx, &ssm.PutComplianceItemsInput{
			ResourceID:     "res-rcs-" + string(rune('a'+i)),
			ResourceType:   "ManagedInstance",
			ComplianceType: "Patch",
			Items: []ssm.ComplianceItem{
				{ComplianceType: "Patch", ID: "item-1", Title: "item-1", Status: "COMPLIANT", Severity: "LOW"},
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

// Ensure json package is referenced (used in decode helpers).
var _ = json.Marshal
