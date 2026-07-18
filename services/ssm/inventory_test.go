package ssm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestInventory_DeletionJobRecorded(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	_, err := b.PutInventory(ctx, &ssm.PutInventoryInput{
		InstanceID: "i-1",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Application", SchemaVersion: "1.0"},
		},
	})
	require.NoError(t, err)

	del, err := b.DeleteInventory(ctx, &ssm.DeleteInventoryInput{TypeName: "AWS:Application"})
	require.NoError(t, err)
	require.NotEmpty(t, del.DeletionID, "DeleteInventory must return a DeletionId")

	list, err := b.DescribeInventoryDeletions(ctx, &ssm.DescribeInventoryDeletionsInput{})
	require.NoError(t, err)
	require.Len(t, list.InventoryDeletions, 1,
		"the recorded deletion job must be returned")

	// Filtering by the returned DeletionId returns the same job.
	filtered, err := b.DescribeInventoryDeletions(ctx, &ssm.DescribeInventoryDeletionsInput{
		DeletionID: del.DeletionID,
	})
	require.NoError(t, err)
	assert.Len(t, filtered.InventoryDeletions, 1)
}
func ptr64(v int64) *int64 {
	p := new(int64)
	*p = v

	return p
}
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
func TestBackendOps_PutInventory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-1234567890abcdef0",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-01T00:00:00Z",
				Content: []map[string]string{
					{"Name": "AmazonSSMAgent", "Version": "3.0.0"},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
func TestBackendOps_GetInventory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-abcdef1234567890",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Network", SchemaVersion: "1.0"},
		},
	})
	require.NoError(t, err)

	out, err := b.GetInventory(context.TODO(), &ssm.GetInventoryInput{})
	require.NoError(t, err)
	assert.Len(t, out.Entities, 1)
	assert.Equal(t, "i-abcdef1234567890", out.Entities[0].ID)
}
func TestBackendOps_GetInventorySchema(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.GetInventorySchema(context.TODO(), &ssm.GetInventorySchemaInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.Schemas)
}
func TestBackendOps_ListInventoryEntries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	content := []map[string]string{{"Name": "sshd", "Version": "1.0"}}

	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-entries-test",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Service", SchemaVersion: "1.0", Content: content},
		},
	})
	require.NoError(t, err)

	out, err := b.ListInventoryEntries(context.TODO(), &ssm.ListInventoryEntriesInput{
		InstanceID: "i-entries-test",
		TypeName:   "AWS:Service",
	})
	require.NoError(t, err)
	assert.Equal(t, "i-entries-test", out.InstanceID)
	assert.Equal(t, "AWS:Service", out.TypeName)
	assert.Len(t, out.Entries, 1)
}
func TestBackendOps_DeleteInventory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-delete-test",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Application"},
			{TypeName: "AWS:Network"},
		},
	})
	require.NoError(t, err)

	out, err := b.DeleteInventory(context.TODO(), &ssm.DeleteInventoryInput{TypeName: "AWS:Application"})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Verify only AWS:Application was deleted.
	listOut, err := b.ListInventoryEntries(context.TODO(), &ssm.ListInventoryEntriesInput{
		InstanceID: "i-delete-test",
		TypeName:   "AWS:Application",
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.Entries)
}
func TestBackendOps_DescribeInventoryDeletions(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribeInventoryDeletions(context.TODO(), &ssm.DescribeInventoryDeletionsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.InventoryDeletions)
}
func TestBackendOps_PutComplianceItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-compliance-test",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Association",
		Items: []ssm.ComplianceItem{
			{Status: "COMPLIANT", Severity: "UNSPECIFIED", Title: "AssocComp"},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
func TestBackendOps_ListComplianceItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:   "i-list-compliance",
		ResourceType: "ManagedInstance",
		Items: []ssm.ComplianceItem{
			{Status: "COMPLIANT", Title: "Item1"},
		},
	})
	require.NoError(t, err)

	out, err := b.ListComplianceItems(context.TODO(), &ssm.ListComplianceItemsInput{
		ResourceID: "i-list-compliance",
	})
	require.NoError(t, err)
	assert.Len(t, out.ComplianceItems, 1)
	assert.Equal(t, "i-list-compliance", out.ComplianceItems[0].ResourceID)
}
func TestBackendOps_ListComplianceSummaries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.ListComplianceSummaries(context.TODO(), &ssm.ListComplianceSummariesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.ComplianceSummaryItems)
}
func TestBackendOps_ListResourceComplianceSummaries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.ListResourceComplianceSummaries(context.TODO(), &ssm.ListResourceComplianceSummariesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.ResourceComplianceSummaryItems)
}
func TestFull_Inventory_PutGetSchema(t *testing.T) {
	t.Parallel()
	h := newHandler()

	code, _ := postJSON(t, h, "PutInventory", map[string]any{
		"InstanceId": "i-inv001",
		"Items": []map[string]any{
			{
				"TypeName":      "AWS:Application",
				"SchemaVersion": "1.1",
				"CaptureTime":   "2024-01-01T00:00:00Z",
				"Content":       []map[string]any{{"Name": "nginx", "Version": "1.24"}},
			},
		},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := postJSON(t, h, "GetInventory", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Entities"])

	code, out = postJSON(t, h, "GetInventorySchema", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Schemas"])

	code, out = postJSON(t, h, "ListInventoryEntries", map[string]any{
		"InstanceId": "i-inv001",
		"TypeName":   "AWS:Application",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Entries"])
}
func TestFull_Compliance_PutListSummary(t *testing.T) {
	t.Parallel()
	h := newHandler()

	code, _ := postJSON(t, h, "PutComplianceItems", map[string]any{
		"ResourceId":     "i-comp001",
		"ResourceType":   "ManagedInstance",
		"ComplianceType": "Association",
		"ExecutionSummary": map[string]any{
			"ExecutionTime": "2024-01-01T00:00:00Z",
		},
		"Items": []map[string]any{
			{"Id": "a1", "Title": "patch-1", "Status": "COMPLIANT", "Severity": "UNSPECIFIED"},
		},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := postJSON(t, h, "ListComplianceItems", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ComplianceItems"])

	code, out = postJSON(t, h, "ListComplianceSummaries", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ComplianceSummaryItems"])

	code, out = postJSON(t, h, "ListResourceComplianceSummaries", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ResourceComplianceSummaryItems"])
}
func TestComplianceSummaries_AggregateByCType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Store some compliance items.
	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-abc123",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Association",
		Items: []ssm.ComplianceItem{
			{Title: "AWS-RunPatchBaseline", Status: "COMPLIANT", Severity: "UNSPECIFIED"},
			{Title: "AWS-GatherSoftwareInventory", Status: "NON_COMPLIANT", Severity: "HIGH"},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListComplianceSummaries", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items := resp["ComplianceSummaryItems"].([]any)
	assert.NotEmpty(t, items, "should have compliance summary items")

	// Find the Association type summary.
	var foundAssoc bool
	for _, item := range items {
		m := item.(map[string]any)
		if m["ComplianceType"] == "Association" {
			foundAssoc = true
		}
	}

	assert.True(t, foundAssoc, "should have Association compliance type summary")
}
func TestResourceComplianceSummaries_PerResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-res123",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Patch",
		Items: []ssm.ComplianceItem{
			{Title: "KB123456", Status: "COMPLIANT", Severity: "MEDIUM"},
			{Title: "KB789012", Status: "COMPLIANT", Severity: "LOW"},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListResourceComplianceSummaries", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items := resp["ResourceComplianceSummaryItems"].([]any)
	assert.NotEmpty(t, items)

	// The resource should show COMPLIANT status.
	var found bool
	for _, item := range items {
		m := item.(map[string]any)
		if m["ResourceId"] == "i-res123" {
			found = true
			assert.Equal(t, "COMPLIANT", m["Status"])
		}
	}

	assert.True(t, found, "should have resource compliance summary for i-res123")
}
func TestResourceComplianceSummaries_NonCompliantStatus(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-bad456",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Patch",
		Items: []ssm.ComplianceItem{
			{Title: "KB001", Status: "COMPLIANT", Severity: "LOW"},
			{Title: "KB002", Status: "NON_COMPLIANT", Severity: "HIGH"},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListResourceComplianceSummaries", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, item := range resp["ResourceComplianceSummaryItems"].([]any) {
		m := item.(map[string]any)
		if m["ResourceId"] == "i-bad456" {
			assert.Equal(t, "NON_COMPLIANT", m["Status"])
		}
	}
}
func TestGetInventorySchema_ReturnsBuiltInTypes(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetInventorySchema", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schemas := resp["Schemas"].([]any)
	assert.NotEmpty(t, schemas, "GetInventorySchema should return built-in schema types")

	// Verify key AWS types are present.
	var typeNames []string
	for _, s := range schemas {
		schema := s.(map[string]any)
		if name, ok := schema["TypeName"].(string); ok {
			typeNames = append(typeNames, name)
		}
	}

	assert.Contains(t, typeNames, "AWS:Application")
	assert.Contains(t, typeNames, "AWS:InstanceInformation")
	assert.Contains(t, typeNames, "AWS:Network")
}
func TestGetInventorySchema_FilterByTypeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		typeName     string
		wantContains string
	}{
		{
			name:         "filter_aws_application",
			typeName:     "AWS:Application",
			wantContains: "AWS:Application",
		},
		{
			name:         "filter_aws_network",
			typeName:     "AWS:Network",
			wantContains: "AWS:Network",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body, _ := json.Marshal(map[string]any{"TypeName": tt.typeName})
			rec := doRequest(t, h, "GetInventorySchema", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}
func TestInventory_TypeMergeAndLookup(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Put inventory with two types.
	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-inv001",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-01T00:00:00Z",
				Content: []map[string]string{
					{"Name": "aws-cli", "Version": "2.0"},
				},
			},
		},
	})
	require.NoError(t, err)

	// List entries for the type.
	body, _ := json.Marshal(map[string]any{
		"InstanceId": "i-inv001",
		"TypeName":   "AWS:Application",
	})
	rec := doRequest(t, h, "ListInventoryEntries", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "aws-cli")

	// Put again with same type (overwrite) and add a new type.
	_, err = b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-inv001",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-02T00:00:00Z",
				Content: []map[string]string{
					{"Name": "aws-cli", "Version": "2.1"},
				},
			},
			{
				TypeName:      "AWS:Network",
				SchemaVersion: "1.0",
				CaptureTime:   "2024-01-02T00:00:00Z",
				Content: []map[string]string{
					{"Name": "eth0", "SubnetId": "subnet-abc123"},
				},
			},
		},
	})
	require.NoError(t, err)

	// GetInventory should show both types for the instance.
	rec = doRequest(t, h, "GetInventory", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "i-inv001")

	// Application version should be updated to 2.1.
	body, _ = json.Marshal(map[string]any{
		"InstanceId": "i-inv001",
		"TypeName":   "AWS:Application",
	})
	rec = doRequest(t, h, "ListInventoryEntries", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "2.1")
}
