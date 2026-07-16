package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataModelRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody map[string]any
		startOp   string
		descOp    string
	}{
		{
			startOp: "StartMetadataModelAssessment",
			descOp:  "DescribeMetadataModelAssessments",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj1",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelConversion",
			descOp:  "DescribeMetadataModelConversions",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj2",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelCreation",
			descOp:  "DescribeMetadataModelCreations",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj3",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelExportAsScript",
			descOp:  "DescribeMetadataModelExportsAsScript",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj4",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelExportToTarget",
			descOp:  "DescribeMetadataModelExportsToTarget",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj5",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelImport",
			descOp:  "DescribeMetadataModelImports",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj6",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.startOp, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			// Before start: Describe returns empty.
			descBody := map[string]any{"MigrationProjectIdentifier": tt.startBody["MigrationProjectIdentifier"]}
			pre := parseJSON(t, doDMS(t, h, tt.descOp, descBody))
			assert.Empty(t, listField(pre))

			// Start the operation.
			rec := doDMS(t, h, tt.startOp, tt.startBody)
			require.Equal(t, http.StatusOK, rec.Code)
			startResp := parseJSON(t, rec)
			reqID, ok := startResp["RequestIdentifier"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, reqID)

			// After start: Describe returns the request.
			post := parseJSON(t, doDMS(t, h, tt.descOp, descBody))
			reqs := listField(post)
			require.Len(t, reqs, 1)
			req0, ok := reqs[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, reqID, req0["RequestIdentifier"])
			assert.Equal(t, "running", req0["Status"])
		})
	}
}

func TestCancelMetadataModelConversion(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Start a conversion.
	startResp := parseJSON(t, doDMS(t, h, "StartMetadataModelConversion", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
		"SelectionRules":             `{"rules":[]}`,
	}))
	reqID := startResp["RequestIdentifier"].(string)

	// Cancel it.
	rec := doDMS(t, h, "CancelMetadataModelConversion", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
		"RequestIdentifier":          reqID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, reqID, parseJSON(t, rec)["RequestIdentifier"])

	// Status should be "cancelling".
	descResp := parseJSON(t, doDMS(t, h, "DescribeMetadataModelConversions", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
	}))
	reqs := descResp["Requests"].([]any)
	require.Len(t, reqs, 1)
	assert.Equal(t, "cancelling", reqs[0].(map[string]any)["Status"])
}

func listField(m map[string]any) []any {
	for _, k := range []string{"Requests", "Items"} {
		if v, ok := m[k].([]any); ok {
			return v
		}
	}

	return nil
}
