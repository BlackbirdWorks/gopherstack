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
				"MetadataModelName":          "my-model",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelExportAsScript",
			descOp:  "DescribeMetadataModelExportsAsScript",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj4",
				"Origin":                     "SOURCE",
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
				"Origin":                     "SOURCE",
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

// TestStartExtensionPackAssociation verifies StartExtensionPackAssociation
// records a real request row visible via DescribeExtensionPackAssociations
// (previously a disguised no-op: it returned a random UUID without ever
// touching backend state, so the request was always invisible afterward).
func TestStartExtensionPackAssociation(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	pre := parseJSON(t, doDMS(t, h, "DescribeExtensionPackAssociations", map[string]any{
		"MigrationProjectIdentifier": "epa-proj",
	}))
	assert.Empty(t, pre["Requests"])

	startRec := doDMS(t, h, "StartExtensionPackAssociation", map[string]any{
		"MigrationProjectIdentifier": "epa-proj",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	reqID := parseJSON(t, startRec)["RequestIdentifier"].(string)
	assert.NotEmpty(t, reqID)

	post := parseJSON(t, doDMS(t, h, "DescribeExtensionPackAssociations", map[string]any{
		"MigrationProjectIdentifier": "epa-proj",
	}))
	reqs := post["Requests"].([]any)
	require.Len(t, reqs, 1)
	assert.Equal(t, reqID, reqs[0].(map[string]any)["RequestIdentifier"])

	missing := doDMS(t, h, "StartExtensionPackAssociation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, missing.Code)
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
	cancelled := parseJSON(t, rec)["Request"].(map[string]any)
	assert.Equal(t, reqID, cancelled["RequestIdentifier"])

	// Status should be "cancelling".
	descResp := parseJSON(t, doDMS(t, h, "DescribeMetadataModelConversions", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
	}))
	reqs := descResp["Requests"].([]any)
	require.Len(t, reqs, 1)
	assert.Equal(t, "cancelling", reqs[0].(map[string]any)["Status"])
}

// TestDescribeMetadataModel locks gap #1 from PARITY.md: DescribeMetadataModel
// previously always returned an empty {} instead of validating its three
// required fields (MigrationProjectIdentifier, Origin, SelectionRules) and
// the real {Definition, MetadataModelName, MetadataModelType,
// TargetMetadataModels} shape.
func TestDescribeMetadataModel(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"MigrationProjectIdentifier": "dmm-proj",
		"Origin":                     "SOURCE",
		"SelectionRules":             `{"rules":[]}`,
	}

	cases := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{name: "all fields present", body: full, wantCode: http.StatusOK},
		{
			name: "missing MigrationProjectIdentifier",
			body: withoutKey(full, "MigrationProjectIdentifier"), wantCode: http.StatusBadRequest,
		},
		{name: "missing Origin", body: withoutKey(full, "Origin"), wantCode: http.StatusBadRequest},
		{
			name: "invalid Origin",
			body: mergeMaps(full, map[string]any{"Origin": "bogus"}), wantCode: http.StatusBadRequest,
		},
		{
			name: "missing SelectionRules",
			body: withoutKey(full, "SelectionRules"), wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "DescribeMetadataModel", tc.body)
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode == http.StatusOK {
				resp := parseJSON(t, rec)
				targets, ok := resp["TargetMetadataModels"].([]any)
				require.True(t, ok, "TargetMetadataModels must be present (even if empty)")
				assert.Empty(t, targets)
			}
		})
	}
}

// TestDescribeMetadataModelChildren locks the wire-shape fix for the
// metadata-model deferred family: the real field name is
// "MetadataModelChildren" (a list of MetadataModelReference), not "Items".
func TestDescribeMetadataModelChildren(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DescribeMetadataModelChildren", map[string]any{
		"MigrationProjectIdentifier": "dmc-proj",
		"Origin":                     "TARGET",
		"SelectionRules":             `{"rules":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseJSON(t, rec)
	children, ok := resp["MetadataModelChildren"].([]any)
	require.True(t, ok, "response must use the MetadataModelChildren field name")
	assert.Empty(t, children)

	missingOrigin := doDMS(t, h, "DescribeMetadataModelChildren", map[string]any{
		"MigrationProjectIdentifier": "dmc-proj",
		"SelectionRules":             `{"rules":[]}`,
	})
	assert.Equal(t, http.StatusBadRequest, missingOrigin.Code)
}

// TestGetTargetSelectionRules locks the wire-shape fix: the real output is a
// single TargetSelectionRules string, not a "Rules" list.
func TestGetTargetSelectionRules(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "GetTargetSelectionRules", map[string]any{
		"MigrationProjectIdentifier": "gtsr-proj",
		"SelectionRules":             `{"rules":[{"rule-type":"selection"}]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseJSON(t, rec)
	rules, ok := resp["TargetSelectionRules"].(string)
	require.True(t, ok, "response must use the TargetSelectionRules scalar field name")
	assert.NotEmpty(t, rules)

	missing := doDMS(t, h, "GetTargetSelectionRules", map[string]any{"MigrationProjectIdentifier": "gtsr-proj"})
	assert.Equal(t, http.StatusBadRequest, missing.Code)
}

func listField(m map[string]any) []any {
	for _, k := range []string{"Requests", "Items"} {
		if v, ok := m[k].([]any); ok {
			return v
		}
	}

	return nil
}

func TestHandler_CancelMetadataModelConversion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
				"RequestIdentifier":          "req-abc",
			},
		},
		{
			name:     "missing_project_identifier",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"RequestIdentifier": "req-abc",
			},
		},
		{
			name:     "missing_request_identifier",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CancelMetadataModelConversion", tt.input)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := parseJSON(t, rec)
				req, ok := resp["Request"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "req-abc", req["RequestIdentifier"])
			}
		})
	}
}

func TestHandler_CancelMetadataModelCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
				"RequestIdentifier":          "req-xyz",
			},
		},
		{
			name:     "missing_project",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"RequestIdentifier": "req-xyz",
			},
		},
		{
			name:     "missing_request_identifier",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CancelMetadataModelCreation", tt.input)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := parseJSON(t, rec)
				req, ok := resp["Request"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "req-xyz", req["RequestIdentifier"])
			}
		})
	}
}
