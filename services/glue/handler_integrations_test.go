package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeInboundIntegrations verifies integrations are returned from the backend.
func TestDescribeInboundIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input        map[string]any
		name         string
		wantMinCount int
		createCount  int
	}{
		{
			name:         "no integrations returns empty list",
			createCount:  0,
			input:        map[string]any{},
			wantMinCount: 0,
		},
		{
			name:         "one integration returned",
			createCount:  1,
			input:        map[string]any{},
			wantMinCount: 1,
		},
		{
			name:         "two integrations both returned",
			createCount:  2,
			input:        map[string]any{},
			wantMinCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tc.createCount {
				rec := doGlueRequest(t, h, "CreateIntegration", map[string]any{
					"IntegrationName": "integ-" + string(rune('a'+i)),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "DescribeInboundIntegrations", tc.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Integrations []any `json:"Integrations"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.GreaterOrEqual(t, len(out.Integrations), tc.wantMinCount)
		})
	}
}

// TestDeleteIntegrationResourceProperty verifies creation then deletion round-trip.
func TestDeleteIntegrationResourceProperty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		createFirst bool
		wantCode    int
	}{
		{
			name:        "delete existing resource property succeeds",
			resourceArn: "arn:aws:glue:us-east-1:000000000000:resource/myres",
			createFirst: true,
			wantCode:    http.StatusOK,
		},
		{
			name:        "delete non-existent resource property returns 400",
			resourceArn: "arn:aws:glue:us-east-1:000000000000:resource/noresource",
			createFirst: false,
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "empty ResourceArn returns 400",
			resourceArn: "",
			createFirst: false,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.createFirst {
				rec := doGlueRequest(t, h, "CreateIntegrationResourceProperty", map[string]any{
					"ResourceArn":      tc.resourceArn,
					"SourceProperties": map[string]string{"key": "val"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "DeleteIntegrationResourceProperty", map[string]any{
				"ResourceArn": tc.resourceArn,
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDeleteIntegrationTableProperties verifies creation then deletion round-trip.
func TestDeleteIntegrationTableProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		tableName   string
		createFirst bool
		wantCode    int
	}{
		{
			name:        "delete existing table properties succeeds",
			resourceArn: "arn:aws:glue:us-east-1:000000000000:resource/myres",
			tableName:   "orders",
			createFirst: true,
			wantCode:    http.StatusOK,
		},
		{
			name:        "delete non-existent table properties returns 400",
			resourceArn: "arn:aws:glue:us-east-1:000000000000:resource/myres",
			tableName:   "no_table",
			createFirst: false,
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "empty ResourceArn returns 400",
			resourceArn: "",
			tableName:   "orders",
			createFirst: false,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.createFirst {
				rec := doGlueRequest(t, h, "CreateIntegrationTableProperties", map[string]any{
					"ResourceArn": tc.resourceArn,
					"TableName":   tc.tableName,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "DeleteIntegrationTableProperties", map[string]any{
				"ResourceArn": tc.resourceArn,
				"TableName":   tc.tableName,
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestBatch2_Integration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(t, h, "CreateIntegration", map[string]any{"IntegrationName": "my-integration"})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeIntegrations
	rec = doGlueRequest(t, h, "DescribeIntegrations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Integrations")

	// ModifyIntegration
	rec = doGlueRequest(t, h, "ModifyIntegration", map[string]any{"IntegrationIdentifier": "my-integration"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteIntegration
	rec = doGlueRequest(t, h, "DeleteIntegration", map[string]any{"IntegrationIdentifier": "my-integration"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListIntegrationResourceProperties_ReturnsStoredEntries verifies the op
// reads real entries created via CreateIntegrationResourceProperty and uses
// the real IntegrationResourcePropertyList wire field.
func TestListIntegrationResourceProperties_ReturnsStoredEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "ListIntegrationResourceProperties", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty struct {
		IntegrationResourcePropertyList []map[string]any `json:"IntegrationResourcePropertyList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	assert.Empty(t, empty.IntegrationResourcePropertyList)

	doGlueRequest(t, h, "CreateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      "arn:aws:glue:us-east-1:123456789012:connection/conn1",
		"SourceProperties": map[string]string{"key": "val"},
	})

	rec = doGlueRequest(t, h, "ListIntegrationResourceProperties", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		IntegrationResourcePropertyList []map[string]any `json:"IntegrationResourcePropertyList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.IntegrationResourcePropertyList, 1)
	assert.Equal(t, "arn:aws:glue:us-east-1:123456789012:connection/conn1",
		out.IntegrationResourcePropertyList[0]["ResourceArn"])
}

// TestUpdateIntegrationResourceProperty_RequiresExistingEntry verifies the op
// mutates a previously created entry instead of no-op'ing.
func TestUpdateIntegrationResourceProperty_RequiresExistingEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:glue:us-east-1:123456789012:connection/conn1"

	rec := doGlueRequest(t, h, "UpdateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      arn,
		"SourceProperties": map[string]string{"a": "b"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no entry created yet")

	doGlueRequest(t, h, "CreateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      arn,
		"SourceProperties": map[string]string{"key": "original"},
	})

	rec = doGlueRequest(t, h, "UpdateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      arn,
		"SourceProperties": map[string]string{"key": "updated"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetIntegrationResourceProperty", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SourceProperties map[string]string `json:"SourceProperties"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "updated", out.SourceProperties["key"])
}

// TestUpdateIntegrationTableProperties_RequiresExistingEntry mirrors
// TestUpdateIntegrationResourceProperty_RequiresExistingEntry for table props.
func TestUpdateIntegrationTableProperties_RequiresExistingEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:glue:us-east-1:123456789012:connection/conn1"

	rec := doGlueRequest(t, h, "UpdateIntegrationTableProperties", map[string]any{
		"ResourceArn": arn,
		"TableName":   "tbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no entry created yet")

	doGlueRequest(t, h, "CreateIntegrationTableProperties", map[string]any{
		"ResourceArn":       arn,
		"TableName":         "tbl",
		"SourceTableConfig": map[string]any{"key": "original"},
	})

	rec = doGlueRequest(t, h, "UpdateIntegrationTableProperties", map[string]any{
		"ResourceArn":       arn,
		"TableName":         "tbl",
		"SourceTableConfig": map[string]any{"key": "updated"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetIntegrationTableProperties", map[string]any{
		"ResourceArn": arn, "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SourceTableConfig map[string]string `json:"SourceTableConfig"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "updated", out.SourceTableConfig["key"])
}

// TestIntegration_ErrorPropagation verifies integration create/delete/modify lifecycle.
func TestIntegration_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create_ok",
			action:   "CreateIntegration",
			input:    map[string]any{"IntegrationName": "my-integration"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteIntegration",
			input:    map[string]any{"IntegrationIdentifier": "no-such-integration"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "modify_not_found",
			action:   "ModifyIntegration",
			input:    map[string]any{"IntegrationIdentifier": "no-such-integration"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tc.action, tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestIntegrationResourceProperty verifies CreateIntegrationResourceProperty stores and
// GetIntegrationResourceProperty retrieves properties by ARN.
func TestIntegrationResourceProperty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createInput map[string]any
		getInput    map[string]any
		name        string
		wantCreate  int
		wantGet     int
	}{
		{
			name:        "missing_arn_returns_400",
			createInput: map[string]any{},
			wantCreate:  http.StatusBadRequest,
		},
		{
			name: "create_and_retrieve",
			createInput: map[string]any{
				"ResourceArn":      "arn:aws:glue:us-east-1:123:resource/r1",
				"SourceProperties": map[string]any{"key": "val"},
			},
			getInput:   map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:resource/r1"},
			wantCreate: http.StatusOK,
			wantGet:    http.StatusOK,
		},
		{
			name:     "get_missing_returns_400",
			getInput: map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:resource/no-such"},
			wantGet:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.createInput != nil {
				rec := doGlueRequest(t, h, "CreateIntegrationResourceProperty", tc.createInput)
				assert.Equal(t, tc.wantCreate, rec.Code, "create")
			}

			if tc.getInput != nil {
				rec := doGlueRequest(t, h, "GetIntegrationResourceProperty", tc.getInput)
				assert.Equal(t, tc.wantGet, rec.Code, "get")
			}
		})
	}
}

// TestIntegrationTableProperties verifies create/get table property round-trip.
func TestIntegrationTableProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createInput map[string]any
		getInput    map[string]any
		name        string
		wantCreate  int
		wantGet     int
	}{
		{
			name:        "missing_fields_returns_400",
			createInput: map[string]any{},
			wantCreate:  http.StatusBadRequest,
		},
		{
			name:        "create_and_retrieve",
			createInput: map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:table/t1", "TableName": "orders"},
			getInput:    map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:table/t1", "TableName": "orders"},
			wantCreate:  http.StatusOK,
			wantGet:     http.StatusOK,
		},
		{
			name:     "get_missing_returns_400",
			getInput: map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:table/t1", "TableName": "no-such"},
			wantGet:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.createInput != nil {
				rec := doGlueRequest(t, h, "CreateIntegrationTableProperties", tc.createInput)
				assert.Equal(t, tc.wantCreate, rec.Code, "create")
			}

			if tc.getInput != nil {
				rec := doGlueRequest(t, h, "GetIntegrationTableProperties", tc.getInput)
				assert.Equal(t, tc.wantGet, rec.Code, "get")
			}
		})
	}
}
