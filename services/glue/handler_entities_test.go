package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// createTestConnection creates a JDBC connection through the HTTP handler.
func createTestConnection(t *testing.T, h *glue.Handler) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "c",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_DescribeEntity_Fields verifies DescribeEntity returns real schema fields
// with AWS-shaped field metadata over HTTP.
func TestHandler_DescribeEntity_Fields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestConnection(t, h)

	rec := doGlueRequest(t, h, "DescribeEntity", map[string]any{
		"ConnectionName": "c",
		"EntityName":     "Account",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Fields []struct {
			FieldName string `json:"FieldName"`
			FieldType string `json:"FieldType"`
		} `json:"Fields"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Fields)

	names := make([]string, 0, len(out.Fields))
	for _, f := range out.Fields {
		assert.NotEmpty(t, f.FieldType)
		names = append(names, f.FieldName)
	}
	assert.Contains(t, names, "Id")
	assert.Contains(t, names, "AnnualRevenue")
}

// TestHandler_DescribeEntity_UnknownEntity verifies an unknown entity is
// EntityNotFoundException, not an empty success.
func TestHandler_DescribeEntity_UnknownEntity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestConnection(t, h)

	rec := doGlueRequest(t, h, "DescribeEntity", map[string]any{
		"ConnectionName": "c",
		"EntityName":     "Nope",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
}

// TestHandler_ListEntities verifies ListEntities requires a connection and returns
// the catalog descriptors over HTTP.
func TestHandler_ListEntities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		seedConn bool
		wantCode int
		wantMin  int
	}{
		{
			name:     "missing connection name is 400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown connection is 400",
			input:    map[string]any{"ConnectionName": "ghost"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid connection lists entities",
			seedConn: true,
			input:    map[string]any{"ConnectionName": "c"},
			wantCode: http.StatusOK,
			wantMin:  5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.seedConn {
				createTestConnection(t, h)
			}

			rec := doGlueRequest(t, h, "ListEntities", tc.input)
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				Entities []struct {
					EntityName string `json:"EntityName"`
				} `json:"Entities"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.GreaterOrEqual(t, len(out.Entities), tc.wantMin)
		})
	}
}

// TestHandler_GetEntityRecords_Pagination verifies GetEntityRecords paginates over
// HTTP, returning a next token and a distinct second page.
func TestHandler_GetEntityRecords_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestConnection(t, h)

	rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
		"ConnectionName": "c",
		"EntityName":     "Product",
		"Limit":          2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string           `json:"NextToken"`
		Records   []map[string]any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Records, 2)
	require.NotEmpty(t, page1.NextToken)

	rec2 := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
		"ConnectionName": "c",
		"EntityName":     "Product",
		"Limit":          2,
		"NextToken":      page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Records []map[string]any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	require.NotEmpty(t, page2.Records)
	assert.NotEqual(t, page1.Records[0]["Id"], page2.Records[0]["Id"])
}

// TestHandler_ConnectionTypeLifecycle verifies the register → list → describe →
// delete lifecycle over HTTP.
func TestHandler_ConnectionTypeLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a custom type.
	reg := doGlueRequest(t, h, "RegisterConnectionType", map[string]any{
		"ConnectionType": "AcmeConn",
		"Description":    "Acme connector",
	})
	require.Equal(t, http.StatusOK, reg.Code)

	// It appears in ListConnectionTypes alongside built-ins.
	list := doGlueRequest(t, h, "ListConnectionTypes", map[string]any{})
	require.Equal(t, http.StatusOK, list.Code)

	var listOut struct {
		ConnectionTypes []struct {
			ConnectionType string `json:"ConnectionType"`
		} `json:"ConnectionTypes"`
	}
	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &listOut))

	names := make([]string, 0, len(listOut.ConnectionTypes))
	for _, ct := range listOut.ConnectionTypes {
		names = append(names, ct.ConnectionType)
	}
	assert.Contains(t, names, "ACMECONN")
	assert.Contains(t, names, "JDBC")

	// Describe the custom type.
	desc := doGlueRequest(t, h, "DescribeConnectionType", map[string]any{"ConnectionType": "AcmeConn"})
	require.Equal(t, http.StatusOK, desc.Code)

	// Delete it, then a second delete is EntityNotFound.
	del := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "AcmeConn"})
	require.Equal(t, http.StatusOK, del.Code)

	del2 := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "AcmeConn"})
	assert.Equal(t, http.StatusBadRequest, del2.Code)
	assert.Contains(t, del2.Body.String(), "EntityNotFoundException")

	// Deleting a built-in type is rejected with AccessDeniedException.
	delBuiltIn := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "JDBC"})
	assert.Equal(t, http.StatusBadRequest, delBuiltIn.Code)
	assert.Contains(t, delBuiltIn.Body.String(), "AccessDeniedException")
}

// TestDescribeEntity verifies input validation and connection existence check.
func TestDescribeEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing ConnectionName returns 400",
			input:    map[string]any{"EntityName": "Account"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing EntityName returns 400",
			input:    map[string]any{"ConnectionName": "myconn"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent connection returns 400",
			input:    map[string]any{"ConnectionName": "noconn", "EntityName": "Account"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "DescribeEntity", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestGetEntityRecords verifies input validation and connection existence check.
func TestGetEntityRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing ConnectionName returns 400",
			input:    map[string]any{"EntityName": "Account"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing EntityName returns 400",
			input:    map[string]any{"ConnectionName": "myconn"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent connection returns 400",
			input:    map[string]any{"ConnectionName": "noconn", "EntityName": "Account"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetEntityRecords", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
