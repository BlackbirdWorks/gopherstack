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

// TestHandler_ListEntities verifies ListEntities does not require a connection
// (ConnectionName is optional — glue@v1.152.0 api_op_ListEntities.go:29-49 declares
// no required members) and returns the catalog descriptors over HTTP.
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
			name:     "missing connection name lists native catalog, not a 400",
			input:    map[string]any{},
			wantCode: http.StatusOK,
			wantMin:  0,
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
		"ConnectionType":  "AcmeConn",
		"Description":     "Acme connector",
		"IntegrationType": "REST",
		"ConnectionProperties": map[string]any{
			"Url": map[string]any{"Name": "endpoint"},
		},
		"ConnectorAuthenticationConfiguration": map[string]any{
			"AuthenticationTypes": []any{"BASIC"},
		},
		"RestConfiguration": map[string]any{
			"ValidationEndpointConfiguration": map[string]any{"RequestMethod": "GET"},
		},
	})
	require.Equal(t, http.StatusOK, reg.Code)

	var regOut struct {
		ConnectionTypeArn string `json:"ConnectionTypeArn"`
	}
	require.NoError(t, json.Unmarshal(reg.Body.Bytes(), &regOut))
	assert.NotEmpty(t, regOut.ConnectionTypeArn, "RegisterConnectionType must return ConnectionTypeArn")

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

	// Describe the custom type. RestConfiguration must round-trip: it's the
	// one field RegisterConnectionType's input and DescribeConnectionType's
	// real output share verbatim (glue@v1.152.0 api_op_DescribeConnectionType.go:76-79).
	desc := doGlueRequest(t, h, "DescribeConnectionType", map[string]any{"ConnectionType": "AcmeConn"})
	require.Equal(t, http.StatusOK, desc.Code)

	var descOut struct {
		RestConfiguration map[string]any `json:"RestConfiguration"`
	}
	require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.RestConfiguration)
	validationCfg, _ := descOut.RestConfiguration["ValidationEndpointConfiguration"].(map[string]any)
	require.NotNil(t, validationCfg)
	assert.Equal(t, "GET", validationCfg["RequestMethod"])

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
			name:     "missing EntityName returns 400",
			input:    map[string]any{"ConnectionName": "myconn", "Limit": 5},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing Limit returns 400",
			input:    map[string]any{"ConnectionName": "myconn", "EntityName": "Account"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent connection returns 400",
			input:    map[string]any{"ConnectionName": "noconn", "EntityName": "Account", "Limit": 5},
			wantCode: http.StatusBadRequest,
		},
		{
			// ConnectionName is optional (glue@v1.152.0
			// api_op_GetEntityRecords.go:35-48): this resolves via the
			// native-catalog path, where "Account" is not a "database.table"
			// name, so it is EntityNotFoundException, not the old
			// ConnectionName-required 400.
			name:     "missing ConnectionName resolves via native catalog, unqualified name not found",
			input:    map[string]any{"EntityName": "Account", "Limit": 5},
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

// seedNativeCatalog creates database "salesdb" with table "orders" (five
// StorageDescriptor columns spanning every EntityField FieldType this file maps,
// plus one partition key) for native-catalog ListEntities/GetEntityRecords tests.
func seedNativeCatalog(t *testing.T, h *glue.Handler) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "salesdb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "salesdb",
		"TableInput": map[string]any{
			"Name": "orders",
			"StorageDescriptor": map[string]any{
				"Columns": []map[string]any{
					{"Name": "id", "Type": "bigint"},
					{"Name": "name", "Type": "string"},
					{"Name": "amount", "Type": "decimal(10,2)"},
					{"Name": "active", "Type": "boolean"},
					{"Name": "created_at", "Type": "timestamp"},
				},
			},
			"PartitionKeys": []map[string]any{
				{"Name": "dt", "Type": "date"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_ListEntities_NativeCatalog verifies ListEntities' native Amazon S3
// Glue Data Catalog path: omitting ConnectionName lists real databases/tables from
// gopherstack's own catalog rather than erroring, ParentEntityName drills into a
// database's tables, and a connection-scoped call still filters to the connector's
// own canned catalog (the regression guard).
func TestHandler_ListEntities_NativeCatalog(t *testing.T) {
	t.Parallel()

	t.Run("empty catalog is an empty list not an error", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doGlueRequest(t, h, "ListEntities", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Entities []glue.EntityDescriptor `json:"Entities"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.Entities)
	})

	t.Run("top level lists databases", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "ListEntities", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Entities []glue.EntityDescriptor `json:"Entities"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.Len(t, out.Entities, 1)
		assert.Equal(t, "salesdb", out.Entities[0].EntityName)
		assert.Equal(t, "DATABASES", out.Entities[0].Category)
		assert.True(t, out.Entities[0].IsParentEntity)
	})

	t.Run("parent entity name lists a database's tables", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "ListEntities", map[string]any{"ParentEntityName": "salesdb"})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Entities []glue.EntityDescriptor `json:"Entities"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.Len(t, out.Entities, 1)
		assert.Equal(t, "salesdb.orders", out.Entities[0].EntityName)
		assert.Equal(t, "TABLES", out.Entities[0].Category)
		assert.False(t, out.Entities[0].IsParentEntity)
	})

	t.Run("unknown parent entity name is EntityNotFoundException", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doGlueRequest(t, h, "ListEntities", map[string]any{"ParentEntityName": "ghostdb"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
	})

	t.Run("connection scoped call still filters to the connector catalog", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)
		createTestConnection(t, h)

		rec := doGlueRequest(t, h, "ListEntities", map[string]any{"ConnectionName": "c"})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Entities []glue.EntityDescriptor `json:"Entities"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		names := make([]string, 0, len(out.Entities))
		for _, e := range out.Entities {
			names = append(names, e.EntityName)
		}
		assert.Contains(t, names, "Account")
		assert.NotContains(t, names, "salesdb")
		assert.NotContains(t, names, "salesdb.orders")
	})
}

// TestHandler_GetEntityRecords_NativeCatalog verifies GetEntityRecords' native
// Amazon S3 Glue Data Catalog path: a "database.table" EntityName with no
// ConnectionName returns records shaped by the table's real columns, and that
// Limit stays required and connection-scoped names stay out of native lookup.
func TestHandler_GetEntityRecords_NativeCatalog(t *testing.T) {
	t.Parallel()

	t.Run("records conform to the table schema", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
			"EntityName": "salesdb.orders",
			"Limit":      3,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Records []map[string]any `json:"Records"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.Len(t, out.Records, 3)

		for _, r := range out.Records {
			assert.Contains(t, r, "id")
			assert.Contains(t, r, "amount")
			assert.Contains(t, r, "dt", "partition key columns must be queryable fields too")
			_, ok := r["created_at"].(float64)
			assert.True(t, ok, "created_at must be an epoch number")
		}
	})

	t.Run("bare database name is not a queryable entity", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
			"EntityName": "salesdb",
			"Limit":      3,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
	})

	t.Run("unknown table is EntityNotFoundException", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
			"EntityName": "salesdb.ghost",
			"Limit":      3,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
	})

	t.Run("pagination across the native table's sample records", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
			"EntityName": "salesdb.orders",
			"Limit":      2,
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
			"EntityName": "salesdb.orders",
			"Limit":      2,
			"NextToken":  page1.NextToken,
		})
		require.Equal(t, http.StatusOK, rec2.Code)

		var page2 struct {
			Records []map[string]any `json:"Records"`
		}
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
		require.NotEmpty(t, page2.Records)
		assert.NotEqual(t, page1.Records[0]["id"], page2.Records[0]["id"])
	})

	t.Run("Limit is required even without a ConnectionName", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{"EntityName": "salesdb.orders"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("connection scoped entity name does not leak into native lookup", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedNativeCatalog(t, h)

		rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
			"EntityName": "Account",
			"Limit":      3,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
	})
}
