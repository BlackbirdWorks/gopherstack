package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckSchemaVersionValidity verifies AWS-accurate schema validation for
// AVRO, JSON, and PROTOBUF formats.
func TestCheckSchemaVersionValidity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		wantError string
		wantCode  int
		wantValid bool
	}{
		{
			name: "missing DataFormat returns 400",
			input: map[string]any{
				"SchemaDefinition": `{"type":"record","name":"T","fields":[]}`,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing SchemaDefinition returns 400",
			input:    map[string]any{"DataFormat": "AVRO"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid AVRO schema",
			input: map[string]any{
				"DataFormat":       "AVRO",
				"SchemaDefinition": `{"type":"record","name":"T","fields":[]}`,
			},
			wantCode:  http.StatusOK,
			wantValid: true,
		},
		{
			name: "invalid AVRO schema missing type field",
			input: map[string]any{
				"DataFormat":       "AVRO",
				"SchemaDefinition": `{"name":"T","fields":[]}`,
			},
			wantCode:  http.StatusOK,
			wantValid: false,
			wantError: "type",
		},
		{
			name:      "invalid AVRO schema not valid JSON",
			input:     map[string]any{"DataFormat": "AVRO", "SchemaDefinition": `not json`},
			wantCode:  http.StatusOK,
			wantValid: false,
			wantError: "JSON",
		},
		{
			name: "valid JSON schema",
			input: map[string]any{
				"DataFormat":       "JSON",
				"SchemaDefinition": `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`,
			},
			wantCode:  http.StatusOK,
			wantValid: true,
		},
		{
			name:      "invalid JSON schema",
			input:     map[string]any{"DataFormat": "JSON", "SchemaDefinition": `{broken json`},
			wantCode:  http.StatusOK,
			wantValid: false,
		},
		{
			name: "valid PROTOBUF schema",
			input: map[string]any{
				"DataFormat":       "PROTOBUF",
				"SchemaDefinition": `syntax = "proto3"; message Person { string name = 1; }`,
			},
			wantCode:  http.StatusOK,
			wantValid: true,
		},
		{
			name: "invalid PROTOBUF missing syntax",
			input: map[string]any{
				"DataFormat":       "PROTOBUF",
				"SchemaDefinition": `message Person { string name = 1; }`,
			},
			wantCode:  http.StatusOK,
			wantValid: false,
			wantError: "syntax",
		},
		{
			name: "invalid PROTOBUF missing message",
			input: map[string]any{
				"DataFormat":       "PROTOBUF",
				"SchemaDefinition": `syntax = "proto3";`,
			},
			wantCode:  http.StatusOK,
			wantValid: false,
			wantError: "message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CheckSchemaVersionValidity", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				Error string `json:"Error"`
				Valid bool   `json:"Valid"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantValid, out.Valid)

			if tc.wantError != "" {
				assert.Contains(t, out.Error, tc.wantError)
			}
		})
	}
}

// TestCreateScript verifies that CreateScript generates non-empty ETL code from DagNodes/DagEdges.
func TestCreateScript(t *testing.T) {
	t.Parallel()

	datasource := map[string]any{
		"Id":       "datasource0",
		"NodeType": "DataSource",
		"Args": []map[string]any{
			{"Name": "database", "Value": "mydb"},
			{"Name": "table_name", "Value": "mytable"},
		},
	}
	datasink := map[string]any{
		"Id":       "datasink1",
		"NodeType": "DataSink",
		"Args": []map[string]any{
			{"Name": "database", "Value": "outdb"},
			{"Name": "table_name", "Value": "outtable"},
		},
	}
	edge := map[string]any{"Source": "datasource0", "Target": "datasink1"}

	tests := []struct {
		input              map[string]any
		name               string
		wantContains       []string
		wantPythonNonEmpty bool
		wantScalaNonEmpty  bool
	}{
		{
			name: "empty DAG returns boilerplate python",
			input: map[string]any{
				"DagNodes": []any{},
				"DagEdges": []any{},
				"Language": "Python",
			},
			wantPythonNonEmpty: true,
			wantContains:       []string{"GlueContext", "job.commit()"},
		},
		{
			name: "datasource and datasink produce python with table refs",
			input: map[string]any{
				"DagNodes": []any{datasource, datasink},
				"DagEdges": []any{edge},
				"Language": "Python",
			},
			wantPythonNonEmpty: true,
			wantContains: []string{
				"mydb",
				"mytable",
				"outdb",
				"outtable",
				"create_dynamic_frame",
				"write_dynamic_frame",
			},
		},
		{
			name: "scala language generates scala not python",
			input: map[string]any{
				"DagNodes": []any{datasource},
				"DagEdges": []any{},
				"Language": "Scala",
			},
			wantScalaNonEmpty: true,
			wantContains:      []string{"GlueContext", "Job.commit()"},
		},
		{
			name: "no language defaults to python",
			input: map[string]any{
				"DagNodes": []any{},
				"DagEdges": []any{},
			},
			wantPythonNonEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateScript", tc.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				PythonScript string `json:"PythonScript"`
				ScalaCode    string `json:"ScalaCode"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tc.wantPythonNonEmpty {
				assert.NotEmpty(t, out.PythonScript)
				assert.Empty(t, out.ScalaCode)
			}

			if tc.wantScalaNonEmpty {
				assert.NotEmpty(t, out.ScalaCode)
				assert.Empty(t, out.PythonScript)
			}

			combined := out.PythonScript + out.ScalaCode
			for _, want := range tc.wantContains {
				assert.Contains(t, combined, want)
			}
		})
	}
}

// TestDeleteSchemaVersions verifies version range parsing and per-version deletion.
func TestDeleteSchemaVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input        map[string]any
		name         string
		wantCode     int
		wantErrCount int
	}{
		{
			name: "empty versions string returns empty errors",
			input: map[string]any{
				"SchemaId": map[string]any{"RegistryName": "reg", "SchemaName": "sch"},
				"Versions": "",
			},
			wantCode:     http.StatusOK,
			wantErrCount: 0,
		},
		{
			name: "invalid version string returns 400",
			input: map[string]any{
				"SchemaId": map[string]any{"RegistryName": "reg", "SchemaName": "sch"},
				"Versions": "abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "range start > end returns 400",
			input: map[string]any{
				"SchemaId": map[string]any{"RegistryName": "reg", "SchemaName": "sch"},
				"Versions": "5-3",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "DeleteSchemaVersions", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				SchemaVersionErrors []struct {
					VersionNumber int64 `json:"VersionNumber"`
				} `json:"SchemaVersionErrors"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.SchemaVersionErrors, tc.wantErrCount)
		})
	}
}

// TestDeleteSchemaVersions_Stateful verifies that existing versions are deleted
// and missing ones produce per-version errors.
func TestDeleteSchemaVersions_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Set up: registry → schema → 3 versions.
	createRegistry(t, h, "myreg")
	createSchema(t, h, "myreg", "myschema")
	registerSchemaVersion(t, h, "myreg", "myschema", `{"type":"record","name":"V1","fields":[]}`)
	registerSchemaVersion(t, h, "myreg", "myschema", `{"type":"record","name":"V2","fields":[]}`)
	registerSchemaVersion(t, h, "myreg", "myschema", `{"type":"record","name":"V3","fields":[]}`)

	// Delete versions 1 and 2 (range), plus non-existent 9.
	rec := doGlueRequest(t, h, "DeleteSchemaVersions", map[string]any{
		"SchemaId": map[string]any{"RegistryName": "myreg", "SchemaName": "myschema"},
		"Versions": "1-2,9",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SchemaVersionErrors []struct {
			VersionNumber int64 `json:"VersionNumber"`
		} `json:"SchemaVersionErrors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Version 9 doesn't exist → 1 error.
	require.Len(t, out.SchemaVersionErrors, 1)
	assert.Equal(t, int64(9), out.SchemaVersionErrors[0].VersionNumber)
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

// TestDescribeEntity_ValidConnection verifies that a valid connection returns Fields slice.
func TestDescribeEntity_ValidConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a connection so DescribeEntity can find it.
	createRec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "testconn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/mydb",
				"USERNAME":            "root",
				"PASSWORD":            "secret",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doGlueRequest(t, h, "DescribeEntity", map[string]any{
		"ConnectionName": "testconn",
		"EntityName":     "Customer",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Fields []any `json:"Fields"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out.Fields)
}

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

// TestDeleteConnectionType verifies real state mutation: built-in types are
// undeletable (distinct AccessDeniedException), unknown types are
// EntityNotFoundException, and a registered custom type can be deleted once.
func TestDeleteConnectionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "empty body missing ConnectionType returns 400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "built-in Salesforce is undeletable",
			input:    map[string]any{"ConnectionType": "Salesforce"},
			wantCode: http.StatusBadRequest,
			wantType: "AccessDeniedException",
		},
		{
			name:     "unknown type returns EntityNotFound",
			input:    map[string]any{"ConnectionType": "NoSuchType"},
			wantCode: http.StatusBadRequest,
			wantType: "EntityNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "DeleteConnectionType", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantType)
		})
	}
}

// TestDeleteConnectionType_CustomRoundTrip verifies a custom type registered via
// RegisterConnectionType can be deleted, and a second delete is EntityNotFound.
func TestDeleteConnectionType_CustomRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	regRec := doGlueRequest(t, h, "RegisterConnectionType", map[string]any{
		"ConnectionType": "MyCustomConn",
		"Description":    "custom connector",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	delRec := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "MyCustomConn"})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Second delete: already gone → EntityNotFoundException.
	del2 := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "MyCustomConn"})
	assert.Equal(t, http.StatusBadRequest, del2.Code)
	assert.Contains(t, del2.Body.String(), "EntityNotFoundException")
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

// TestGetDataflowGraph verifies DAG extraction from ETL scripts.
func TestGetDataflowGraph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         map[string]any
		name          string
		wantNodeCount int
		wantEdgeCount int
	}{
		{
			name:          "empty script returns empty nodes and edges",
			input:         map[string]any{"PythonScript": ""},
			wantNodeCount: 0,
			wantEdgeCount: 0,
		},
		{
			name: "script with datasource produces DataSource node",
			input: map[string]any{
				"PythonScript": "datasource0 = glueContext.create_dynamic_frame" +
					`.from_catalog(database="mydb", table_name="mytable",` +
					` transformation_ctx="datasource0")`,
			},
			wantNodeCount: 1,
			wantEdgeCount: 0,
		},
		{
			name: "datasource and datasink with edge",
			input: map[string]any{
				"PythonScript": "datasource0 = glueContext.create_dynamic_frame" +
					`.from_catalog(database="mydb", table_name="mytable",` +
					` transformation_ctx="datasource0")` + "\n" +
					`glueContext.write_dynamic_frame.from_catalog(` +
					`frame=datasource0, database="outdb",` +
					` table_name="outtable", transformation_ctx="datasink1")`,
			},
			wantNodeCount: 2,
			wantEdgeCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetDataflowGraph", tc.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				DagNodes []any `json:"DagNodes"`
				DagEdges []any `json:"DagEdges"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.DagNodes, tc.wantNodeCount)
			assert.Len(t, out.DagEdges, tc.wantEdgeCount)
		})
	}
}

// TestGetDataflowGraph_RoundTrip verifies CreateScript→GetDataflowGraph round-trip.
func TestGetDataflowGraph_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a script from a DAG.
	scriptRec := doGlueRequest(t, h, "CreateScript", map[string]any{
		"Language": "Python",
		"DagNodes": []any{
			map[string]any{
				"Id":       "datasource0",
				"NodeType": "DataSource",
				"Args": []any{
					map[string]any{"Name": "database", "Value": "db1"},
					map[string]any{"Name": "table_name", "Value": "tbl1"},
				},
			},
			map[string]any{
				"Id":       "datasink1",
				"NodeType": "DataSink",
				"Args": []any{
					map[string]any{"Name": "database", "Value": "outdb"},
					map[string]any{"Name": "table_name", "Value": "outtbl"},
				},
			},
		},
		"DagEdges": []any{
			map[string]any{"Source": "datasource0", "Target": "datasink1"},
		},
	})
	require.Equal(t, http.StatusOK, scriptRec.Code)

	var scriptOut struct {
		PythonScript string `json:"PythonScript"`
	}
	require.NoError(t, json.Unmarshal(scriptRec.Body.Bytes(), &scriptOut))
	require.NotEmpty(t, scriptOut.PythonScript)

	// Parse the script back to a DAG.
	graphRec := doGlueRequest(t, h, "GetDataflowGraph", map[string]any{
		"PythonScript": scriptOut.PythonScript,
	})
	require.Equal(t, http.StatusOK, graphRec.Code)

	var graphOut struct {
		DagNodes []any `json:"DagNodes"`
		DagEdges []any `json:"DagEdges"`
	}
	require.NoError(t, json.Unmarshal(graphRec.Body.Bytes(), &graphOut))

	// Should have recovered 2 nodes (DataSource + DataSink) and 1 edge.
	assert.Len(t, graphOut.DagNodes, 2)
	assert.Len(t, graphOut.DagEdges, 1)
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

// TestGetEntityRecords_ValidConnection verifies a valid connection + known entity
// returns real, schema-shaped records (not an empty success), and that an unknown
// entity returns EntityNotFoundException.
func TestGetEntityRecords_ValidConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "sfconn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/sf",
				"USERNAME":            "user",
				"PASSWORD":            "pass",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
		"ConnectionName": "sfconn",
		"EntityName":     "Account",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string           `json:"NextToken"`
		Records   []map[string]any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Records)
	// Each record must conform to the Account schema.
	for _, r := range out.Records {
		assert.Contains(t, r, "Id")
		assert.Contains(t, r, "Name")
		assert.Contains(t, r, "AnnualRevenue")
	}

	// An unknown entity for a valid connection is EntityNotFoundException, not empty.
	missRec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
		"ConnectionName": "sfconn",
		"EntityName":     "DoesNotExist",
	})
	assert.Equal(t, http.StatusBadRequest, missRec.Code)
	assert.Contains(t, missRec.Body.String(), "EntityNotFoundException")
}
