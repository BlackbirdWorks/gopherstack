package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestGetMapping_DerivesFromRealSchema verifies GetMapping reads the source (and
// optional sink) table schema from the catalog rather than returning a
// hardcoded empty mapping.
func TestGetMapping_DerivesFromRealSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name:     "missing_source_returns_400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown_source_table_returns_400",
			input: map[string]any{
				"Source": map[string]any{"DatabaseName": "nodb", "TableName": "notable"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "source_only_maps_identity",
			input: map[string]any{
				"Source": map[string]any{"DatabaseName": "db", "TableName": "src"},
			},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name: "source_and_sink_maps_across_tables",
			input: map[string]any{
				"Source": map[string]any{"DatabaseName": "db", "TableName": "src"},
				"Sinks":  []map[string]any{{"DatabaseName": "db", "TableName": "sink"}},
			},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
			doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "db",
				"TableInput": map[string]any{
					"Name": "src",
					"StorageDescriptor": map[string]any{"Columns": []map[string]any{
						{"Name": "id", "Type": "int"},
						{"Name": "name", "Type": "string"},
					}},
				},
			})
			doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "db",
				"TableInput": map[string]any{
					"Name": "sink",
					"StorageDescriptor": map[string]any{"Columns": []map[string]any{
						{"Name": "id", "Type": "bigint"},
					}},
				},
			})

			rec := doGlueRequest(t, h, "GetMapping", tc.input)
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				Mapping []map[string]any `json:"Mapping"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Mapping, tc.wantLen)
			assert.Equal(t, "id", out.Mapping[0]["SourcePath"])
		})
	}
}

// TestGetPlan_Languages verifies GetPlan returns code for both languages.
func TestGetPlan_Languages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		language   string
		wantPython bool
		wantScala  bool
	}{
		{
			name:       "python_language",
			language:   "Python",
			wantPython: true,
			wantScala:  false,
		},
		{
			name:       "scala_language",
			language:   "Scala",
			wantPython: false,
			wantScala:  true,
		},
		{
			name:       "default_to_python",
			language:   "",
			wantPython: true,
			wantScala:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doGlueRequest(t, h, "GetPlan", map[string]any{
				"Language": tc.language,
				"Mapping":  []any{},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				PythonScript string `json:"PythonScript"`
				ScalaCode    string `json:"ScalaCode"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tc.wantPython {
				assert.NotEmpty(t, out.PythonScript)
			} else {
				assert.Empty(t, out.PythonScript)
			}

			if tc.wantScala {
				assert.NotEmpty(t, out.ScalaCode)
			} else {
				assert.Empty(t, out.ScalaCode)
			}
		})
	}
}

// TestGetPlan tests deterministic script generation.
func TestGetPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input          map[string]any
		name           string
		wantSourceName string
		wantPython     bool
		wantScala      bool
	}{
		{
			name:       "no_input_returns_python",
			input:      map[string]any{},
			wantPython: true,
		},
		{
			name: "python_language_explicit",
			input: map[string]any{
				"Language": "Python",
				"Source":   map[string]any{"TableName": "mytable", "DatabaseName": "mydb"},
			},
			wantPython:     true,
			wantSourceName: "mytable",
		},
		{
			name: "scala_language",
			input: map[string]any{
				"Language": "Scala",
				"Source":   map[string]any{"TableName": "scalatable", "DatabaseName": "scaladb"},
			},
			wantScala:      true,
			wantSourceName: "scalatable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetPlan", tc.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				PythonScript string `json:"PythonScript"`
				ScalaCode    string `json:"ScalaCode"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tc.wantPython {
				assert.NotEmpty(t, out.PythonScript)
				assert.Empty(t, out.ScalaCode)
				if tc.wantSourceName != "" {
					assert.Contains(t, out.PythonScript, tc.wantSourceName)
				}
			}

			if tc.wantScala {
				assert.NotEmpty(t, out.ScalaCode)
				assert.Empty(t, out.PythonScript)
				if tc.wantSourceName != "" {
					assert.Contains(t, out.ScalaCode, tc.wantSourceName)
				}
			}
		})
	}
}
