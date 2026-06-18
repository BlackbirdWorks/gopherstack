package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestCatalogImportStatus tests GetCatalogImportStatus and ImportCatalogToGlue.
func TestCatalogImportStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		catalogID     string
		importFirst   bool
		wantCompleted bool
	}{
		{
			name:          "default_not_imported",
			importFirst:   false,
			catalogID:     "account-level",
			wantCompleted: false,
		},
		{
			name:          "after_import_completed",
			importFirst:   true,
			catalogID:     "account-level",
			wantCompleted: true,
		},
		{
			name:          "empty_catalog_id_defaults",
			importFirst:   false,
			catalogID:     "",
			wantCompleted: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.importFirst {
				importRec := doGlueRequest(t, h, "ImportCatalogToGlue", map[string]any{
					"CatalogId": tc.catalogID,
				})
				require.Equal(t, http.StatusOK, importRec.Code)
			}

			input := map[string]any{}
			if tc.catalogID != "" {
				input["CatalogId"] = tc.catalogID
			}

			rec := doGlueRequest(t, h, "GetCatalogImportStatus", input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				ImportStatus struct {
					ImportedBy      string  `json:"ImportedBy"`
					ImportTime      float64 `json:"ImportTime"`
					ImportCompleted bool    `json:"ImportCompleted"`
				} `json:"ImportStatus"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantCompleted, out.ImportStatus.ImportCompleted)

			if tc.wantCompleted {
				assert.NotZero(t, out.ImportStatus.ImportTime)
				assert.Equal(t, "Glue", out.ImportStatus.ImportedBy)
			}
		})
	}
}

// TestGetSchemaVersionsDiff tests the diff between schema versions.
func TestGetSchemaVersionsDiff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *glue.Handler) (registryName, schemaName string)
		name     string
		v1       int64
		v2       int64
		wantCode int
		wantDiff bool
		nilID    bool
	}{
		{
			name: "nil_schema_id_returns_empty_diff",
			setup: func(_ *testing.T, _ *glue.Handler) (string, string) {
				return "", ""
			},
			wantCode: http.StatusOK,
			wantDiff: false,
			nilID:    true,
		},
		{
			name: "same_version_returns_empty_diff",
			setup: func(t *testing.T, h *glue.Handler) (string, string) {
				t.Helper()
				createRegistry(t, h, "reg1")
				createSchema(t, h, "reg1", "schema1")
				registerSchemaVersion(t, h, "reg1", "schema1", `{"type":"record","name":"Test"}`)

				return "reg1", "schema1"
			},
			v1:       1,
			v2:       1,
			wantCode: http.StatusOK,
			wantDiff: false,
		},
		{
			name: "different_versions_returns_diff",
			setup: func(t *testing.T, h *glue.Handler) (string, string) {
				t.Helper()
				createRegistry(t, h, "reg2")
				createSchema(t, h, "reg2", "schema2")
				registerSchemaVersion(t, h, "reg2", "schema2", `{"type":"record","name":"V1"}`)
				registerSchemaVersion(t, h, "reg2", "schema2", `{"type":"record","name":"V2"}`)

				return "reg2", "schema2"
			},
			v1:       1,
			v2:       2,
			wantCode: http.StatusOK,
			wantDiff: true,
		},
		{
			name: "unknown_schema_returns_400",
			setup: func(_ *testing.T, _ *glue.Handler) (string, string) {
				return "no-reg", "no-schema"
			},
			v1:       1,
			v2:       2,
			wantCode: http.StatusBadRequest,
			wantDiff: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			registryName, schemaName := tc.setup(t, h)

			var input map[string]any
			if tc.nilID {
				input = map[string]any{}
			} else {
				input = map[string]any{
					"SchemaId": map[string]any{
						"RegistryName": registryName,
						"SchemaName":   schemaName,
					},
					"FirstSchemaVersionNumber":  tc.v1,
					"SecondSchemaVersionNumber": tc.v2,
					"SchemaDiffType":            "SYNTAX_DIFF",
				}
			}

			rec := doGlueRequest(t, h, "GetSchemaVersionsDiff", input)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode == http.StatusOK {
				var out struct {
					Diff string `json:"Diff"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				if tc.wantDiff {
					assert.NotEmpty(t, out.Diff)
				} else {
					assert.Empty(t, out.Diff)
				}
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

// TestGetUsageProfile_FullFields tests that GetUsageProfile returns all profile fields.
func TestGetUsageProfile_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateUsageProfile", map[string]any{
		"Name":        "full-profile",
		"Description": "test description",
		"Tags":        map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	getRec := doGlueRequest(t, h, "GetUsageProfile", map[string]any{
		"Name": "full-profile",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Tags           map[string]string `json:"Tags"`
		Name           string            `json:"Name"`
		Description    string            `json:"Description"`
		CreatedOn      float64           `json:"CreatedOn"`
		LastModifiedOn float64           `json:"LastModifiedOn"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.Equal(t, "full-profile", out.Name)
	assert.Equal(t, "test description", out.Description)
	assert.NotZero(t, out.CreatedOn)
	assert.NotZero(t, out.LastModifiedOn)
}

// createRegistry is a test helper that creates a schema registry.
func createRegistry(t *testing.T, h *glue.Handler, name string) {
	t.Helper()
	rec := doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

// createSchema is a test helper that creates a schema within a registry.
func createSchema(t *testing.T, h *glue.Handler, registryName, schemaName string) {
	t.Helper()
	rec := doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId":    map[string]any{"RegistryName": registryName},
		"SchemaName":    schemaName,
		"DataFormat":    "AVRO",
		"Compatibility": "NONE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// registerSchemaVersion is a test helper that registers a schema version.
func registerSchemaVersion(t *testing.T, h *glue.Handler, registryName, schemaName, definition string) {
	t.Helper()
	rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId": map[string]any{
			"RegistryName": registryName,
			"SchemaName":   schemaName,
		},
		"SchemaDefinition": definition,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
