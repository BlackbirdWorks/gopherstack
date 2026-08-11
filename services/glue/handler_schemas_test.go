package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
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

func TestSchemaRegistry_Registry_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		check       func(t *testing.T, body string)
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name:        "create-registry",
			op:          "CreateRegistry",
			body:        map[string]any{"RegistryName": "my-reg", "Description": "test registry"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "my-reg")
			},
		},
		{
			name:     "create-registry-duplicate",
			op:       "CreateRegistry",
			body:     map[string]any{"RegistryName": "my-reg"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-registry",
			op:       "GetRegistry",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "my-reg"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-registry-missing",
			op:       "GetRegistry",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "no-reg"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list-registries",
			op:       "ListRegistries",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "update-registry",
			op:   "UpdateRegistry",
			body: map[string]any{
				"RegistryId":  map[string]any{"RegistryName": "my-reg"},
				"Description": "updated description",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-registry",
			op:       "DeleteRegistry",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "my-reg"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "my-reg"})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestSchemaRegistry_Schema_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name: "create-schema",
			op:   "CreateSchema",
			body: map[string]any{
				"RegistryId":    map[string]any{"RegistryName": "reg1"},
				"SchemaName":    "schema1",
				"DataFormat":    "AVRO",
				"Compatibility": "BACKWARD",
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name: "create-schema-duplicate",
			op:   "CreateSchema",
			body: map[string]any{
				"RegistryId": map[string]any{"RegistryName": "reg1"},
				"SchemaName": "schema1",
				"DataFormat": "AVRO",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-schema",
			op:       "GetSchema",
			body:     map[string]any{"SchemaId": map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-schema-missing",
			op:       "GetSchema",
			body:     map[string]any{"SchemaId": map[string]any{"RegistryName": "reg1", "SchemaName": "no-schema"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list-schemas",
			op:       "ListSchemas",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "reg1"}},
			wantCode: http.StatusOK,
		},
		{
			name: "update-schema",
			op:   "UpdateSchema",
			body: map[string]any{
				"SchemaId":      map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
				"Compatibility": "FORWARD",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-schema",
			op:       "DeleteSchema",
			body:     map[string]any{"SchemaId": map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "reg1"})
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateSchema", map[string]any{
					"RegistryId": map[string]any{"RegistryName": "reg1"},
					"SchemaName": "schema1",
					"DataFormat": "AVRO",
				})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSchemaRegistry_SchemaVersion_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "svr"})
	doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId": map[string]any{"RegistryName": "svr"},
		"SchemaName": "sv-schema",
		"DataFormat": "AVRO",
	})

	avroDefinition := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`

	registerRec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId":         map[string]any{"RegistryName": "svr", "SchemaName": "sv-schema"},
		"SchemaDefinition": avroDefinition,
	})
	require.Equal(t, http.StatusOK, registerRec.Code)
	var registerOut map[string]any
	require.NoError(t, json.Unmarshal(registerRec.Body.Bytes(), &registerOut))
	assert.NotEmpty(t, registerOut["SchemaVersionId"])

	getSchemaVersionRec := doGlueRequest(t, h, "GetSchemaVersion", map[string]any{
		"SchemaId":            map[string]any{"RegistryName": "svr", "SchemaName": "sv-schema"},
		"SchemaVersionNumber": map[string]any{"VersionNumber": 1},
	})
	require.Equal(t, http.StatusOK, getSchemaVersionRec.Code)
	var getSchemaVersionOut map[string]any
	require.NoError(t, json.Unmarshal(getSchemaVersionRec.Body.Bytes(), &getSchemaVersionOut))
	assert.EqualValues(t, 1, getSchemaVersionOut["VersionNumber"])

	listSchemaVersionsRec := doGlueRequest(t, h, "ListSchemaVersions", map[string]any{
		"SchemaId": map[string]any{"RegistryName": "svr", "SchemaName": "sv-schema"},
	})
	require.Equal(t, http.StatusOK, listSchemaVersionsRec.Code)
	assert.Contains(t, listSchemaVersionsRec.Body.String(), "SchemaVersions")

	checkValidityRec := doGlueRequest(t, h, "CheckSchemaVersionValidity", map[string]any{
		"DataFormat":       "AVRO",
		"SchemaDefinition": avroDefinition,
	})
	require.Equal(t, http.StatusOK, checkValidityRec.Code)
	var checkValidityOut map[string]any
	require.NoError(t, json.Unmarshal(checkValidityRec.Body.Bytes(), &checkValidityOut))
	assert.True(t, checkValidityOut["Valid"].(bool))
}

// TestCreateSchema_WithDefinition_PopulatesVersionFields verifies the wire
// response for gopherstack-i60f: once CreateSchema can carry the first
// schema definition, CreateSchemaOutput's version fields (SchemaVersionId,
// SchemaVersionStatus, LatestSchemaVersion, NextSchemaVersion) must reflect
// it -- aws-sdk-go-v2/service/glue@v1.152.0 api_op_CreateSchema.go:129-132
// (LatestSchemaVersion/NextSchemaVersion), 154-157 (SchemaVersionId/Status).
func TestCreateSchema_WithDefinition_PopulatesVersionFields(t *testing.T) {
	t.Parallel()

	const def = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`

	tests := []struct {
		name          string
		definition    string
		wantVersionID bool
		wantLatest    float64
		wantNext      float64
	}{
		{name: "with_definition", definition: def, wantVersionID: true, wantLatest: 1, wantNext: 2},
		{name: "without_definition", definition: "", wantVersionID: false, wantLatest: 0, wantNext: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRegistry(t, h, "reg")

			body := map[string]any{
				"RegistryId": map[string]any{"RegistryName": "reg"},
				"SchemaName": "sch",
				"DataFormat": "AVRO",
			}
			if tt.definition != "" {
				body["SchemaDefinition"] = tt.definition
			}

			rec := doGlueRequest(t, h, "CreateSchema", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.InDelta(t, tt.wantLatest, out["LatestSchemaVersion"], 0)
			assert.InDelta(t, tt.wantNext, out["NextSchemaVersion"], 0)

			if !tt.wantVersionID {
				assert.NotContains(t, out, "SchemaVersionId")
				assert.NotContains(t, out, "SchemaVersionStatus")

				return
			}

			assert.NotEmpty(t, out["SchemaVersionId"])
			assert.Equal(t, "AVAILABLE", out["SchemaVersionStatus"])
		})
	}
}

// TestCreateSchema_RejectsInvalidDefinition verifies a malformed
// SchemaDefinition fails CreateSchema atomically -- no half-created schema
// left behind, per gopherstack-i60f.
func TestCreateSchema_RejectsInvalidDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRegistry(t, h, "reg")

	rec := doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId":       map[string]any{"RegistryName": "reg"},
		"SchemaName":       "sch",
		"DataFormat":       "AVRO",
		"SchemaDefinition": "not json",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	getRec := doGlueRequest(t, h, "GetSchema", map[string]any{
		"SchemaId": map[string]any{"RegistryName": "reg", "SchemaName": "sch"},
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code, "a rejected definition must not leave a half-created schema")
}

// setupTestSchema creates a registry, schema, and registers two versions.
// Returns the schema version IDs of the two registered versions.
func setupTestSchema(t *testing.T, h *glue.Handler) string {
	t.Helper()

	regRec := doGlueRequest(t, h, "CreateRegistry", map[string]any{
		"RegistryName": "reg1",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	schemaRec := doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId":    map[string]any{"RegistryName": "reg1"},
		"SchemaName":    "schema1",
		"DataFormat":    "AVRO",
		"Compatibility": "NONE",
	})
	require.Equal(t, http.StatusOK, schemaRec.Code)

	const schemaV2Def = `{"type":"record","name":"A","fields":[` +
		`{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	reg1Rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId":         map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
		"SchemaDefinition": `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`,
	})
	require.Equal(t, http.StatusOK, reg1Rec.Code)

	var sv1Out struct {
		SchemaVersionID string `json:"SchemaVersionId"`
	}
	require.NoError(t, json.Unmarshal(reg1Rec.Body.Bytes(), &sv1Out))

	reg2Rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId":         map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
		"SchemaDefinition": schemaV2Def,
	})
	require.Equal(t, http.StatusOK, reg2Rec.Code)

	var sv2Out struct {
		SchemaVersionID string `json:"SchemaVersionId"`
	}
	require.NoError(t, json.Unmarshal(reg2Rec.Body.Bytes(), &sv2Out))

	return sv1Out.SchemaVersionID
}

// TestSchemaVersionMetadata_Lifecycle tests put/query/remove round-trip.
func TestSchemaVersionMetadata_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pairs     map[string]string
		wantAfter map[string]string
		name      string
		removeKey string
	}{
		{
			name:      "single_key_put_and_query",
			pairs:     map[string]string{"owner": "team-a"},
			wantAfter: map[string]string{"owner": "team-a"},
		},
		{
			name:      "multiple_keys",
			pairs:     map[string]string{"owner": "team-a", "env": "prod"},
			wantAfter: map[string]string{"owner": "team-a", "env": "prod"},
		},
		{
			name:      "put_then_remove",
			pairs:     map[string]string{"owner": "team-a", "env": "prod"},
			removeKey: "env",
			wantAfter: map[string]string{"owner": "team-a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			v1ID := setupTestSchema(t, h)

			for k, v := range tc.pairs {
				putRec := doGlueRequest(t, h, "PutSchemaVersionMetadata", map[string]any{
					"SchemaVersionId":  v1ID,
					"MetadataKeyValue": map[string]any{"MetadataKey": k, "MetadataValue": v},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			if tc.removeKey != "" {
				removeRec := doGlueRequest(t, h, "RemoveSchemaVersionMetadata", map[string]any{
					"SchemaVersionId":  v1ID,
					"MetadataKeyValue": map[string]any{"MetadataKey": tc.removeKey},
				})
				require.Equal(t, http.StatusOK, removeRec.Code)
			}

			queryRec := doGlueRequest(t, h, "QuerySchemaVersionMetadata", map[string]any{
				"SchemaVersionId": v1ID,
			})
			require.Equal(t, http.StatusOK, queryRec.Code)

			var out struct {
				MetadataInfo map[string]struct {
					MetadataValue string `json:"MetadataValue"`
				} `json:"MetadataInfo"`
				SchemaVersionID string `json:"SchemaVersionId"`
			}
			require.NoError(t, json.Unmarshal(queryRec.Body.Bytes(), &out))
			assert.Equal(t, v1ID, out.SchemaVersionID)

			got := make(map[string]string, len(out.MetadataInfo))
			for k, v := range out.MetadataInfo {
				got[k] = v.MetadataValue
			}

			assert.Equal(t, tc.wantAfter, got)
		})
	}
}

// TestSchemaVersionMetadata_EmptyQuery verifies empty map returned for unknown ID.
func TestSchemaVersionMetadata_EmptyQuery(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "QuerySchemaVersionMetadata", map[string]any{
		"SchemaVersionId": "no-such-version",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		MetadataInfo map[string]any `json:"MetadataInfo"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.MetadataInfo)
}

// TestGetSchemaByDefinition_Stateful verifies lookup by definition content.
func TestGetSchemaByDefinition_Stateful(t *testing.T) {
	t.Parallel()

	const def1 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`
	const def2 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	tests := []struct {
		name       string
		definition string
		wantCode   int
		wantFound  bool
	}{
		{
			name:       "found_v1_by_definition",
			definition: def1,
			wantCode:   http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "found_v2_by_definition",
			definition: def2,
			wantCode:   http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "unknown_definition_returns_400",
			definition: `{"type":"record","name":"X"}`,
			wantCode:   http.StatusBadRequest,
			wantFound:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupTestSchema(t, h)

			rec := doGlueRequest(t, h, "GetSchemaByDefinition", map[string]any{
				"SchemaId":         map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
				"SchemaDefinition": tc.definition,
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantFound {
				var out struct {
					SchemaVersionID string `json:"SchemaVersionId"`
					Status          string `json:"Status"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.SchemaVersionID)
				assert.Equal(t, "AVAILABLE", out.Status)
			}
		})
	}
}

// TestGetSchemaVersionsDiff_Stateful verifies diff between schema versions.
func TestGetSchemaVersionsDiff_Stateful(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		v1       int64
		v2       int64
		wantDiff bool
	}{
		{
			name:     "same_version_returns_empty_diff",
			v1:       1,
			v2:       1,
			wantDiff: false,
		},
		{
			name:     "different_versions_return_diff",
			v1:       1,
			v2:       2,
			wantDiff: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupTestSchema(t, h)

			rec := doGlueRequest(t, h, "GetSchemaVersionsDiff", map[string]any{
				"SchemaId":                  map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
				"FirstSchemaVersionNumber":  map[string]any{"VersionNumber": tc.v1},
				"SecondSchemaVersionNumber": map[string]any{"VersionNumber": tc.v2},
				"SchemaDiffType":            "SYNTAX_DIFF",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Diff string `json:"Diff"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tc.wantDiff {
				assert.NotEmpty(t, out.Diff)
			} else {
				assert.Empty(t, out.Diff)
			}
		})
	}
}

// TestGetSchemaVersionsDiff_UnknownSchema verifies 400 for missing schema.
func TestGetSchemaVersionsDiff_UnknownSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetSchemaVersionsDiff", map[string]any{
		"SchemaId":                  map[string]any{"RegistryName": "no-reg", "SchemaName": "no-schema"},
		"FirstSchemaVersionNumber":  map[string]any{"VersionNumber": int64(1)},
		"SecondSchemaVersionNumber": map[string]any{"VersionNumber": int64(2)},
		"SchemaDiffType":            "SYNTAX_DIFF",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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
