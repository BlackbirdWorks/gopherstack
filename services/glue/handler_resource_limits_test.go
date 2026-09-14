package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// testResourceCap is the lowered cap used by every subtest below, via
// WithResourceLimits, so each case only has to create testResourceCap+1
// resources instead of the real 40-to-10,000,000-sized AWS default.
const testResourceCap = 3

// assertResourceLimitExceeded posts one more request past the cap and
// confirms it fails with the real wire exception.
func assertResourceLimitExceeded(t *testing.T, h *glue.Handler, action string, body map[string]any) {
	t.Helper()

	rec := doGlueRequest(t, h, action, body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "ResourceNumberLimitExceededException", out["__type"])
}

// TestResourceNumberLimitExceeded covers gopherstack-qd3.5's
// ResourceNumberLimitExceededException gap for the resource kinds whose real
// Create op declares the exception (aws-sdk-go-v2/service/glue@v1.157.0/
// deserializers.go) and for which AWS publishes a distinct per-resource-kind
// account quota (docs.aws.amazon.com/general/latest/gr/glue.html) -- see
// services/glue/limits.go for the full op list and cited values. Each case
// lowers its cap to testResourceCap via WithResourceLimits so the test
// doesn't have to create the real (up to 10,000,000) resources.
func TestResourceNumberLimitExceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(i int) (action string, body map[string]any)
		name   string
		limits glue.ResourceLimits
	}{
		{
			name:   "connections",
			limits: glue.ResourceLimits{Connections: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{
						"Name":           fmt.Sprintf("conn-%d", i),
						"ConnectionType": "JDBC",
						"ConnectionProperties": map[string]string{
							"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
							"USERNAME":            "root",
							"PASSWORD":            "secret",
						},
					},
				}
			},
		},
		{
			name:   "crawlers",
			limits: glue.ResourceLimits{Crawlers: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateCrawler", map[string]any{
					"Name": fmt.Sprintf("crawler-%d", i),
					"Role": "r",
					"Targets": map[string]any{
						"S3Targets": []map[string]any{{"Path": "s3://b/p"}},
					},
				}
			},
		},
		{
			name:   "databases",
			limits: glue.ResourceLimits{Databases: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateDatabase", map[string]any{
					"DatabaseInput": map[string]any{"Name": fmt.Sprintf("db-%d", i)},
				}
			},
		},
		{
			name:   "jobs",
			limits: glue.ResourceLimits{Jobs: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateJob", map[string]any{
					"Name":    fmt.Sprintf("job-%d", i),
					"Role":    "r",
					"Command": map[string]any{"Name": "glueetl"},
				}
			},
		},
		{
			name:   "ml_transforms",
			limits: glue.ResourceLimits{MLTransforms: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateMLTransform", map[string]any{
					"Name": fmt.Sprintf("ml-%d", i),
					"Role": "arn:aws:iam::123456789012:role/GlueRole",
					"InputRecordTables": []map[string]any{
						{"DatabaseName": "db1", "TableName": "t1"},
					},
					"Parameters": map[string]any{
						"TransformType": "FIND_MATCHES",
						"FindMatchesParameters": map[string]any{
							"PrimaryKeyColumnName": "id",
						},
					},
				}
			},
		},
		{
			name:   "security_configurations",
			limits: glue.ResourceLimits{SecurityConfigs: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateSecurityConfiguration", map[string]any{
					"Name": fmt.Sprintf("sc-%d", i),
				}
			},
		},
		{
			name:   "triggers",
			limits: glue.ResourceLimits{Triggers: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateTrigger", map[string]any{
					"Name": fmt.Sprintf("trig-%d", i),
					"Type": "ON_DEMAND",
					"Actions": []map[string]any{
						{"JobName": "some-job"},
					},
				}
			},
		},
		{
			name:   "workflows",
			limits: glue.ResourceLimits{Workflows: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateWorkflow", map[string]any{"Name": fmt.Sprintf("wf-%d", i)}
			},
		},
		{
			name:   "schema_registries",
			limits: glue.ResourceLimits{SchemaRegistries: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateRegistry", map[string]any{"RegistryName": fmt.Sprintf("reg-%d", i)}
			},
		},
		{
			name:   "integrations",
			limits: glue.ResourceLimits{Integrations: testResourceCap},
			create: func(i int) (string, map[string]any) {
				return "CreateIntegration", map[string]any{
					"IntegrationName": fmt.Sprintf("integ-%d", i),
					"SourceArn":       fmt.Sprintf("arn:aws:s3:::integ-src-%d", i),
					"TargetArn": fmt.Sprintf(
						"arn:aws:redshift:us-east-1:123456789012:cluster/integ-tgt-%d", i,
					),
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion).WithResourceLimits(tc.limits)
			h := glue.NewHandler(backend)

			for i := range testResourceCap {
				action, body := tc.create(i)
				rec := doGlueRequest(t, h, action, body)
				require.Equal(t, http.StatusOK, rec.Code, "item %d should succeed under the limit", i)
			}

			action, body := tc.create(testResourceCap)
			assertResourceLimitExceeded(t, h, action, body)
		})
	}
}

// TestResourceNumberLimitExceeded_TablesPerDatabase covers CreateTable's
// per-database cap (AWS's published "Max tables per database": 200,000),
// which needs a database set up first, so it doesn't fit the flat table
// above.
func TestResourceNumberLimitExceeded_TablesPerDatabase(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion).
		WithResourceLimits(glue.ResourceLimits{TablesPerDatabase: testResourceCap})
	h := glue.NewHandler(backend)

	dbRec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "tbl-limit-db"},
	})
	require.Equal(t, http.StatusOK, dbRec.Code)

	for i := range testResourceCap {
		rec := doGlueRequest(t, h, "CreateTable", map[string]any{
			"DatabaseName": "tbl-limit-db",
			"TableInput":   map[string]any{"Name": fmt.Sprintf("tbl-%d", i)},
		})
		require.Equal(t, http.StatusOK, rec.Code, "table %d should succeed under the limit", i)
	}

	assertResourceLimitExceeded(t, h, "CreateTable", map[string]any{
		"DatabaseName": "tbl-limit-db",
		"TableInput":   map[string]any{"Name": "tbl-over-limit"},
	})
}

// TestResourceNumberLimitExceeded_FunctionsPerDatabase covers
// CreateUserDefinedFunction's per-database cap (AWS's published "Max
// functions per database": 100).
func TestResourceNumberLimitExceeded_FunctionsPerDatabase(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion).
		WithResourceLimits(glue.ResourceLimits{FunctionsPerDatabase: testResourceCap})
	h := glue.NewHandler(backend)

	dbRec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "udf-limit-db"},
	})
	require.Equal(t, http.StatusOK, dbRec.Code)

	for i := range testResourceCap {
		rec := doGlueRequest(t, h, "CreateUserDefinedFunction", map[string]any{
			"DatabaseName": "udf-limit-db",
			"FunctionInput": map[string]any{
				"FunctionName": fmt.Sprintf("func-%d", i),
				"ClassName":    "com.example.Func",
				"OwnerName":    "alice",
				"OwnerType":    "USER",
			},
		})
		require.Equal(t, http.StatusOK, rec.Code, "function %d should succeed under the limit", i)
	}

	assertResourceLimitExceeded(t, h, "CreateUserDefinedFunction", map[string]any{
		"DatabaseName": "udf-limit-db",
		"FunctionInput": map[string]any{
			"FunctionName": "func-over-limit",
			"ClassName":    "com.example.Func",
			"OwnerName":    "alice",
			"OwnerType":    "USER",
		},
	})
}

// TestResourceNumberLimitExceeded_SchemaVersions covers RegisterSchemaVersion's
// account-wide cap (AWS's published, non-adjustable "Number of Schema
// Versions": 10,000).
func TestResourceNumberLimitExceeded_SchemaVersions(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion).
		WithResourceLimits(glue.ResourceLimits{SchemaVersions: testResourceCap})
	h := glue.NewHandler(backend)

	regRec := doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "sv-limit-reg"})
	require.Equal(t, http.StatusOK, regRec.Code)

	schemaRec := doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId": map[string]any{"RegistryName": "sv-limit-reg"},
		"SchemaName": "sv-limit-schema",
		"DataFormat": "AVRO",
	})
	require.Equal(t, http.StatusOK, schemaRec.Code)

	definition := `{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}`

	for i := range testResourceCap {
		rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
			"SchemaId":         map[string]any{"RegistryName": "sv-limit-reg", "SchemaName": "sv-limit-schema"},
			"SchemaDefinition": definition,
		})
		require.Equal(t, http.StatusOK, rec.Code, "version %d should succeed under the limit", i)
	}

	assertResourceLimitExceeded(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId":         map[string]any{"RegistryName": "sv-limit-reg", "SchemaName": "sv-limit-schema"},
		"SchemaDefinition": definition,
	})
}

// TestResourceNumberLimitExceeded_PartitionsPerTable covers
// BatchCreatePartition's per-table cap (AWS's published "Max partitions per
// table": 10,000,000). Unlike the single-resource Create ops above,
// BatchCreatePartition reports over-limit entries per-item in its Errors
// list rather than failing the whole call.
func TestResourceNumberLimitExceeded_PartitionsPerTable(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion).
		WithResourceLimits(glue.ResourceLimits{PartitionsPerTable: testResourceCap})
	h := glue.NewHandler(backend)

	createTestDB(t, h, "part-limit-db", "part-limit-tbl")

	parts := make([]map[string]any, testResourceCap+1)
	for i := range parts {
		parts[i] = map[string]any{"Values": []string{fmt.Sprintf("v%d", i)}}
	}

	rec := doGlueRequest(t, h, "BatchCreatePartition", map[string]any{
		"DatabaseName":       "part-limit-db",
		"TableName":          "part-limit-tbl",
		"PartitionInputList": parts,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Errors []struct {
			ErrorDetail struct {
				ErrorCode    string `json:"ErrorCode"`
				ErrorMessage string `json:"ErrorMessage"`
			} `json:"ErrorDetail"`
		} `json:"Errors"`
		Partitions []map[string]any `json:"Partitions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Len(t, out.Partitions, testResourceCap)
	require.Len(t, out.Errors, 1)
	assert.Equal(t, "ResourceNumberLimitExceededException", out.Errors[0].ErrorDetail.ErrorCode)
}
