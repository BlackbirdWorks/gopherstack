package glue_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// doGlueOp is a helper to POST a JSON body to the Glue handler via echo.
func doGlueOp(t *testing.T, h *glue.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AWSGlue."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)
	if hErr := h.Handler()(c); hErr != nil {
		t.Fatalf("handler error: %v", hErr)
	}

	return rr
}

// dispatchNewOp sends an operation and expects HTTP 200.
func dispatchNewOp(t *testing.T, h *glue.Handler, op string, body any) map[string]any {
	t.Helper()
	rr := doGlueOp(t, h, op, body)
	if rr.Code != http.StatusOK {
		t.Fatalf("op %s: got status %d, body: %s", op, rr.Code, rr.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return out
}

// dispatchNewOpExpectError sends an operation and expects a non-200 status.
func dispatchNewOpExpectError(t *testing.T, h *glue.Handler, op string, body any) {
	t.Helper()
	rr := doGlueOp(t, h, op, body)
	if rr.Code == http.StatusOK {
		t.Fatalf("op %s: expected error but got 200, body: %s", op, rr.Body.String())
	}
}

func newGlueHandler(t *testing.T) *glue.Handler {
	t.Helper()
	b := glue.NewInMemoryBackend("123456789012", "us-east-1")

	return glue.NewHandler(b)
}

// TestNewOps_UserDefinedFunction tests UDF CRUD.
func TestNewOps_UserDefinedFunction(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// Setup: create a database first.
	dispatchNewOp(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "mydb"},
	})

	// CreateUserDefinedFunction
	dispatchNewOp(t, h, "CreateUserDefinedFunction", map[string]any{
		"DatabaseName": "mydb",
		"FunctionInput": map[string]any{
			"FunctionName": "my_func",
			"ClassName":    "com.example.MyFunc",
			"OwnerName":    "alice",
			"OwnerType":    "USER",
		},
	})

	// GetUserDefinedFunction
	out := dispatchNewOp(t, h, "GetUserDefinedFunction", map[string]any{
		"DatabaseName": "mydb",
		"FunctionName": "my_func",
	})
	udf, ok := out["UserDefinedFunction"].(map[string]any)
	if !ok {
		t.Fatalf("expected UserDefinedFunction in response, got: %v", out)
	}
	if udf["ClassName"] != "com.example.MyFunc" {
		t.Errorf("ClassName mismatch: %v", udf["ClassName"])
	}

	// GetUserDefinedFunctions
	out2 := dispatchNewOp(t, h, "GetUserDefinedFunctions", map[string]any{"DatabaseName": "mydb"})
	udfs, _ := out2["UserDefinedFunctions"].([]any)
	if len(udfs) != 1 {
		t.Errorf("expected 1 UDF, got %d", len(udfs))
	}

	// UpdateUserDefinedFunction
	dispatchNewOp(t, h, "UpdateUserDefinedFunction", map[string]any{
		"DatabaseName": "mydb",
		"FunctionName": "my_func",
		"FunctionInput": map[string]any{
			"FunctionName": "my_func",
			"ClassName":    "com.example.UpdatedFunc",
		},
	})

	// Verify update
	out3 := dispatchNewOp(t, h, "GetUserDefinedFunction", map[string]any{
		"DatabaseName": "mydb",
		"FunctionName": "my_func",
	})
	udf2 := out3["UserDefinedFunction"].(map[string]any)
	if udf2["ClassName"] != "com.example.UpdatedFunc" {
		t.Errorf("updated ClassName mismatch: %v", udf2["ClassName"])
	}

	// DeleteUserDefinedFunction
	dispatchNewOp(t, h, "DeleteUserDefinedFunction", map[string]any{
		"DatabaseName": "mydb",
		"FunctionName": "my_func",
	})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetUserDefinedFunction", map[string]any{
		"DatabaseName": "mydb",
		"FunctionName": "my_func",
	})
}

// TestNewOps_SecurityConfiguration tests SecurityConfiguration CRUD.
func TestNewOps_SecurityConfiguration(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateSecurityConfiguration
	out := dispatchNewOp(t, h, "CreateSecurityConfiguration", map[string]any{
		"Name": "my-sec-config",
		"EncryptionConfiguration": map[string]any{
			"S3Encryption": []any{
				map[string]any{"S3EncryptionMode": "SSE-S3"},
			},
		},
	})
	if out["Name"] != "my-sec-config" {
		t.Errorf("Name mismatch: %v", out["Name"])
	}

	// GetSecurityConfiguration
	out2 := dispatchNewOp(t, h, "GetSecurityConfiguration", map[string]any{"Name": "my-sec-config"})
	sc := out2["SecurityConfiguration"].(map[string]any)
	if sc["Name"] != "my-sec-config" {
		t.Errorf("SecurityConfiguration Name mismatch: %v", sc)
	}

	// GetSecurityConfigurations
	out3 := dispatchNewOp(t, h, "GetSecurityConfigurations", map[string]any{})
	configs, _ := out3["SecurityConfigurations"].([]any)
	if len(configs) != 1 {
		t.Errorf("expected 1 security config, got %d", len(configs))
	}

	// DeleteSecurityConfiguration
	dispatchNewOp(t, h, "DeleteSecurityConfiguration", map[string]any{"Name": "my-sec-config"})

	// Verify deletion
	dispatchNewOpExpectError(
		t,
		h,
		"GetSecurityConfiguration",
		map[string]any{"Name": "my-sec-config"},
	)
}

// TestNewOps_Session tests Session + Statement CRUD.
func TestNewOps_Session(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateSession
	out := dispatchNewOp(t, h, "CreateSession", map[string]any{
		"Id":   "sess-1",
		"Role": "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]any{
			"Name":          "glueetl",
			"PythonVersion": "3",
		},
	})
	sess := out["Session"].(map[string]any)
	if sess["Id"] != "sess-1" {
		t.Errorf("session ID mismatch: %v", sess["Id"])
	}
	if sess["Status"] != "PROVISIONING" {
		t.Errorf("status mismatch: %v", sess["Status"])
	}

	// GetSession
	out2 := dispatchNewOp(t, h, "GetSession", map[string]any{"Id": "sess-1"})
	sess2 := out2["Session"].(map[string]any)
	if sess2["Id"] != "sess-1" {
		t.Errorf("GetSession ID mismatch: %v", sess2)
	}

	// RunStatement
	out3 := dispatchNewOp(t, h, "RunStatement", map[string]any{
		"SessionId": "sess-1",
		"Code":      "print('hello')",
	})
	if out3["Id"] == nil {
		t.Errorf("expected statement ID in response")
	}
	stmtID := int32(out3["Id"].(float64))

	// GetStatement
	out4 := dispatchNewOp(t, h, "GetStatement", map[string]any{
		"SessionId": "sess-1",
		"Id":        stmtID,
	})
	stmt := out4["Statement"].(map[string]any)
	if stmt["Code"] != "print('hello')" {
		t.Errorf("statement code mismatch: %v", stmt["Code"])
	}

	// ListStatements
	out5 := dispatchNewOp(t, h, "ListStatements", map[string]any{"SessionId": "sess-1"})
	stmts, _ := out5["Statements"].([]any)
	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}

	// CancelStatement
	dispatchNewOp(t, h, "CancelStatement", map[string]any{
		"SessionId": "sess-1",
		"Id":        stmtID,
	})

	// ListSessions
	out6 := dispatchNewOp(t, h, "ListSessions", map[string]any{})
	sessions, _ := out6["Sessions"].([]any)
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}

	// StopSession
	dispatchNewOp(t, h, "StopSession", map[string]any{"Id": "sess-1"})

	// DeleteSession
	dispatchNewOp(t, h, "DeleteSession", map[string]any{"Id": "sess-1"})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetSession", map[string]any{"Id": "sess-1"})
}

// TestNewOps_TableOptimizer tests TableOptimizer CRUD.
func TestNewOps_TableOptimizer(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateTableOptimizer
	dispatchNewOp(t, h, "CreateTableOptimizer", map[string]any{
		"CatalogId":    "123456789012",
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
		"TableOptimizerConfiguration": map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/GlueRole",
			"Enabled": true,
		},
	})

	// GetTableOptimizer
	out := dispatchNewOp(t, h, "GetTableOptimizer", map[string]any{
		"CatalogId":    "123456789012",
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
	})
	to := out["TableOptimizer"].(map[string]any)
	if to["Type"] != "compaction" {
		t.Errorf("Type mismatch: %v", to["Type"])
	}
	config := to["Configuration"].(map[string]any)
	if config["Enabled"] != true {
		t.Errorf("Enabled mismatch: %v", config["Enabled"])
	}

	// UpdateTableOptimizer
	dispatchNewOp(t, h, "UpdateTableOptimizer", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
		"TableOptimizerConfiguration": map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/GlueRole",
			"Enabled": false,
		},
	})

	// BatchGetTableOptimizer
	out2 := dispatchNewOp(t, h, "BatchGetTableOptimizer", map[string]any{
		"Entries": []any{
			map[string]any{
				"DatabaseName": "mydb",
				"TableName":    "mytable",
				"Type":         "compaction",
			},
		},
	})
	opts, _ := out2["TableOptimizers"].([]any)
	if len(opts) != 1 {
		t.Errorf("expected 1 optimizer, got %d", len(opts))
	}

	// DeleteTableOptimizer
	dispatchNewOp(t, h, "DeleteTableOptimizer", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
	})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetTableOptimizer", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
	})
}

// TestNewOps_ColumnStatistics tests column statistics for table and partition.
func TestNewOps_ColumnStatistics(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// Setup: create db and table.
	dispatchNewOp(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "mydb"},
	})
	dispatchNewOp(t, h, "CreateTable", map[string]any{
		"DatabaseName": "mydb",
		"TableInput":   map[string]any{"Name": "mytable"},
	})

	// UpdateColumnStatisticsForTable
	dispatchNewOp(t, h, "UpdateColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"ColumnStatisticsList": []any{
			map[string]any{
				"ColumnName": "col1",
				"ColumnType": "string",
				"StatisticsData": map[string]any{
					"Type": "STRING",
				},
			},
		},
	})

	// GetColumnStatisticsForTable
	out := dispatchNewOp(t, h, "GetColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"ColumnNames":  []any{"col1"},
	})
	stats, _ := out["ColumnStatisticsList"].([]any)
	if len(stats) != 1 {
		t.Errorf("expected 1 column stat, got %d", len(stats))
	}

	// DeleteColumnStatisticsForTable
	dispatchNewOp(t, h, "DeleteColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"ColumnName":   "col1",
	})

	// Verify deletion
	out2 := dispatchNewOp(t, h, "GetColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	stats2, _ := out2["ColumnStatisticsList"].([]any)
	if len(stats2) != 0 {
		t.Errorf("expected 0 column stats after delete, got %d", len(stats2))
	}

	// UpdateColumnStatisticsForPartition
	dispatchNewOp(t, h, "UpdateColumnStatisticsForPartition", map[string]any{
		"DatabaseName":    "mydb",
		"TableName":       "mytable",
		"PartitionValues": []any{"2023"},
		"ColumnStatisticsList": []any{
			map[string]any{
				"ColumnName": "col2",
				"ColumnType": "bigint",
				"StatisticsData": map[string]any{
					"Type": "LONG",
				},
			},
		},
	})

	// GetColumnStatisticsForPartition
	out3 := dispatchNewOp(t, h, "GetColumnStatisticsForPartition", map[string]any{
		"DatabaseName":    "mydb",
		"TableName":       "mytable",
		"PartitionValues": []any{"2023"},
	})
	stats3, _ := out3["ColumnStatisticsList"].([]any)
	if len(stats3) != 1 {
		t.Errorf("expected 1 partition stat, got %d", len(stats3))
	}

	// DeleteColumnStatisticsForPartition
	dispatchNewOp(t, h, "DeleteColumnStatisticsForPartition", map[string]any{
		"DatabaseName":    "mydb",
		"TableName":       "mytable",
		"PartitionValues": []any{"2023"},
		"ColumnName":      "col2",
	})
}

// TestNewOps_ResourcePolicy tests resource policy CRUD.
func TestNewOps_ResourcePolicy(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// PutResourcePolicy
	out := dispatchNewOp(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[]}`,
	})
	hash, _ := out["PolicyHash"].(string)
	if hash == "" {
		t.Error("expected non-empty PolicyHash")
	}

	// GetResourcePolicy
	out2 := dispatchNewOp(t, h, "GetResourcePolicy", map[string]any{})
	if out2["PolicyInJson"] == "" {
		t.Errorf("expected non-empty PolicyInJson: %v", out2)
	}

	// DeleteResourcePolicy
	dispatchNewOp(t, h, "DeleteResourcePolicy", map[string]any{})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetResourcePolicy", map[string]any{})
}

// TestNewOps_MLTransform tests MLTransform CRUD.
func TestNewOps_MLTransform(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateMLTransform
	out := dispatchNewOp(t, h, "CreateMLTransform", map[string]any{
		"Name":        "my-transform",
		"Description": "test transform",
		"Role":        "arn:aws:iam::123456789012:role/GlueRole",
		"InputRecordTables": []any{
			map[string]any{"DatabaseName": "mydb", "TableName": "mytable"},
		},
		"Parameters": map[string]any{"TransformType": "FIND_MATCHES"},
	})
	transformID, _ := out["TransformId"].(string)
	if transformID == "" {
		t.Fatalf("expected TransformId, got: %v", out)
	}

	// GetMLTransform
	out2 := dispatchNewOp(t, h, "GetMLTransform", map[string]any{"TransformId": transformID})
	if out2["Name"] != "my-transform" {
		t.Errorf("Name mismatch: %v", out2["Name"])
	}

	// GetMLTransforms
	out3 := dispatchNewOp(t, h, "GetMLTransforms", map[string]any{})
	transforms, _ := out3["Transforms"].([]any)
	if len(transforms) != 1 {
		t.Errorf("expected 1 transform, got %d", len(transforms))
	}

	// UpdateMLTransform
	dispatchNewOp(t, h, "UpdateMLTransform", map[string]any{
		"TransformId": transformID,
		"Name":        "my-transform",
		"Description": "updated transform",
	})

	// DeleteMLTransform
	dispatchNewOp(t, h, "DeleteMLTransform", map[string]any{"TransformId": transformID})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetMLTransform", map[string]any{"TransformId": transformID})
}

// TestNewOps_Catalog tests Catalog CRUD.
func TestNewOps_Catalog(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateCatalog
	dispatchNewOp(t, h, "CreateCatalog", map[string]any{
		"CatalogId": "my-catalog",
		"CatalogInput": map[string]any{
			"Description": "test catalog",
		},
	})

	// GetCatalog
	out := dispatchNewOp(t, h, "GetCatalog", map[string]any{"CatalogId": "my-catalog"})
	catalog := out["Catalog"].(map[string]any)
	if catalog["CatalogId"] != "my-catalog" {
		t.Errorf("CatalogId mismatch: %v", catalog)
	}

	// GetCatalogs
	out2 := dispatchNewOp(t, h, "GetCatalogs", map[string]any{})
	catalogs, _ := out2["CatalogList"].([]any)
	if len(catalogs) != 1 {
		t.Errorf("expected 1 catalog, got %d", len(catalogs))
	}

	// UpdateCatalog
	dispatchNewOp(t, h, "UpdateCatalog", map[string]any{
		"CatalogId": "my-catalog",
		"CatalogInput": map[string]any{
			"Description": "updated catalog",
		},
	})

	// DeleteCatalog
	dispatchNewOp(t, h, "DeleteCatalog", map[string]any{"CatalogId": "my-catalog"})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetCatalog", map[string]any{"CatalogId": "my-catalog"})
}

// TestNewOps_DataCatalogEncryptionSettings tests encryption settings.
func TestNewOps_DataCatalogEncryptionSettings(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// PutDataCatalogEncryptionSettings
	dispatchNewOp(t, h, "PutDataCatalogEncryptionSettings", map[string]any{
		"DataCatalogEncryptionSettings": map[string]any{
			"EncryptionAtRest": map[string]any{
				"CatalogEncryptionMode": "SSE-KMS",
				"SseAwsKmsKeyId":        "key-id",
			},
		},
	})

	// GetDataCatalogEncryptionSettings
	out := dispatchNewOp(t, h, "GetDataCatalogEncryptionSettings", map[string]any{})
	settings := out["DataCatalogEncryptionSettings"].(map[string]any)
	ear, _ := settings["EncryptionAtRest"].(map[string]any)
	if ear == nil || ear["CatalogEncryptionMode"] != "SSE-KMS" {
		t.Errorf("EncryptionAtRest mismatch: %v", settings)
	}
}

// TestNewOps_WorkflowRunExtras tests StopWorkflowRun and PutWorkflowRunProperties.
func TestNewOps_WorkflowRunExtras(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// Setup: create workflow and start a run.
	dispatchNewOp(t, h, "CreateWorkflow", map[string]any{
		"Name":        "my-workflow",
		"Description": "test",
	})
	out := dispatchNewOp(t, h, "StartWorkflowRun", map[string]any{"Name": "my-workflow"})
	runID, _ := out["RunId"].(string)
	if runID == "" {
		t.Fatalf("expected RunId")
	}

	// PutWorkflowRunProperties
	dispatchNewOp(t, h, "PutWorkflowRunProperties", map[string]any{
		"Name":          "my-workflow",
		"RunId":         runID,
		"RunProperties": map[string]any{"key1": "value1"},
	})

	// StopWorkflowRun
	dispatchNewOp(t, h, "StopWorkflowRun", map[string]any{
		"Name":  "my-workflow",
		"RunId": runID,
	})
}

// TestNewOps_DeleteTableVersion tests DeleteTableVersion.
func TestNewOps_DeleteTableVersion(t *testing.T) {
	t.Parallel()
	b := glue.NewInMemoryBackend("123456789012", "us-east-1")
	h := glue.NewHandler(b)

	// Seed a table version directly.
	b.AddTableVersionInternal("mydb", "mytable", &glue.TableVersion{VersionID: "1"})

	// DeleteTableVersion
	dispatchNewOp(t, h, "DeleteTableVersion", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"VersionId":    "1",
	})

	// Verify deletion (second delete should fail).
	dispatchNewOpExpectError(t, h, "DeleteTableVersion", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"VersionId":    "1",
	})
}

// TestNewOps_UpdateDevEndpoint tests UpdateDevEndpoint.
func TestNewOps_UpdateDevEndpoint(t *testing.T) {
	t.Parallel()
	b := glue.NewInMemoryBackend("123456789012", "us-east-1")
	h := glue.NewHandler(b)

	// Create a dev endpoint via internal helper.
	ep := &glue.DevEndpoint{EndpointName: "my-endpoint", Status: "READY"}
	b.AddDevEndpointInternal(ep)

	// UpdateDevEndpoint
	dispatchNewOp(t, h, "UpdateDevEndpoint", map[string]any{
		"EndpointName": "my-endpoint",
		"AddArguments": map[string]any{"GLUE_PYTHON_VERSION": "3"},
	})
}
