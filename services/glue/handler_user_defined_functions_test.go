package glue_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// dispatchNewOpExpectError sends an operation and expects a non-200 status.
func dispatchNewOpExpectError(t *testing.T, h *glue.Handler, op string, body any) {
	t.Helper()
	rr := doGlueOp(t, h, op, body)
	if rr.Code == http.StatusOK {
		t.Fatalf("op %s: expected error but got 200, body: %s", op, rr.Body.String())
	}
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
