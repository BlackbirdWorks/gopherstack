package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestUserDefinedFunction tests UDF CRUD.
func TestUserDefinedFunction(t *testing.T) {
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
	out2 := dispatchNewOp(t, h, "GetUserDefinedFunctions", map[string]any{"DatabaseName": "mydb", "Pattern": ".*"})
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

// TestGetUserDefinedFunctions_Pattern locks in Pattern (required,
// api_op_GetUserDefinedFunctions.go, enforced client-side by
// validateOpGetUserDefinedFunctionsInput despite its own doc comment calling
// it optional). Before this fix Pattern wasn't even declared on the input
// struct, so every call returned every UDF in the database unfiltered.
func TestGetUserDefinedFunctions_Pattern(t *testing.T) {
	t.Parallel()

	h := newGlueHandler(t)
	dispatchNewOp(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "patterndb"}})
	for _, name := range []string{"alpha_func", "beta_func", "alpha_other"} {
		dispatchNewOp(t, h, "CreateUserDefinedFunction", map[string]any{
			"DatabaseName":  "patterndb",
			"FunctionInput": map[string]any{"FunctionName": name, "ClassName": "com.example.C"},
		})
	}

	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{name: "matches subset", pattern: "^alpha_.*", want: []string{"alpha_func", "alpha_other"}},
		{name: "matches all", pattern: ".*", want: []string{"alpha_func", "alpha_other", "beta_func"}},
		{name: "matches none", pattern: "^zzz.*", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rr := doGlueOp(t, h, "GetUserDefinedFunctions", map[string]any{
				"DatabaseName": "patterndb",
				"Pattern":      tc.pattern,
			})
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			var out struct {
				UserDefinedFunctions []struct {
					FunctionName string `json:"FunctionName"`
				} `json:"UserDefinedFunctions"`
			}
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&out))

			got := make([]string, 0, len(out.UserDefinedFunctions))
			for _, u := range out.UserDefinedFunctions {
				got = append(got, u.FunctionName)
			}
			assert.ElementsMatch(t, tc.want, got)
		})
	}

	t.Run("missing pattern rejected", func(t *testing.T) {
		t.Parallel()
		dispatchNewOpExpectError(t, h, "GetUserDefinedFunctions", map[string]any{"DatabaseName": "patterndb"})
	})
}
