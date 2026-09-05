package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlueprint_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(
		t, h, "CreateBlueprint", map[string]any{"Name": "my-bp", "BlueprintLocation": "s3://bucket/my-bp"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBlueprint returns it
	rec = doGlueRequest(t, h, "GetBlueprint", map[string]any{"Name": "my-bp"})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListBlueprints
	rec = doGlueRequest(t, h, "ListBlueprints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-bp")

	// UpdateBlueprint
	rec = doGlueRequest(
		t, h, "UpdateBlueprint", map[string]any{"Name": "my-bp", "BlueprintLocation": "s3://bucket/my-bp"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteBlueprint
	rec = doGlueRequest(t, h, "DeleteBlueprint", map[string]any{"Name": "my-bp"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBlueprintRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a blueprint first
	doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": "run-bp", "BlueprintLocation": "s3://bucket/run-bp"})

	// Start a run
	rec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{
		"BlueprintName": "run-bp",
		"RoleArn":       "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RunId")

	// GetBlueprintRuns
	rec = doGlueRequest(t, h, "GetBlueprintRuns", map[string]any{"BlueprintName": "run-bp"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Runs")
}

// TestBlueprint_GetNotFound verifies GetBlueprint returns
// EntityNotFoundException for a missing blueprint.
func TestBlueprint_GetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bpName    string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "missing_blueprint_returns_404_error",
			bpName:    "no-such-bp",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "existing_blueprint_returns_ok",
			bpName:   "real-bp",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.create {
				body := map[string]any{
					"Name":              tt.bpName,
					"BlueprintLocation": "s3://bucket/" + tt.bpName,
				}
				rec := doGlueRequest(t, h, "CreateBlueprint", body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "GetBlueprint", map[string]any{"Name": tt.bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBlueprint_UpdateNotFound verifies UpdateBlueprint returns
// EntityNotFoundException for a missing blueprint (not upsert).
func TestBlueprint_UpdateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "update_missing_blueprint_fails",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "update_existing_blueprint_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bpName := "upd-bp-" + tt.name
			body := map[string]any{"Name": bpName, "BlueprintLocation": "s3://bucket/" + bpName}
			if tt.create {
				doGlueRequest(t, h, "CreateBlueprint", body)
			}

			rec := doGlueRequest(t, h, "UpdateBlueprint", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBlueprint_DeleteNotFound verifies DeleteBlueprint returns
// InvalidInputException for a missing blueprint: its error switch
// (glue@v1.152.0 deserializers.go) has no EntityNotFoundException case,
// unlike GetBlueprint's.
func TestBlueprint_DeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "delete_missing_blueprint_fails",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidInputException",
		},
		{
			name:     "delete_existing_blueprint_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bpName := "del-bp-" + tt.name
			if tt.create {
				createBody := map[string]any{"Name": bpName, "BlueprintLocation": "s3://bucket/" + bpName}
				doGlueRequest(t, h, "CreateBlueprint", createBody)
			}

			rec := doGlueRequest(t, h, "DeleteBlueprint", map[string]any{"Name": bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestStartBlueprintRun_NotFound verifies StartBlueprintRun returns
// EntityNotFoundException when the blueprint does not exist.
func TestStartBlueprintRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "start_run_missing_blueprint_fails",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "start_run_existing_blueprint_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bpName := "run-bp-" + tt.name
			if tt.create {
				createBody := map[string]any{"Name": bpName, "BlueprintLocation": "s3://bucket/" + bpName}
				doGlueRequest(t, h, "CreateBlueprint", createBody)
			}

			rec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{
				"BlueprintName": bpName,
				"RoleArn":       "arn:aws:iam::000000000000:role/GlueRole",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestCreateBlueprint_NameRequired verifies CreateBlueprint
// rejects an empty Name with InvalidInputException.
func TestCreateBlueprint_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bpName    string
		wantError string
		wantCode  int
	}{
		{
			name:      "empty_name_rejected",
			bpName:    "",
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidInputException",
		},
		{
			name:     "valid_name_accepted",
			bpName:   "valid-blueprint",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Name": tt.bpName, "BlueprintLocation": "s3://bucket/" + tt.bpName}
			rec := doGlueRequest(t, h, "CreateBlueprint", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestBlueprint_CRUD_Full(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name:        "create",
			op:          "CreateBlueprint",
			body:        map[string]any{"Name": "bp1", "BlueprintLocation": "s3://bucket/bp1"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name:     "get",
			op:       "GetBlueprint",
			body:     map[string]any{"Name": "bp1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "list",
			op:       "ListBlueprints",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "update",
			op:       "UpdateBlueprint",
			body:     map[string]any{"Name": "bp1", "BlueprintLocation": "s3://bucket/bp1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "batch-get",
			op:       "BatchGetBlueprints",
			body:     map[string]any{"Names": []string{"bp1", "no-bp"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteBlueprint",
			body:     map[string]any{"Name": "bp1"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				body := map[string]any{"Name": "bp1", "BlueprintLocation": "s3://bucket/bp1"}
				doGlueRequest(t, h, "CreateBlueprint", body)
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBlueprint_Run_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": "bp-run", "BlueprintLocation": "s3://bucket/bp-run"})

	startBpRec1 := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{
		"BlueprintName": "bp-run",
		"RoleArn":       "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, startBpRec1.Code)
	var startBpOut1 map[string]any
	require.NoError(t, json.Unmarshal(startBpRec1.Body.Bytes(), &startBpOut1))
	assert.NotEmpty(t, startBpOut1["RunId"])

	startRec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{
		"BlueprintName": "bp-run",
		"RoleArn":       "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	getBpRunRec := doGlueRequest(t, h, "GetBlueprintRun", map[string]any{
		"BlueprintName": "bp-run",
		"RunId":         runID,
	})
	require.Equal(t, http.StatusOK, getBpRunRec.Code)
	assert.Contains(t, getBpRunRec.Body.String(), runID)

	getBpRunsRec := doGlueRequest(t, h, "GetBlueprintRuns", map[string]any{"BlueprintName": "bp-run"})
	require.Equal(t, http.StatusOK, getBpRunsRec.Code)
	var getBpRunsOut map[string]any
	require.NoError(t, json.Unmarshal(getBpRunsRec.Body.Bytes(), &getBpRunsOut))
	bpRuns := getBpRunsOut["BlueprintRuns"].([]any)
	assert.GreaterOrEqual(t, len(bpRuns), 1)
}

// TestBlueprintRun_ErrorPropagation verifies error propagation for blueprint runs.
func TestBlueprintRun_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty_run_id_returns_400",
			input:    map[string]any{"BlueprintName": "bp", "RunId": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown_run_id_returns_400",
			input:    map[string]any{"BlueprintName": "bp", "RunId": "no-such-run"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetBlueprintRun", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
