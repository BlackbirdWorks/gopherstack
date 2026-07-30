package codepipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_CreateCustomActionType(t *testing.T) {
	t.Parallel()

	validInput := map[string]any{
		"category":              "Build",
		"provider":              "MyBuilder",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 1, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
	}

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name:       "success",
			input:      validInput,
			httpStatus: http.StatusOK,
		},
		{
			name: "duplicate",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", validInput)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      validInput,
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_category",
			input:      map[string]any{"provider": "P", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_provider",
			input:      map[string]any{"category": "Build", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_version",
			input:      map[string]any{"category": "Build", "provider": "P"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CreateCustomActionType", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				at, ok := out["actionType"].(map[string]any)
				require.True(t, ok)
				id, ok := at["id"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Build", id["category"])
				assert.Equal(t, "MyBuilder", id["provider"])
				assert.Equal(t, "1", id["version"])
			}
		})
	}
}

func TestCreateCustomActionType_ActionConfigurationProperties_EmptyNotAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		includeEmptyVal bool // true → send configurationProperties:[], false → omit key
	}{
		{
			name:            "no configurationProperties returns actionConfigurationProperties:[]",
			includeEmptyVal: false,
		},
		{
			name:            "empty configurationProperties returns actionConfigurationProperties:[]",
			includeEmptyVal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"category":              "Build",
				"provider":              "MyProvider",
				"version":               "1",
				"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
				"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
			}
			if tt.includeEmptyVal {
				body["configurationProperties"] = []any{}
			}

			rec := doRequest(t, h, "CreateCustomActionType", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			actionType, ok := raw["actionType"].(map[string]any)
			require.True(t, ok, "actionType must be an object")

			props, hasKey := actionType["actionConfigurationProperties"]
			assert.True(t, hasKey,
				"CreateCustomActionType must include 'actionConfigurationProperties' key even when empty")
			assert.IsType(t, []any{}, props,
				"'actionConfigurationProperties' must be an array, not null or absent")
			assert.Empty(t, props, "'actionConfigurationProperties' must be [] not populated")
		})
	}
}

func TestCreateCustomActionType_ActionConfigurationProperties_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with explicit configuration properties.
	rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
		"category":              "Build",
		"provider":              "MyProvider",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		"configurationProperties": []any{
			map[string]any{"name": "ProjectName", "required": true, "key": true, "secret": false},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	actionType, ok := raw["actionType"].(map[string]any)
	require.True(t, ok)

	props, hasKey := actionType["actionConfigurationProperties"]
	assert.True(t, hasKey, "must have actionConfigurationProperties key")
	propsSlice, ok := props.([]any)
	require.True(t, ok, "must be an array")
	assert.Len(t, propsSlice, 1, "must have the one property we created")
}

// ── Bug 3: Webhook Filters [] not absent ────────────────────────────────────

func TestHandler_DeleteCustomActionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category": "Deploy", "provider": "MyDeploy", "version": "2",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      map[string]any{"category": "Deploy", "provider": "MyDeploy", "version": "2"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"category": "Build", "provider": "Ghost", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_category",
			setup:      nil,
			input:      map[string]any{"provider": "P", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteCustomActionType", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteCustomActionType_InUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "type in use by pipeline rejected",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category": "Build", "provider": "InUseBuilder", "version": "1",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				p := samplePipeline("using-pipeline")
				p.Stages[0].Actions[0].ActionTypeID = codepipeline.ActionTypeID{
					Category: "Build", Owner: "Custom", Provider: "InUseBuilder", Version: "1",
				}
				_, err := h.Backend.CreatePipeline(context.Background(), p, nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"category": "Build", "provider": "InUseBuilder", "version": "1",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceInUseException",
		},
		{
			name: "type not in use deleted ok",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category": "Build", "provider": "FreeBuilder", "version": "1",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      map[string]any{"category": "Build", "provider": "FreeBuilder", "version": "1"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteCustomActionType", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantType, out["__type"])
			}
		})
	}
}

// --------------------------------------------------------------------------
// #22 GetPipelineState includes actionStates
// --------------------------------------------------------------------------

func TestHandler_GetActionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category": "Test", "provider": "MyTest", "version": "3",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      map[string]any{"category": "Test", "owner": "Custom", "provider": "MyTest", "version": "3"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"category": "Build", "owner": "Custom", "provider": "Ghost", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_category",
			setup:      nil,
			input:      map[string]any{"owner": "Custom", "provider": "P", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetActionType", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				at, ok := out["actionType"].(map[string]any)
				require.True(t, ok)
				id, ok := at["id"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Test", id["category"])
				assert.Equal(t, "MyTest", id["provider"])
			}
		})
	}
}

// TestGetActionType_ActionTypeDeclarationShape asserts GetActionType's real wire
// shape is types.ActionTypeDeclaration, not the legacy types.ActionType shape
// CreateCustomActionType/ListActionTypes use. types.ActionTypeDeclaration has no
// "actionConfigurationProperties"/"settings" members at all (those are
// ActionType-only) -- it has "properties"/"urls" instead, and (like the real
// serializer's `if v.Properties != nil`) those are omitted, not forced to an
// empty array, when the record was only ever created via the legacy
// CreateCustomActionType path and never given declaration data by UpdateActionType.
func TestGetActionType_ActionTypeDeclarationShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create action type via the legacy shape -- no declaration data exists yet.
	createRec := doRequest(t, h, "CreateCustomActionType", map[string]any{
		"category":              "Build",
		"provider":              "MyProvider",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, "GetActionType", map[string]any{
		"category": "Build",
		"owner":    "Custom",
		"provider": "MyProvider",
		"version":  "1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	actionType, ok := raw["actionType"].(map[string]any)
	require.True(t, ok, "actionType must be an object")

	_, hasLegacyProps := actionType["actionConfigurationProperties"]
	assert.False(t, hasLegacyProps,
		"ActionTypeDeclaration has no actionConfigurationProperties member -- that's ActionType-only")

	_, hasLegacySettings := actionType["settings"]
	assert.False(t, hasLegacySettings,
		"ActionTypeDeclaration has no settings member -- that's ActionType-only")

	_, hasExecutor := actionType["executor"]
	assert.False(t, hasExecutor,
		"executor must be genuinely absent (omitted, not fabricated) for a record never given declaration data")

	_, hasProperties := actionType["properties"]
	assert.False(t, hasProperties,
		"properties must be genuinely absent, matching the real serializer's `if v.Properties != nil` behavior")

	id, ok := actionType["id"].(map[string]any)
	require.True(t, ok, "id is a required ActionTypeDeclaration member")
	assert.Equal(t, "Build", id["category"])
	assert.Equal(t, "MyProvider", id["provider"])

	_, hasInput := actionType["inputArtifactDetails"]
	assert.True(t, hasInput, "inputArtifactDetails is a required ActionTypeDeclaration member")

	_, hasOutput := actionType["outputArtifactDetails"]
	assert.True(t, hasOutput, "outputArtifactDetails is a required ActionTypeDeclaration member")
}

// TestGetActionType_AfterUpdateActionType_ReturnsDeclarationData asserts that
// once UpdateActionType supplies real ActionTypeDeclaration data (executor/
// properties/urls/description/permissions), GetActionType round-trips it --
// while the legacy CreateCustomActionType-era settings/configurationProperties
// (which UpdateActionType's real input has no members to express) survive
// unchanged, verified separately via ListActionTypes.
func TestGetActionType_AfterUpdateActionType_ReturnsDeclarationData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCustomActionType", map[string]any{
		"category":              "Build",
		"provider":              "MyProvider",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		"settings": map[string]any{
			"entityUrlTemplate": "https://legacy.example.com",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	updateRec := doRequest(t, h, "UpdateActionType", map[string]any{
		"actionType": map[string]any{
			"id": map[string]any{
				"category": "Build", "owner": "Custom", "provider": "MyProvider", "version": "1",
			},
			"description":           "updated via declaration",
			"inputArtifactDetails":  map[string]any{"minimumCount": 1, "maximumCount": 5},
			"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
			"executor": map[string]any{
				"type": "Lambda",
				"configuration": map[string]any{
					"lambdaExecutorConfiguration": map[string]any{
						"lambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:MyFn",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	rec := doRequest(t, h, "GetActionType", map[string]any{
		"category": "Build", "owner": "Custom", "provider": "MyProvider", "version": "1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	actionType, ok := raw["actionType"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "updated via declaration", actionType["description"])

	executor, ok := actionType["executor"].(map[string]any)
	require.True(t, ok, "executor must round-trip after UpdateActionType sets it")
	assert.Equal(t, "Lambda", executor["type"])

	config, ok := executor["configuration"].(map[string]any)
	require.True(t, ok)
	lambdaConfig, ok := config["lambdaExecutorConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:MyFn", lambdaConfig["lambdaFunctionArn"])

	// The legacy settings this record was created with are untouched by
	// UpdateActionType (its real input has no member that could express them) --
	// verified via ListActionTypes, the legacy-shape read path.
	listRec := doRequest(t, h, "ListActionTypes", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	items, _ := listOut["actionTypes"].([]any)
	require.Len(t, items, 1)
	item0, _ := items[0].(map[string]any)
	settings, ok := item0["settings"].(map[string]any)
	require.True(t, ok, "legacy settings must survive an UpdateActionType call")
	assert.Equal(t, "https://legacy.example.com", settings["entityUrlTemplate"])
}

func TestHandler_ListActionTypes_FullShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		checkFn    func(t *testing.T, items []any)
		filter     string
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "full shape includes id/input/output/settings",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category":              "Build",
					"provider":              "MyBuilder",
					"version":               "1",
					"inputArtifactDetails":  map[string]any{"minimumCount": 1, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 1},
					"settings": map[string]any{
						"entityUrlTemplate": "https://example.com/{Config:JobType}",
					},
					"configurationProperties": []map[string]any{
						{"name": "ProjectName", "key": true, "required": true, "secret": false},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
			checkFn: func(t *testing.T, items []any) {
				t.Helper()

				item0, _ := items[0].(map[string]any)
				id, ok := item0["id"].(map[string]any)
				require.True(t, ok, "id key required")
				assert.Equal(t, "Build", id["category"])
				assert.Equal(t, "Custom", id["owner"])
				assert.Equal(t, "MyBuilder", id["provider"])
				assert.Equal(t, "1", id["version"])

				_, ok = item0["inputArtifactDetails"]
				assert.True(t, ok, "inputArtifactDetails required")

				_, ok = item0["outputArtifactDetails"]
				assert.True(t, ok, "outputArtifactDetails required")

				settings, ok := item0["settings"].(map[string]any)
				require.True(t, ok, "settings required")
				assert.Equal(t, "https://example.com/{Config:JobType}", settings["entityUrlTemplate"])
			},
		},
		{
			name: "actionOwnerFilter=Custom returns matching",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category":              "Test",
					"provider":              "MyTest",
					"version":               "1",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			filter:     "Custom",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "actionOwnerFilter=ThirdParty returns empty when only Custom types",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category":              "Build",
					"provider":              "MyBuilder",
					"version":               "1",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			filter:     "ThirdParty",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "empty list when no types",
			setup:      nil,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			input := map[string]any{}
			if tt.filter != "" {
				input["actionOwnerFilter"] = tt.filter
			}

			rec := doRequest(t, h, "ListActionTypes", input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			items, _ := out["actionTypes"].([]any)
			assert.Len(t, items, tt.wantCount)

			if tt.checkFn != nil && len(items) > 0 {
				tt.checkFn(t, items)
			}
		})
	}
}

// --------------------------------------------------------------------------
// #17 UpdateActionType preserves all fields
// --------------------------------------------------------------------------

func TestCPPagination_ListActionTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 5 custom action types to paginate over.
	for i := range 5 {
		rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
			"category":              "Build",
			"provider":              fmt.Sprintf("CustomProvider%02d", i),
			"version":               "1",
			"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
			"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Verify all 5 appear with no NextToken (default cap 25 covers all).
	rec := doRequest(t, h, "ListActionTypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken   string           `json:"nextToken"`
		ActionTypes []map[string]any `json:"actionTypes"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	assert.Len(t, out.ActionTypes, 5)
	assert.Empty(t, out.NextToken, "5 items fit within the 25-cap default page")
}

func TestCPPagination_ListActionTypes_MultiPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 30 custom action types to exceed the fixed cap of 25.
	for i := range 30 {
		rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
			"category":              "Build",
			"provider":              fmt.Sprintf("Provider%03d", i),
			"version":               "1",
			"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
			"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var nextToken string
	total := 0
	pages := 0

	for {
		body := map[string]any{}
		if nextToken != "" {
			body["nextToken"] = nextToken
		}

		rec := doRequest(t, h, "ListActionTypes", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken   string           `json:"nextToken"`
			ActionTypes []map[string]any `json:"actionTypes"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		total += len(out.ActionTypes)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 30, total)
	assert.Equal(t, 2, pages) // 25+5
}

// ---------------------------------------------------------------------------
// ListRuleExecutions — MaxResults (1-100), always empty list for known pipeline
// ---------------------------------------------------------------------------

func TestHandler_UpdateActionType_FullFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateInput map[string]any
		checkFn     func(t *testing.T, h *codepipeline.Handler)
		name        string
		wantStatus  int
	}{
		{
			name: "update with full ActionTypeDeclaration body persists executor and artifact details",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"category": "Build",
						"owner":    "Custom",
						"provider": "MyBuilder",
						"version":  "1",
					},
					"inputArtifactDetails":  map[string]any{"minimumCount": 2, "maximumCount": 10},
					"outputArtifactDetails": map[string]any{"minimumCount": 1, "maximumCount": 5},
					"description":           "a custom builder",
					"executor": map[string]any{
						"type": "JobWorker",
						"configuration": map[string]any{
							"jobWorkerExecutorConfiguration": map[string]any{
								"pollingAccounts": []any{"000000000000"},
							},
						},
					},
					"urls": map[string]any{
						"entityUrlTemplate": "https://ci.example.com/job/{Config:ProjectName}",
					},
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, h *codepipeline.Handler) {
				t.Helper()

				rec := doRequest(t, h, "GetActionType", map[string]any{
					"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				at, _ := out["actionType"].(map[string]any)

				inArt, _ := at["inputArtifactDetails"].(map[string]any)
				minCnt, _ := inArt["minimumCount"].(float64)
				assert.InDelta(t, float64(2), minCnt, 0)

				assert.Equal(t, "a custom builder", at["description"])

				executor, _ := at["executor"].(map[string]any)
				require.NotNil(t, executor)
				assert.Equal(t, "JobWorker", executor["type"])

				urls, _ := at["urls"].(map[string]any)
				require.NotNil(t, urls)
				assert.Equal(t, "https://ci.example.com/job/{Config:ProjectName}", urls["entityUrlTemplate"])
			},
		},
		{
			name: "update missing category rejected",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"owner": "Custom", "provider": "MyBuilder", "version": "1",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update missing executor rejected",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
					},
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update non-existent type returns ActionTypeNotFoundException",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"category": "Build", "owner": "Custom", "provider": "Ghost", "version": "1",
					},
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
					"executor": map[string]any{
						"type":          "JobWorker",
						"configuration": map[string]any{},
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update invalid executor type rejected",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
					},
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
					"executor": map[string]any{
						"type":          "NotARealExecutorType",
						"configuration": map[string]any{},
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update Lambda executor missing lambdaFunctionArn rejected",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
					},
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
					"executor": map[string]any{
						"type": "Lambda",
						"configuration": map[string]any{
							"lambdaExecutorConfiguration": map[string]any{},
						},
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Pre-create the action type for update tests that expect success or type lookup.
			if tt.wantStatus == http.StatusOK ||
				tt.name == "update non-existent type returns ActionTypeNotFoundException" {
				if tt.wantStatus == http.StatusOK {
					rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
						"category": "Build", "provider": "MyBuilder", "version": "1",
						"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
						"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			rec := doRequest(t, h, "UpdateActionType", tt.updateInput)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFn != nil && tt.wantStatus == http.StatusOK {
				tt.checkFn(t, h)
			}
		})
	}
}

// --------------------------------------------------------------------------
// #18 CreatePipeline validation: RoleArn required
// --------------------------------------------------------------------------

func TestHandler_ActionTypeOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	cat := &codepipeline.CustomActionType{
		Category: "Test",
		Provider: "MyCIProvider",
		Version:  "1",
	}
	_, err := h.Backend.CreateCustomActionType(context.Background(), cat)
	require.NoError(t, err)

	// List action types
	rec := doRequest(t, h, "ListActionTypes", map[string]any{})
	require.Equal(t, 200, rec.Code)

	// Get action type
	rec = doRequest(t, h, "GetActionType", map[string]any{
		"category": "Test",
		"owner":    "Custom",
		"provider": "MyCIProvider",
		"version":  "1",
	})
	assert.Equal(t, 200, rec.Code)

	// Get action type with missing id
	rec = doRequest(t, h, "GetActionType", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Update action type: real UpdateActionType input is ActionTypeDeclaration,
	// which requires executor/inputArtifactDetails/outputArtifactDetails in
	// addition to id.
	rec = doRequest(t, h, "UpdateActionType", map[string]any{
		"actionType": map[string]any{
			"id": map[string]any{
				"category": "Test",
				"owner":    "Custom",
				"provider": "MyCIProvider",
				"version":  "1",
			},
			"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
			"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
			"executor": map[string]any{
				"type": "JobWorker",
				"configuration": map[string]any{
					"jobWorkerExecutorConfiguration": map[string]any{},
				},
			},
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Update with missing actionType
	rec = doRequest(t, h, "UpdateActionType", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Delete custom action type
	rec = doRequest(t, h, "DeleteCustomActionType", map[string]any{
		"category": "Test",
		"provider": "MyCIProvider",
		"version":  "1",
	})
	assert.Equal(t, 200, rec.Code)

	// Delete with missing category
	rec = doRequest(t, h, "DeleteCustomActionType", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Execution list tests ----

func TestActionCategoryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category string
		wantErr  bool
	}{
		{name: "source", category: "Source", wantErr: false},
		{name: "build", category: "Build", wantErr: false},
		{name: "deploy", category: "Deploy", wantErr: false},
		{name: "test", category: "Test", wantErr: false},
		{name: "invoke", category: "Invoke", wantErr: false},
		{name: "approval", category: "Approval", wantErr: false},
		{name: "compute", category: "Compute", wantErr: false},
		{name: "invalid", category: "Invalid", wantErr: true},
		{name: "lowercase", category: "build", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
				"category":              tt.category,
				"provider":              "TestProvider",
				"version":               "1",
				"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
				"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
			})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestAddCustomActionTypeInternal_DeepCopy(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	cat := &codepipeline.CustomActionType{
		Category:              "Build",
		Provider:              "CopyTest",
		Version:               "1",
		Tags:                  map[string]string{"original": "value"},
		InputArtifactDetails:  codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
		OutputArtifactDetails: codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
	}

	b.AddCustomActionTypeInternal(cat)

	// Mutate original - backend should not be affected.
	cat.Tags["original"] = "mutated"

	retrieved, err := b.GetActionType(context.Background(), "Build", "Custom", "CopyTest", "1")
	require.NoError(t, err)
	assert.Equal(t, "value", retrieved.Tags["original"])
}
