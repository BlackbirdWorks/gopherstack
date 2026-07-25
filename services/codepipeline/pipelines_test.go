package codepipeline_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_CreatePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		name       string
		wantName   string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			input: map[string]any{
				"pipeline": samplePipeline("my-pipeline"),
			},
			wantStatus: http.StatusOK,
			wantName:   "my-pipeline",
		},
		{
			name:       "missing pipeline",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name: "missing name",
			input: map[string]any{
				"pipeline": map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name: "duplicate name",
			input: map[string]any{
				"pipeline": samplePipeline("duplicate-pipeline"),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate name" {
				rec := doRequest(t, h, "CreatePipeline", map[string]any{
					"pipeline": samplePipeline("duplicate-pipeline"),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreatePipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pipeline, ok := out["pipeline"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, pipeline["name"])
			}
		})
	}
}

func TestHandler_CreatePipeline_RoleArnRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pipeline   map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "missing roleArn rejected",
			pipeline: map[string]any{
				"name":          "no-role-pipeline",
				"artifactStore": map[string]any{"type": "S3", "location": "bucket"},
				"stages": []map[string]any{
					{
						"name": "Source",
						"actions": []map[string]any{
							{
								"name": "Src",
								"actionTypeId": map[string]any{
									"category": "Source",
									"owner":    "AWS",
									"provider": "S3",
									"version":  "1",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "InvalidStructureException",
		},
		{
			name:       "with roleArn succeeds",
			pipeline:   map[string]any(nil), // use samplePipeline
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			input := tt.pipeline
			if input == nil {
				// marshal samplePipeline to map
				b, _ := json.Marshal(samplePipeline("role-test"))
				require.NoError(t, json.Unmarshal(b, &input))
			}

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": input})
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
// #21 ResourceNotFoundException for webhook ARNs in ListTagsForResource
// --------------------------------------------------------------------------

func TestCreatePipeline_DuplicateName_PipelineNameInUseException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate pipeline name returns PipelineNameInUseException"},
		{name: "second duplicate also returns PipelineNameInUseException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			pipeline := samplePipeline("my-pipeline")

			// First create succeeds.
			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
			require.Equal(t, http.StatusOK, rec.Code, "first CreatePipeline must succeed")

			// Second create with same name must return PipelineNameInUseException.
			rec2 := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
			require.Equal(t, http.StatusBadRequest, rec2.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body))

			errType, ok := body["__type"].(string)
			require.True(t, ok, "response must have __type field")
			assert.Equal(t, "PipelineNameInUseException", errType,
				"duplicate pipeline name must return PipelineNameInUseException not InvalidStructureException")
		})
	}
}

func TestCreatePipeline_DuplicateName_NotInvalidStructureException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	pipeline := samplePipeline("collision-pipeline")

	rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
	require.Equal(t, http.StatusBadRequest, rec2.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body))

	errType, _ := body["__type"].(string)
	assert.NotEqual(t, "InvalidStructureException", errType,
		"duplicate pipeline name must NOT return InvalidStructureException")
}

// ── Bug 2: ActionConfigurationProperties [] not absent ──────────────────────

func TestInMemoryBackend_CreatePipeline_WithTags(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	p, err := backend.CreatePipeline(
		context.Background(),
		samplePipeline("tagged-pipeline"),
		map[string]string{"Env": "prod"},
	)
	require.NoError(t, err)

	tags, err := backend.ListTagsForResource(context.Background(), p.Metadata.PipelineArn)
	require.NoError(t, err)

	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}

	assert.Equal(t, "prod", tagMap["Env"])
}

func TestHandler_GetPipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		pipelineFn func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			pipelineFn: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("get-pipeline"), nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"name": "get-pipeline"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			pipelineFn: nil,
			input:      map[string]any{"name": "nonexistent"},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing name",
			pipelineFn: nil,
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.pipelineFn != nil {
				tt.pipelineFn(h)
			}

			rec := doRequest(t, h, "GetPipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetPipeline_VersionHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		version    int
		wantStatus int
	}{
		{
			name:       "version 0 returns latest",
			version:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "exact version match",
			version:    1,
			wantStatus: http.StatusOK,
		},
		{
			// AWS distinguishes a missing pipeline (PipelineNotFoundException)
			// from a missing version of an existing pipeline
			// (PipelineVersionNotFoundException) -- these are different error
			// types on the wire, not just different messages.
			name:       "wrong version returns PipelineVersionNotFoundException",
			version:    99,
			wantStatus: http.StatusBadRequest,
			wantType:   "PipelineVersionNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("ver-pipeline"), nil)
			require.NoError(t, err)

			rec := doRequest(t, h, "GetPipeline", map[string]any{
				"name":    "ver-pipeline",
				"version": tt.version,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantType, out["__type"])
			}
		})
	}
}

// TestHandler_PipelineExecution_TerminalStatus verifies that
// StartPipelineExecution and StopPipelineExecution reach a terminal status
// ("Succeeded" / "Stopped") instead of leaving the execution stuck at
// "InProgress" / "Stopping" forever, and that operating on an unknown
// execution ID returns PipelineExecutionNotFoundException rather than a
// fabricated success response.

func TestHandler_UpdatePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("update-pipeline"), nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"pipeline": samplePipeline("update-pipeline"),
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "not found",
			setup: nil,
			input: map[string]any{
				"pipeline": samplePipeline("nonexistent"),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing pipeline",
			setup:      nil,
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
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

			rec := doRequest(t, h, "UpdatePipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInMemoryBackend_UpdatePipeline_IncrementsVersion(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := backend.CreatePipeline(context.Background(), samplePipeline("versioned-pipeline"), nil)
	require.NoError(t, err)

	updated, err := backend.UpdatePipeline(context.Background(), samplePipeline("versioned-pipeline"))
	require.NoError(t, err)
	assert.Equal(t, 2, updated.Declaration.Version)
}

// TestHandler_UpdatePipeline_VersionIsIgnored locks in that UpdatePipeline
// never validates the incoming pipeline.version against the pipeline's
// current version, for ANY value (including one that could never legitimately
// match) -- real AWS's PipelineDeclaration.Version field is documented as
// purely informational/system-managed ("A new pipeline always has a version
// number of 1. This number is incremented when a pipeline is updated"), with
// no documented optimistic-concurrency contract. A prior revision of this
// backend fabricated a ConflictException for a version mismatch (a
// gopherstack-invented behavior flagged, but left unfixed, in an earlier
// audit pass); that check has been removed. UpdatePipeline instead always
// succeeds and always increments the version by exactly 1, regardless of
// what the caller sent.
func TestHandler_UpdatePipeline_VersionIsIgnored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version int
	}{
		{name: "version 0 (omitted)", version: 0},
		{name: "version matching current", version: 1},
		{name: "version that could never match", version: 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("ver-ignored"), nil)
			require.NoError(t, err)

			p := samplePipeline("ver-ignored")
			p.Version = tt.version

			rec := doRequest(t, h, "UpdatePipeline", map[string]any{"pipeline": p})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			pipeline, _ := out["pipeline"].(map[string]any)
			assert.InDelta(
				t,
				2,
				pipeline["version"],
				0,
				"version always becomes current+1, regardless of the input version",
			)
		})
	}
}

// --------------------------------------------------------------------------
// #30 DeleteCustomActionType: ResourceInUseException when referenced
// --------------------------------------------------------------------------

func TestHandler_DeletePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("delete-pipeline"), nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"name": "delete-pipeline"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setup:      nil,
			input:      map[string]any{"name": "nonexistent"},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing name",
			setup:      nil,
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
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

			rec := doRequest(t, h, "DeletePipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInMemoryBackend_DeletePipeline_ClearsExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn func(t *testing.T)
		name    string
	}{
		{
			name: "executions removed on pipeline delete",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("del-exec-pl"), nil)
				require.NoError(t, err)

				_, err = b.StartPipelineExecution(context.Background(), "del-exec-pl")
				require.NoError(t, err)

				require.NoError(t, b.DeletePipeline(context.Background(), "del-exec-pl"))

				_, err = b.ListPipelineExecutions(context.Background(), "del-exec-pl")
				assert.Error(t, err, "should not find executions for deleted pipeline")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.checkFn(t)
		})
	}
}

// TestInMemoryBackend_DeletePipeline_ClearsActionExecutions verifies that
// deleting a pipeline also clears its recorded action-execution history, not
// just its pipeline-execution history. Without this, recreating a pipeline
// with the same name resurrected phantom ListActionExecutions entries from
// the deleted pipeline's earlier runs, since actionExecutions is keyed only
// by pipeline name.

func TestInMemoryBackend_DeletePipeline_ClearsActionExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn func(t *testing.T)
		name    string
	}{
		{
			name: "action executions removed on pipeline delete and do not leak into a recreated pipeline",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				decl := samplePipeline("del-ae-pl")

				_, err := b.CreatePipeline(context.Background(), decl, nil)
				require.NoError(t, err)

				_, err = b.StartPipelineExecution(context.Background(), "del-ae-pl")
				require.NoError(t, err)

				h := codepipeline.NewHandler(b)
				rec := doRequest(t, h, "ListActionExecutions", map[string]any{"pipelineName": "del-ae-pl"})
				require.Equal(t, http.StatusOK, rec.Code)

				var before map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &before))
				beforeDetails, _ := before["actionExecutionDetails"].([]any)
				require.NotEmpty(t, beforeDetails, "action executions must be recorded before delete")

				require.NoError(t, b.DeletePipeline(context.Background(), "del-ae-pl"))

				// Recreate a pipeline with the same name; its action-execution
				// history must start clean, not inherit the deleted pipeline's.
				_, err = b.CreatePipeline(context.Background(), decl, nil)
				require.NoError(t, err)

				rec = doRequest(t, h, "ListActionExecutions", map[string]any{"pipelineName": "del-ae-pl"})
				require.Equal(t, http.StatusOK, rec.Code)

				var after map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &after))
				afterDetails, _ := after["actionExecutionDetails"].([]any)
				assert.Empty(
					t,
					afterDetails,
					"recreated pipeline must not inherit the deleted pipeline's action executions",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.checkFn(t)
		})
	}
}

// --------------------------------------------------------------------------
// GetPipelineState includes latestExecution in actionStates
// --------------------------------------------------------------------------

func TestDeletePipeline_CascadeStageTransitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("cascade-pl"), nil)
	require.NoError(t, err)

	// Disable a stage transition.
	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"pipelineName":   "cascade-pl",
		"stageName":      "Source",
		"transitionType": "Inbound",
		"reason":         "testing cascade",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.StageTransitionCount())

	// Delete the pipeline.
	rec = doRequest(t, h, "DeletePipeline", map[string]any{"name": "cascade-pl"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Transition should also be gone.
	assert.Equal(t, 0, h.Backend.StageTransitionCount())
	assert.Equal(t, 0, h.Backend.PipelineCount())
}

// TestDeletePipeline_CascadeActionRevisions verifies PutActionRevision's
// tracked revision (surfaced via GetPipelineState's actionStates[].
// currentRevision) does not leak into a same-named pipeline recreated after
// a delete -- the same class of stale-data bug fixed for actionExecutions in
// a prior audit pass (see TestInMemoryBackend_DeletePipeline_ClearsActionExecutions),
// now also covering the actionRevisions store this pass added.
func TestDeletePipeline_CascadeActionRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	decl := samplePipeline("cascade-rev-pl")

	_, err := h.Backend.CreatePipeline(context.Background(), decl, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "PutActionRevision", map[string]any{
		"pipelineName": "cascade-rev-pl", "stageName": "Source", "actionName": "SourceAction",
		"actionRevision": map[string]any{"revisionId": "r1", "revisionChangeId": "c1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, h.Backend.DeletePipeline(context.Background(), "cascade-rev-pl"))

	// Recreate a pipeline with the same name; its tracked revision must
	// start clean, not inherit the deleted pipeline's.
	_, err = h.Backend.CreatePipeline(context.Background(), decl, nil)
	require.NoError(t, err)

	stateRec := doRequest(t, h, "GetPipelineState", map[string]any{"name": "cascade-rev-pl"})
	require.Equal(t, http.StatusOK, stateRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(stateRec.Body.Bytes(), &out))
	stageStates, _ := out["stageStates"].([]any)
	require.Len(t, stageStates, 1)
	stage, _ := stageStates[0].(map[string]any)
	actionStates, _ := stage["actionStates"].([]any)
	require.Len(t, actionStates, 1)
	action, _ := actionStates[0].(map[string]any)
	assert.Nil(
		t,
		action["currentRevision"],
		"recreated pipeline must not inherit the deleted pipeline's action revision",
	)
}

func TestHandler_ListPipelines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "with pipelines",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("pipeline-1"), nil)
				require.NoError(t, err)
				_, err = h.Backend.CreatePipeline(context.Background(), samplePipeline("pipeline-2"), nil)
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty",
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

			rec := doRequest(t, h, "ListPipelines", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			pipelines, _ := out["pipelines"].([]any)
			assert.Len(t, pipelines, tt.wantCount)
		})
	}
}

func TestHandler_ListPipelines_IncludesPipelineType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		name       string
		wantType   string
		wantMode   string
		wantStatus int
	}{
		{
			name: "V2 PARALLEL reflected in list",
			setup: func(h *codepipeline.Handler) {
				p := samplePipeline("v2-list-pl")
				p.PipelineType = codepipeline.PipelineTypeV2
				p.ExecutionMode = codepipeline.ExecutionModeParallel
				_, err := h.Backend.CreatePipeline(context.Background(), p, nil)
				require.NoError(t, err)
			},
			wantType:   "V2",
			wantMode:   "PARALLEL",
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

			rec := doRequest(t, h, "ListPipelines", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			pipelines, _ := out["pipelines"].([]any)
			require.Len(t, pipelines, 1)

			pl0, _ := pipelines[0].(map[string]any)
			assert.Equal(t, tt.wantType, pl0["pipelineType"])
			assert.Equal(t, tt.wantMode, pl0["executionMode"])
		})
	}
}

// --------------------------------------------------------------------------
// PutJobSuccessResult / PutJobFailureResult update job status
// --------------------------------------------------------------------------

func TestSortedListPipelines(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra-pl", "apple-pl", "mango-pl"} {
		_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline(name), nil)
		require.NoError(t, err)
	}

	rec := doRequest(t, h, "ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["pipelines"].([]any)
	require.True(t, ok)
	require.Len(t, list, 3)

	names := make([]string, len(list))
	for i, item := range list {
		m := item.(map[string]any)
		names[i] = m["name"].(string)
	}

	assert.Equal(t, []string{"apple-pl", "mango-pl", "zebra-pl"}, names)
}

func TestListPipelines_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// pipelines key must exist and be an array, not null.
	list, ok := out["pipelines"]
	require.True(t, ok)
	assert.NotNil(t, list)
}

func TestListPipelines_IncludesARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("arn-pl"), nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list := out["pipelines"].([]any)
	item := list[0].(map[string]any)
	arn, ok := item["pipelineArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:codepipeline")
	assert.Contains(t, arn, "arn-pl")
}

func TestListPipelines_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pipe-a", "pipe-b", "pipe-c", "pipe-d"} {
		rec := doRequest(t, h, "CreatePipeline", map[string]any{
			"pipeline": samplePipeline(name),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"maxResults": int32(2)},
			wantLen:       2,
			wantNextToken: true,
		},
		{
			name:          "over_cap_rejected",
			body:          map[string]any{"maxResults": int32(1001)},
			wantLen:       0,
			wantNextToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "ListPipelines", tt.body)
			if tt.name == "over_cap_rejected" {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}
			require.Equal(t, http.StatusOK, rec.Code)
			var out struct {
				NextToken string           `json:"nextToken"`
				Pipelines []map[string]any `json:"pipelines"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Pipelines, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListPipelines_FullPagination walks all pages and collects all pipelines.

func TestListPipelines_FullPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const total = 5
	for i := range total {
		names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		rec := doRequest(t, h, "CreatePipeline", map[string]any{
			"pipeline": samplePipeline(names[i]),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := map[string]any{"maxResults": int32(2)}
		if token != "" {
			body["nextToken"] = token
		}

		rec := doRequest(t, h, "ListPipelines", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken string           `json:"nextToken"`
			Pipelines []map[string]any `json:"pipelines"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.LessOrEqual(t, len(out.Pipelines), 2)

		for _, p := range out.Pipelines {
			name := p["name"].(string)
			assert.False(t, seen[name], "pipeline %s seen twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		token = out.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total)
	assert.GreaterOrEqual(t, pages, 3)
}

// TestParity_GetPipelineState_PipelineVersion verifies pipelineVersion is returned.

func TestInMemoryBackend_DeepCopy(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	decl := samplePipeline("deep-copy-pipeline")
	decl.Stages[0].Actions[0].Configuration = map[string]string{"key": "original"}

	p, err := backend.CreatePipeline(context.Background(), decl, nil)
	require.NoError(t, err)

	// Mutate the returned pipeline's nested data.
	p.Declaration.Stages[0].Actions[0].Configuration["key"] = "mutated"

	// The backend should still have the original value.
	stored, err := backend.GetPipeline(context.Background(), "deep-copy-pipeline")
	require.NoError(t, err)
	assert.Equal(t, "original", stored.Declaration.Stages[0].Actions[0].Configuration["key"])
}
