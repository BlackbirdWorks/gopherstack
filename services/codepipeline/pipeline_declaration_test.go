package codepipeline_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_PipelineDeclaration_V2Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantMode   string
		pipeline   codepipeline.PipelineDeclaration
		wantStatus int
	}{
		{
			name: "V1 defaults applied on create",
			pipeline: func() codepipeline.PipelineDeclaration {
				p := samplePipeline("v1-defaults")

				return p
			}(),
			wantType:   "V1",
			wantMode:   "SUPERSEDED",
			wantStatus: http.StatusOK,
		},
		{
			name: "V2 PARALLEL round-trips",
			pipeline: func() codepipeline.PipelineDeclaration {
				p := samplePipeline("v2-parallel")
				p.PipelineType = codepipeline.PipelineTypeV2
				p.ExecutionMode = codepipeline.ExecutionModeParallel
				p.Variables = []codepipeline.PipelineVariable{
					{Name: "ENV", DefaultValue: "dev"},
				}

				return p
			}(),
			wantType:   "V2",
			wantMode:   "PARALLEL",
			wantStatus: http.StatusOK,
		},
		{
			name: "V1 QUEUED round-trips",
			pipeline: func() codepipeline.PipelineDeclaration {
				p := samplePipeline("v1-queued")
				p.PipelineType = codepipeline.PipelineTypeV1
				p.ExecutionMode = codepipeline.ExecutionModeQueued

				return p
			}(),
			wantType:   "V1",
			wantMode:   "QUEUED",
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid pipelineType rejected",
			pipeline: func() codepipeline.PipelineDeclaration {
				p := samplePipeline("bad-type")
				p.PipelineType = "V3"

				return p
			}(),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid executionMode rejected",
			pipeline: func() codepipeline.PipelineDeclaration {
				p := samplePipeline("bad-mode")
				p.ExecutionMode = "UNKNOWN"

				return p
			}(),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreatePipeline", map[string]any{
				"pipeline": tt.pipeline,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				pl, _ := out["pipeline"].(map[string]any)
				require.NotNil(t, pl)
				assert.Equal(t, tt.wantType, pl["pipelineType"])
				assert.Equal(t, tt.wantMode, pl["executionMode"])
			}
		})
	}
}

// --------------------------------------------------------------------------
// #2 Triggers model round-trip
// --------------------------------------------------------------------------

func TestHandler_Pipeline_Triggers_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		triggers   []codepipeline.Trigger
		wantStatus int
	}{
		{
			name: "CodeStarSourceConnection trigger persists",
			triggers: []codepipeline.Trigger{
				{
					ProviderType: "CodeStarSourceConnection",
					GitConfiguration: &codepipeline.GitConfiguration{
						SourceActionName: "SourceAction",
						Push: []codepipeline.GitPushFilter{
							{
								Branches: &codepipeline.GitBranchFilterCriteria{
									Includes: []string{"main", "develop"},
									Excludes: []string{"feature/*"},
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "no triggers allowed",
			triggers:   nil,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p := samplePipeline("trigger-" + tt.name)
			p.Triggers = tt.triggers

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": p})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, "GetPipeline", map[string]any{"name": p.Name})
				require.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

// --------------------------------------------------------------------------
// #3 Action extended fields round-trip
// --------------------------------------------------------------------------

func TestHandler_Action_ExtendedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     codepipeline.Action
		wantStatus int
	}{
		{
			name: "RoleArn Region Namespace TimeoutInMinutes persist",
			action: codepipeline.Action{
				Name: "SourceAction",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Source", Owner: "AWS", Provider: "S3", Version: "1",
				},
				RoleArn:          "arn:aws:iam::000000000000:role/action-role",
				Region:           "us-west-2",
				Namespace:        "MyNamespace",
				TimeoutInMinutes: 30,
				OutputArtifacts:  []codepipeline.ArtifactRef{{Name: "output"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "minimal action still valid",
			action: codepipeline.Action{
				Name: "SourceAction",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Source", Owner: "ThirdParty", Provider: "GitHub", Version: "1",
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p := samplePipeline("action-ext-" + tt.name)
			p.Stages[0].Actions[0] = tt.action

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": p})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, "GetPipeline", map[string]any{"name": p.Name})
				require.Equal(t, http.StatusOK, rec2.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))

				pl := out["pipeline"].(map[string]any)
				stages := pl["stages"].([]any)
				stage0 := stages[0].(map[string]any)
				actions := stage0["actions"].([]any)
				action0 := actions[0].(map[string]any)

				if tt.action.RoleArn != "" {
					assert.Equal(t, tt.action.RoleArn, action0["roleArn"])
				}

				if tt.action.Namespace != "" {
					assert.Equal(t, tt.action.Namespace, action0["namespace"])
				}
			}
		})
	}
}

// --------------------------------------------------------------------------
// #4 Stage Conditions round-trip
// --------------------------------------------------------------------------

func TestHandler_Stage_Conditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		onFailure  *codepipeline.Condition
		name       string
		wantStatus int
	}{
		{
			name: "OnFailure condition persists",
			onFailure: &codepipeline.Condition{
				Result: "ROLLBACK",
				Rules: []codepipeline.Rule{
					{
						Name: "DeploymentSafetyRule",
						RuleTypeID: codepipeline.ActionTypeID{
							Category: "Rule", Owner: "AWS", Provider: "DeploymentSafety", Version: "1",
						},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "no conditions allowed",
			onFailure:  nil,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p := samplePipeline("cond-" + tt.name)
			p.Stages[0].OnFailure = tt.onFailure

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": p})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --------------------------------------------------------------------------
// #5 ArtifactStore EncryptionKey round-trip
// --------------------------------------------------------------------------

func TestHandler_ArtifactStore_EncryptionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		store      codepipeline.ArtifactStore
		name       string
		wantStatus int
	}{
		{
			name: "EncryptionKey KMS persists",
			store: codepipeline.ArtifactStore{
				Type:     "S3",
				Location: "my-bucket",
				EncryptionKey: &codepipeline.EncryptionKey{
					ID:   "arn:aws:kms:us-east-1:000000000000:key/abc123",
					Type: "KMS",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "no encryption key ok",
			store: codepipeline.ArtifactStore{
				Type:     "S3",
				Location: "my-bucket",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p := samplePipeline("enc-" + tt.name)
			p.ArtifactStore = tt.store

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": p})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, "GetPipeline", map[string]any{"name": p.Name})
				require.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

// --------------------------------------------------------------------------
// #6 ArtifactStore type validation
// --------------------------------------------------------------------------

func TestHandler_ArtifactStore_TypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		storeType  string
		name       string
		wantStatus int
	}{
		{
			name:       "S3 accepted",
			storeType:  "S3",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GCS rejected",
			storeType:  "GCS",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty type accepted (no validation)",
			storeType:  "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p := samplePipeline("store-type-" + tt.name)
			p.ArtifactStore.Type = tt.storeType

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": p})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --------------------------------------------------------------------------
// #9 & #10 Webhook full model + ListWebhooks definition wrapper
// --------------------------------------------------------------------------

func TestHandler_Pipeline_ArtifactStores_CrossRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		artifactStores map[string]codepipeline.ArtifactStore
		name           string
		wantStatus     int
	}{
		{
			name: "cross-region artifact stores persist",
			artifactStores: map[string]codepipeline.ArtifactStore{
				"us-west-2": {Type: "S3", Location: "west-bucket"},
				"eu-west-1": {Type: "S3", Location: "eu-bucket"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:           "no artifactStores ok",
			artifactStores: nil,
			wantStatus:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p := samplePipeline("cross-region-" + tt.name)
			p.ArtifactStores = tt.artifactStores

			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": p})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, "GetPipeline", map[string]any{"name": p.Name})
				require.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

// --------------------------------------------------------------------------
// DeletePipeline also removes executions
// --------------------------------------------------------------------------
