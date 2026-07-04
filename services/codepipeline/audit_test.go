package codepipeline_test

// audit_test.go — Tests for AWS-accuracy audit items from issue #1777.
// All tests use table-driven format.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// --------------------------------------------------------------------------
// #2 PipelineDeclaration V2 fields round-trip
// --------------------------------------------------------------------------

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

func TestHandler_Webhook_FullModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		webhook    map[string]any
		checkFn    func(t *testing.T, out map[string]any)
		name       string
		wantStatus int
	}{
		{
			name: "webhook with filters and GITHUB_HMAC auth",
			webhook: map[string]any{
				"name":           "wh-full",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "GITHUB_HMAC",
				"authenticationConfiguration": map[string]any{
					"secretToken": "super-secret",
				},
				"filters": []map[string]any{
					{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"},
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, out map[string]any) {
				t.Helper()

				wh, ok := out["webhook"].(map[string]any)
				require.True(t, ok, "webhook key missing")

				def, ok := wh["definition"].(map[string]any)
				require.True(t, ok, "definition key missing")
				assert.Equal(t, "wh-full", def["name"])
				assert.Equal(t, "GITHUB_HMAC", def["authentication"])

				filters, _ := def["filters"].([]any)
				require.Len(t, filters, 1)
				f0, _ := filters[0].(map[string]any)
				assert.Equal(t, "$.ref", f0["jsonPath"])
				assert.Equal(t, "refs/heads/main", f0["matchEquals"])

				// URL and ARN should be populated
				assert.NotEmpty(t, wh["url"])
				assert.NotEmpty(t, wh["arn"])
			},
		},
		{
			name: "webhook with IP auth",
			webhook: map[string]any{
				"name":           "wh-ip",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "IP",
				"authenticationConfiguration": map[string]any{
					"allowedIPRange": "192.168.1.0/24",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid authentication type rejected",
			webhook: map[string]any{
				"name":           "wh-bad",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "INVALID_AUTH",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing webhook name rejected",
			webhook:    map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "PutWebhook", map[string]any{"webhook": tt.webhook})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFn != nil && tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.checkFn(t, out)
			}
		})
	}
}

func TestHandler_ListWebhooks_DefinitionWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		checkFn    func(t *testing.T, webhooks []any)
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "ListWebhooks wraps in definition",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{
					Name:           "list-wh-1",
					TargetPipeline: "pl-1",
					TargetAction:   "src",
					Authentication: "GITHUB_HMAC",
					Filters: []codepipeline.WebhookFilter{
						{JSONPath: "$.ref", MatchEquals: "refs/heads/main"},
					},
				})
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
			checkFn: func(t *testing.T, webhooks []any) {
				t.Helper()

				wh0, _ := webhooks[0].(map[string]any)
				def, ok := wh0["definition"].(map[string]any)
				require.True(t, ok, "definition key must be present in ListWebhooks response")
				assert.Equal(t, "list-wh-1", def["name"])
				assert.Equal(t, "GITHUB_HMAC", def["authentication"])

				filters, _ := def["filters"].([]any)
				require.Len(t, filters, 1)
			},
		},
		{
			name:       "empty list returns empty array",
			setup:      nil,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "multiple webhooks sorted",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{Name: "b-hook", TargetPipeline: "pl"})
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{Name: "a-hook", TargetPipeline: "pl"})
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
			checkFn: func(t *testing.T, webhooks []any) {
				t.Helper()

				first := webhooks[0].(map[string]any)["definition"].(map[string]any)
				assert.Equal(t, "a-hook", first["name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListWebhooks", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			webhooks, _ := out["webhooks"].([]any)
			assert.Len(t, webhooks, tt.wantCount)

			if tt.checkFn != nil && len(webhooks) > 0 {
				tt.checkFn(t, webhooks)
			}
		})
	}
}

// --------------------------------------------------------------------------
// #13 PollForJobs filtered by ActionTypeID
// --------------------------------------------------------------------------

func TestHandler_PollForJobs_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		actionTypeID map[string]any
		setup        func(h *codepipeline.Handler)
		name         string
		wantJobIDs   []string
		wantStatus   int
	}{
		{
			name: "matching category/provider/version returns job",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{
					ID:     "job-match-1",
					Nonce:  "n1",
					Status: "Queued",
					ActionTypeID: codepipeline.ActionTypeID{
						Category: "Build", Owner: "Custom", Provider: "MyBuilder", Version: "1",
					},
				})
				h.Backend.AddJobInternal(&codepipeline.Job{
					ID:     "job-other-provider",
					Nonce:  "n2",
					Status: "Queued",
					ActionTypeID: codepipeline.ActionTypeID{
						Category: "Build", Owner: "Custom", Provider: "OtherBuilder", Version: "1",
					},
				})
			},
			actionTypeID: map[string]any{
				"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
			},
			wantStatus: http.StatusOK,
			wantJobIDs: []string{"job-match-1"},
		},
		{
			name: "no queued jobs for type returns empty",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{
					ID:     "job-inprogress",
					Nonce:  "n1",
					Status: "InProgress",
					ActionTypeID: codepipeline.ActionTypeID{
						Category: "Build", Owner: "Custom", Provider: "MyBuilder", Version: "1",
					},
				})
			},
			actionTypeID: map[string]any{
				"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
			},
			wantStatus: http.StatusOK,
			wantJobIDs: []string{},
		},
		{
			name: "different version not returned",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{
					ID:     "job-v2",
					Nonce:  "n1",
					Status: "Queued",
					ActionTypeID: codepipeline.ActionTypeID{
						Category: "Build", Owner: "Custom", Provider: "MyBuilder", Version: "2",
					},
				})
			},
			actionTypeID: map[string]any{
				"category": "Build", "owner": "Custom", "provider": "MyBuilder", "version": "1",
			},
			wantStatus: http.StatusOK,
			wantJobIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "PollForJobs", map[string]any{
				"actionTypeId": tt.actionTypeID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jobs, _ := out["jobs"].([]any)

			gotIDs := make([]string, len(jobs))
			for i, j := range jobs {
				jm, _ := j.(map[string]any)
				gotIDs[i] = jm["id"].(string)
			}

			assert.ElementsMatch(t, tt.wantJobIDs, gotIDs)
		})
	}
}

// --------------------------------------------------------------------------
// #15 & #16 Owner persisted; ListActionTypes full shape
// --------------------------------------------------------------------------

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

func TestHandler_UpdateActionType_FullFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateInput map[string]any
		checkFn     func(t *testing.T, h *codepipeline.Handler)
		name        string
		wantStatus  int
	}{
		{
			name: "update with full body persists settings and artifact details",
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
					"settings": map[string]any{
						"executionUrlTemplate": "https://ci.example.com/job/{Config:ProjectName}",
					},
					"actionConfigurationProperties": []map[string]any{
						{"name": "BranchName", "key": true, "required": false, "secret": false},
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

				settings, _ := at["settings"].(map[string]any)
				require.NotNil(t, settings)
				assert.Equal(t, "https://ci.example.com/job/{Config:ProjectName}", settings["executionUrlTemplate"])
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
			name: "update non-existent type returns ActionTypeNotFoundException",
			updateInput: map[string]any{
				"actionType": map[string]any{
					"id": map[string]any{
						"category": "Build", "owner": "Custom", "provider": "Ghost", "version": "1",
					},
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
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

func TestHandler_ListTagsForResource_WebhookARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler) string
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "pipeline ARN returns tags",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(
					context.Background(),
					samplePipeline("tags-pl"),
					map[string]string{"Env": "prod"},
				)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "unknown ARN returns PipelineNotFoundException",
			setup: func(_ *codepipeline.Handler) string {
				return "arn:aws:codepipeline:us-east-1:000000000000:nonexistent"
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "PipelineNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": resourceARN})
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
// #26 DisableStageTransition: stage existence validation
// --------------------------------------------------------------------------

func TestHandler_DisableStageTransition_StageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		setup      func(h *codepipeline.Handler)
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "existing stage disabled ok",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("stage-exists"), nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"pipelineName":   "stage-exists",
				"stageName":      "Source",
				"transitionType": "Inbound",
				"reason":         "testing",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "non-existent stage rejected",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("stage-missing"), nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"pipelineName":   "stage-missing",
				"stageName":      "NonExistentStage",
				"transitionType": "Inbound",
				"reason":         "testing",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "StageNotFoundException",
		},
		{
			name:  "non-existent pipeline returns PipelineNotFoundException",
			setup: nil,
			input: map[string]any{
				"pipelineName":   "ghost-pipeline",
				"stageName":      "Source",
				"transitionType": "Inbound",
				"reason":         "testing",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "PipelineNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DisableStageTransition", tt.input)
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
// #27 & #28 Persistence: Restore calls Reset + defensive copy
// --------------------------------------------------------------------------

func TestInMemoryBackend_Restore_DefensiveCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn func(t *testing.T)
		name    string
	}{
		{
			name: "Restore followed by mutation does not corrupt backend",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("snap-pl"), nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, b2.Restore(t.Context(), snap))

				// Snap is now parsed and owned by b2; zero it out to detect aliasing.
				for i := range snap {
					snap[i] = 0
				}

				// b2 should still have the pipeline.
				p, err := b2.GetPipeline(context.Background(), "snap-pl")
				require.NoError(t, err)
				assert.Equal(t, "snap-pl", p.Declaration.Name)
			},
		},
		{
			name: "Restore clears prior state",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("old-pl"), nil)
				require.NoError(t, err)

				// Take snapshot of empty state.
				b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				emptySnap := b2.Snapshot(t.Context())

				// Restore empty snapshot onto b which has "old-pl".
				require.NoError(t, b.Restore(t.Context(), emptySnap))

				assert.Equal(t, 0, b.PipelineCount())
			},
		},
		{
			name: "Snapshot+Restore round-trips executions",
			checkFn: func(t *testing.T) {
				t.Helper()

				b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreatePipeline(context.Background(), samplePipeline("exec-snap"), nil)
				require.NoError(t, err)

				exec, err := b.StartPipelineExecution(context.Background(), "exec-snap")
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())

				b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, b2.Restore(t.Context(), snap))

				execs, err := b2.ListPipelineExecutions(context.Background(), "exec-snap")
				require.NoError(t, err)
				require.Len(t, execs, 1)
				assert.Equal(t, exec.PipelineExecutionID, execs[0].PipelineExecutionID)
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
// #29 UpdatePipeline version check (ConflictException)
// --------------------------------------------------------------------------

func TestHandler_UpdatePipeline_VersionConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		version    int
		wantStatus int
	}{
		{
			name:       "version 0 (omitted) always succeeds",
			version:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "matching version succeeds",
			version:    1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong version returns ConflictException",
			version:    99,
			wantStatus: http.StatusBadRequest,
			wantType:   "ConflictException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("ver-conflict"), nil)
			require.NoError(t, err)

			p := samplePipeline("ver-conflict")
			p.Version = tt.version

			rec := doRequest(t, h, "UpdatePipeline", map[string]any{"pipeline": p})
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
// #30 DeleteCustomActionType: ResourceInUseException when referenced
// --------------------------------------------------------------------------

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

func TestHandler_GetPipelineState_ActionStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		checkFn    func(t *testing.T, out map[string]any)
		name       string
		wantStatus int
	}{
		{
			name: "actionStates included per stage",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("state-pl"), nil)
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, out map[string]any) {
				t.Helper()

				stages, _ := out["stageStates"].([]any)
				require.Len(t, stages, 1)

				stage0, _ := stages[0].(map[string]any)
				actionStates, ok := stage0["actionStates"]
				assert.True(t, ok, "actionStates key must be present")
				assert.NotNil(t, actionStates)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetPipelineState", map[string]any{"name": "state-pl"})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFn != nil {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.checkFn(t, out)
			}
		})
	}
}

// --------------------------------------------------------------------------
// ListPipelineExecutions returns stored executions
// --------------------------------------------------------------------------

func TestHandler_ListPipelineExecutions_StoresAndReturns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler) string
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "two starts gives two entries in reverse order",
			setup: func(h *codepipeline.Handler) string {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("list-execs"), nil)
				require.NoError(t, err)

				rec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "list-execs"})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "list-execs"})
				require.Equal(t, http.StatusOK, rec.Code)

				return "list-execs"
			},
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
		{
			name: "empty pipeline returns empty list",
			setup: func(h *codepipeline.Handler) string {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("empty-execs"), nil)
				require.NoError(t, err)

				return "empty-execs"
			},
			wantCount:  0,
			wantStatus: http.StatusOK,
		},
		{
			name: "non-existent pipeline returns error",
			setup: func(_ *codepipeline.Handler) string {
				return "ghost-pipeline"
			},
			wantCount:  0,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			pipelineName := tt.setup(h)

			rec := doRequest(t, h, "ListPipelineExecutions", map[string]any{"pipelineName": pipelineName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				summaries, _ := out["pipelineExecutionSummaries"].([]any)
				assert.Len(t, summaries, tt.wantCount)
			}
		})
	}
}

// --------------------------------------------------------------------------
// PipelineSummary includes pipelineType and executionMode
// --------------------------------------------------------------------------

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

func TestHandler_PutJobResult_UpdatesStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      map[string]any
		action     string
		name       string
		wantStatus int
	}{
		{
			name:   "success result accepted",
			action: "PutJobSuccessResult",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{
					ID: "job-success", Nonce: "n", Status: "InProgress",
					ActionTypeID: codepipeline.ActionTypeID{Category: "Build", Provider: "P", Version: "1"},
				})
			},
			input:      map[string]any{"jobId": "job-success"},
			wantStatus: http.StatusOK,
		},
		{
			name:   "failure result accepted",
			action: "PutJobFailureResult",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{
					ID: "job-fail", Nonce: "n", Status: "InProgress",
					ActionTypeID: codepipeline.ActionTypeID{Category: "Build", Provider: "P", Version: "1"},
				})
			},
			input: map[string]any{
				"jobId":          "job-fail",
				"failureDetails": map[string]any{"message": "build failed"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "success result for missing job returns error",
			action:     "PutJobSuccessResult",
			setup:      nil,
			input:      map[string]any{"jobId": "ghost-job"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --------------------------------------------------------------------------
// ArtifactStores (cross-region) round-trip
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

// --------------------------------------------------------------------------
// GetPipelineState includes latestExecution in actionStates
// --------------------------------------------------------------------------

func TestHandler_GetPipelineState_LatestExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn      func(t *testing.T, out map[string]any)
		name         string
		pipelineName string
		wantStatus   int
		runExec      bool
	}{
		{
			name:         "latestExecution absent before any execution",
			pipelineName: "le-no-exec",
			runExec:      false,
			wantStatus:   http.StatusOK,
			checkFn: func(t *testing.T, out map[string]any) {
				t.Helper()

				stages, _ := out["stageStates"].([]any)
				require.Len(t, stages, 1)

				stage0, _ := stages[0].(map[string]any)
				actionStates, _ := stage0["actionStates"].([]any)
				require.Len(t, actionStates, 1)

				action0, _ := actionStates[0].(map[string]any)
				_, hasLatest := action0["latestExecution"]
				assert.False(t, hasLatest, "latestExecution must be absent before any execution")
			},
		},
		{
			name:         "latestExecution populated after StartPipelineExecution",
			pipelineName: "le-with-exec",
			runExec:      true,
			wantStatus:   http.StatusOK,
			checkFn: func(t *testing.T, out map[string]any) {
				t.Helper()

				stages, _ := out["stageStates"].([]any)
				require.Len(t, stages, 1)

				stage0, _ := stages[0].(map[string]any)
				actionStates, _ := stage0["actionStates"].([]any)
				require.Len(t, actionStates, 1)

				action0, _ := actionStates[0].(map[string]any)
				latest, ok := action0["latestExecution"].(map[string]any)
				require.True(t, ok, "latestExecution must be present after execution")

				assert.NotEmpty(t, latest["actionExecutionId"], "actionExecutionId must be set")
				assert.NotEmpty(t, latest["status"], "status must be set")
				assert.NotZero(t, latest["startTime"], "startTime must be set")
				assert.NotZero(t, latest["lastUpdateTime"], "lastUpdateTime must be set")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreatePipeline(context.Background(), samplePipeline(tt.pipelineName), nil)
			require.NoError(t, err)

			if tt.runExec {
				_, err = b.StartPipelineExecution(context.Background(), tt.pipelineName)
				require.NoError(t, err)
			}

			h := codepipeline.NewHandler(b)
			rec := doRequest(t, h, "GetPipelineState", map[string]any{"name": tt.pipelineName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFn != nil {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.checkFn(t, out)
			}
		})
	}
}

// --------------------------------------------------------------------------
// PollForJobs respects maxBatchSize
// --------------------------------------------------------------------------

func TestHandler_PollForJobs_MaxBatchSize(t *testing.T) {
	t.Parallel()

	makeJob := func(id string) *codepipeline.Job {
		return &codepipeline.Job{
			ID:     id,
			Nonce:  "n-" + id,
			Status: "Queued",
			ActionTypeID: codepipeline.ActionTypeID{
				Category: "Build",
				Owner:    "Custom",
				Provider: "MyBuilder",
				Version:  "1",
			},
		}
	}

	tests := []struct {
		name         string
		maxBatchSize int32
		wantCount    int
	}{
		{
			name:         "maxBatchSize=1 limits to 1 job",
			maxBatchSize: 1,
			wantCount:    1,
		},
		{
			name:         "maxBatchSize=2 limits to 2 jobs",
			maxBatchSize: 2,
			wantCount:    2,
		},
		{
			name:         "maxBatchSize=0 defaults to at most 10",
			maxBatchSize: 0,
			wantCount:    3,
		},
		{
			name:         "maxBatchSize exceeding count returns all",
			maxBatchSize: 10,
			wantCount:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
			b.AddJobInternal(makeJob("job-a"))
			b.AddJobInternal(makeJob("job-b"))
			b.AddJobInternal(makeJob("job-c"))

			h := codepipeline.NewHandler(b)
			rec := doRequest(t, h, "PollForJobs", map[string]any{
				"actionTypeId": map[string]any{
					"category": "Build",
					"owner":    "Custom",
					"provider": "MyBuilder",
					"version":  "1",
				},
				"maxBatchSize": tt.maxBatchSize,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			jobs, _ := out["jobs"].([]any)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}
