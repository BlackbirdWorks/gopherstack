package codepipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

type exportingRunner struct {
	vars     map[string]string
	projects []string
	mu       sync.Mutex
}

func (r *exportingRunner) StartBuild(context.Context, string) error { return nil }

func (r *exportingRunner) RunBuild(_ context.Context, req codepipeline.BuildRequest) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.projects = append(r.projects, req.ProjectName)

	return "build:" + req.ProjectName, nil
}

func (r *exportingRunner) BuildStatus(context.Context, string) (string, error) { return "SUCCEEDED", nil }

func (r *exportingRunner) BuildExportedVariables(context.Context, string) (map[string]string, error) {
	return r.vars, nil
}

func (r *exportingRunner) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.projects...)
}

func codeBuildStage(stage, action, ns, project string) codepipeline.Stage {
	return codepipeline.Stage{Name: stage, Actions: []codepipeline.Action{{
		Name:          action,
		Namespace:     ns,
		ActionTypeID:  codepipeline.ActionTypeID{Category: "Build", Owner: "AWS", Provider: "CodeBuild", Version: "1"},
		Configuration: map[string]string{"ProjectName": project},
	}}}
}

func TestCodeBuildExportedVariablesFlowIntoNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vars         map[string]string
		name         string
		namespace    string
		downstream   string
		wantProject  string
		wantOutputs  map[string]string
	}{
		{
			name: "namespaced exported variable resolves downstream", namespace: "BuildVars",
			vars: map[string]string{"OUT": "v1"}, downstream: "next-#{BuildVars.OUT}",
			wantProject: "next-v1", wantOutputs: map[string]string{"OUT": "v1"},
		},
		{
			name: "no namespace leaves reference verbatim", namespace: "",
			vars: map[string]string{"OUT": "v1"}, downstream: "next-#{BuildVars.OUT}",
			wantProject: "next-#{BuildVars.OUT}", wantOutputs: map[string]string{"OUT": "v1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			runner := &exportingRunner{vars: tt.vars}
			h.Backend.SetCodeBuildBackend(runner)

			ctx := t.Context()
			decl := codepipeline.PipelineDeclaration{
				Name:          "p",
				RoleArn:       "arn:aws:iam::000000000000:role/p",
				ArtifactStore: codepipeline.ArtifactStore{Type: "S3", Location: "art"},
				Stages: []codepipeline.Stage{
					codeBuildStage("One", "B1", tt.namespace, "first"),
					codeBuildStage("Two", "B2", "", tt.downstream),
				},
			}
			_, err := h.Backend.CreatePipeline(ctx, decl, nil)
			require.NoError(t, err)

			exec, err := h.Backend.StartPipelineExecution(ctx, "p")
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				got, gErr := h.Backend.GetPipelineExecution(ctx, "p", exec.PipelineExecutionID)

				return gErr == nil && got.Status == "Succeeded"
			}, 10*time.Second, 20*time.Millisecond)

			assert.Equal(t, []string{"first", tt.wantProject}, runner.seen())

			details, err := h.Backend.ListActionExecutions(ctx, "p", exec.PipelineExecutionID)
			require.NoError(t, err)

			found := false

			for _, d := range details {
				if d["actionName"] == "B1" {
					found = true

					out, _ := d["output"].(map[string]any)
					assert.Equal(t, tt.wantOutputs, out["outputVariables"])
				}
			}

			assert.True(t, found)
		})
	}
}
