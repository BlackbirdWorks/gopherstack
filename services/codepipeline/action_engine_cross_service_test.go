package codepipeline_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

var errFakeBackend = errors.New("fake backend failure")

// fakeCodeBuildStarter is a minimal codepipeline.CodeBuildStarter double.
type fakeCodeBuildStarter struct {
	err error
}

func (f *fakeCodeBuildStarter) StartBuild(_ context.Context, _ string) error { return f.err }

// fakeLambdaInvoker is a minimal codepipeline.LambdaInvoker double.
type fakeLambdaInvoker struct {
	err error
}

func (f *fakeLambdaInvoker) InvokeFunction(
	_ context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	return nil, 200, f.err
}

// fakeCodeDeployStarter is a minimal codepipeline.CodeDeployStarter double.
type fakeCodeDeployStarter struct {
	err error
}

func (f *fakeCodeDeployStarter) CreateDeployment(_ context.Context, _, _ string) error { return f.err }

// codeBuildActionPipeline returns a 2-stage pipeline (Source -> Build) whose
// Build stage is a single built-in Build/CodeBuild action configured with
// ProjectName.
func codeBuildActionPipeline(name, projectName string) codepipeline.PipelineDeclaration {
	p := samplePipeline(name)
	p.Stages = append(p.Stages, codepipeline.Stage{
		Name: "Build",
		Actions: []codepipeline.Action{
			{
				Name: "BuildAction",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Build",
					Owner:    "AWS",
					Provider: "CodeBuild",
					Version:  "1",
				},
				Configuration: map[string]string{"ProjectName": projectName},
			},
		},
	})

	return p
}

// lambdaActionPipeline returns a 2-stage pipeline (Source -> Invoke) whose
// Invoke stage is a single built-in Invoke/Lambda action configured with
// FunctionName.
func lambdaActionPipeline(name, functionName string) codepipeline.PipelineDeclaration {
	p := samplePipeline(name)
	p.Stages = append(p.Stages, codepipeline.Stage{
		Name: "Invoke",
		Actions: []codepipeline.Action{
			{
				Name: "InvokeAction",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Invoke",
					Owner:    "AWS",
					Provider: "Lambda",
					Version:  "1",
				},
				Configuration: map[string]string{"FunctionName": functionName},
			},
		},
	})

	return p
}

// TestRunOneAction_CodeBuild covers a Build/CodeBuild action's three
// reachable outcomes: unwired (today's instant-success behavior, unchanged),
// wired against a CodeBuild backend that accepts the build, and wired
// against one that rejects it (e.g. an unknown project). Before
// gopherstack-cb9l's fix, every case here reported Succeeded regardless.
func TestRunOneAction_CodeBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		codeBuild  codepipeline.CodeBuildStarter
		name       string
		wantStatus string
	}{
		{name: "unwired", codeBuild: nil, wantStatus: "Succeeded"},
		{name: "wired success", codeBuild: &fakeCodeBuildStarter{}, wantStatus: "Succeeded"},
		{name: "wired failure", codeBuild: &fakeCodeBuildStarter{err: errFakeBackend}, wantStatus: "Failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.codeBuild != nil {
				h.Backend.SetCodeBuildBackend(tt.codeBuild)
			}

			ctx := context.Background()
			p := codeBuildActionPipeline("cb-"+tt.name, "my-project")
			_, err := h.Backend.CreatePipeline(ctx, p, nil)
			require.NoError(t, err)

			exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, exec.Status)
		})
	}
}

// TestRunOneAction_Lambda covers an Invoke/Lambda action's three reachable
// outcomes, mirroring TestRunOneAction_CodeBuild: unwired (unchanged),
// wired against a Lambda backend that invokes successfully, and wired
// against one whose invocation errors (e.g. an unknown function). Before
// gopherstack-cb9l's fix, every case here reported Succeeded regardless.
func TestRunOneAction_Lambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lambda     codepipeline.LambdaInvoker
		name       string
		wantStatus string
	}{
		{name: "unwired", lambda: nil, wantStatus: "Succeeded"},
		{name: "wired success", lambda: &fakeLambdaInvoker{}, wantStatus: "Succeeded"},
		{name: "wired failure", lambda: &fakeLambdaInvoker{err: errFakeBackend}, wantStatus: "Failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.lambda != nil {
				h.Backend.SetLambdaBackend(tt.lambda)
			}

			ctx := context.Background()
			p := lambdaActionPipeline("lambda-"+tt.name, "my-function")
			_, err := h.Backend.CreatePipeline(ctx, p, nil)
			require.NoError(t, err)

			exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, exec.Status)
		})
	}
}

// codeDeployActionPipeline returns a 2-stage pipeline (Source -> Deploy)
// whose Deploy stage is a single built-in Deploy/CodeDeploy action
// configured with ApplicationName/DeploymentGroupName.
func codeDeployActionPipeline(name, appName, dgName string) codepipeline.PipelineDeclaration {
	p := samplePipeline(name)
	p.Stages = append(p.Stages, codepipeline.Stage{
		Name: "Deploy",
		Actions: []codepipeline.Action{
			{
				Name: "DeployAction",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Deploy",
					Owner:    "AWS",
					Provider: "CodeDeploy",
					Version:  "1",
				},
				Configuration: map[string]string{
					"ApplicationName":     appName,
					"DeploymentGroupName": dgName,
				},
			},
		},
	})

	return p
}

// TestRunOneAction_CodeDeploy covers a Deploy/CodeDeploy action's three
// reachable outcomes, mirroring TestRunOneAction_CodeBuild: unwired
// (unchanged), wired against a CodeDeploy backend that accepts the
// deployment, and wired against one that rejects it (e.g. an unknown
// application/deployment group). Before this fix, every case here reported
// Succeeded regardless.
func TestRunOneAction_CodeDeploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		codeDeploy codepipeline.CodeDeployStarter
		name       string
		wantStatus string
	}{
		{name: "unwired", codeDeploy: nil, wantStatus: "Succeeded"},
		{name: "wired success", codeDeploy: &fakeCodeDeployStarter{}, wantStatus: "Succeeded"},
		{name: "wired failure", codeDeploy: &fakeCodeDeployStarter{err: errFakeBackend}, wantStatus: "Failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.codeDeploy != nil {
				h.Backend.SetCodeDeployBackend(tt.codeDeploy)
			}

			ctx := context.Background()
			p := codeDeployActionPipeline("cd-"+tt.name, "my-app", "my-dg")
			_, err := h.Backend.CreatePipeline(ctx, p, nil)
			require.NoError(t, err)

			exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, exec.Status)
		})
	}
}

// TestRunOneAction_MissingConfiguration proves that a wired backend with no
// ProjectName/FunctionName configured on the action still succeeds instantly
// (nothing to call), rather than newly failing on a Configuration shape this
// backend does not require to be set.
func TestRunOneAction_MissingConfiguration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("codebuild", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		h.Backend.SetCodeBuildBackend(&fakeCodeBuildStarter{err: errFakeBackend})

		p := codeBuildActionPipeline("cb-no-config", "")
		_, err := h.Backend.CreatePipeline(ctx, p, nil)
		require.NoError(t, err)

		exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
		require.NoError(t, err)
		assert.Equal(t, "Succeeded", exec.Status)
	})

	t.Run("lambda", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		h.Backend.SetLambdaBackend(&fakeLambdaInvoker{err: errFakeBackend})

		p := lambdaActionPipeline("lambda-no-config", "")
		_, err := h.Backend.CreatePipeline(ctx, p, nil)
		require.NoError(t, err)

		exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
		require.NoError(t, err)
		assert.Equal(t, "Succeeded", exec.Status)
	})

	t.Run("codedeploy", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		h.Backend.SetCodeDeployBackend(&fakeCodeDeployStarter{err: errFakeBackend})

		p := codeDeployActionPipeline("cd-no-config", "", "")
		_, err := h.Backend.CreatePipeline(ctx, p, nil)
		require.NoError(t, err)

		exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
		require.NoError(t, err)
		assert.Equal(t, "Succeeded", exec.Status)
	})
}

// TestRunOneAction_NonAWSProviderUntouched proves that a custom action type
// sharing the "Build" category with an arbitrary provider (Owner != "AWS")
// is never routed to the wired CodeBuild backend, even when one is wired --
// only the built-in AWS/CodeBuild/Build action type is.
func TestRunOneAction_NonAWSProviderUntouched(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.SetCodeBuildBackend(&fakeCodeBuildStarter{err: errFakeBackend})

	p := samplePipeline("custom-build-provider")
	p.Stages = append(p.Stages, codepipeline.Stage{
		Name: "Build",
		Actions: []codepipeline.Action{
			{
				Name: "BuildAction",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Build",
					Owner:    "Custom",
					Provider: "MyBuilder",
					Version:  "1",
				},
				Configuration: map[string]string{"ProjectName": "my-project"},
			},
		},
	})

	ctx := context.Background()
	_, err := h.Backend.CreatePipeline(ctx, p, nil)
	require.NoError(t, err)

	exec, err := h.Backend.StartPipelineExecution(ctx, p.Name)
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", exec.Status)
}

// regionRecorder records the region each cross-service call carried.
type regionRecorder struct{ regions []string }

func (r *regionRecorder) StartBuild(ctx context.Context, _ string) error {
	r.regions = append(r.regions, awsmeta.Region(ctx))

	return nil
}

func (r *regionRecorder) CreateDeployment(ctx context.Context, _, _ string) error {
	r.regions = append(r.regions, awsmeta.Region(ctx))

	return nil
}

func (r *regionRecorder) InvokeFunction(ctx context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	r.regions = append(r.regions, awsmeta.Region(ctx))

	return nil, 200, nil
}

func TestRunOneAction_CrossServiceCallsCarryPipelineRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		region   string
		provider string
		category string
		config   string
	}{
		{
			name:     "codebuild",
			region:   "eu-west-1",
			provider: "CodeBuild",
			category: "Build",
			config:   `{"ProjectName":"p"}`,
		},
		{name: "lambda", region: "ap-south-1", provider: "Lambda", category: "Invoke", config: `{"FunctionName":"f"}`},
		{
			name: "codedeploy", region: "us-west-2", provider: "CodeDeploy", category: "Deploy",
			config: `{"ApplicationName":"a","DeploymentGroupName":"g"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &regionRecorder{}
			h := newTestHandler(t)
			h.Backend.SetCodeBuildBackend(rec)
			h.Backend.SetLambdaBackend(rec)
			h.Backend.SetCodeDeployBackend(rec)

			call := func(op, body string) {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
				req.Header.Set("X-Amz-Target", "CodePipeline_20150709."+op)
				req.Header.Set("Content-Type", "application/x-amz-json-1.1")
				req.Header.Set("X-Amz-Region", tt.region)

				w := httptest.NewRecorder()
				require.NoError(t, h.Handler()(echo.New().NewContext(req, w)))
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			}

			call("CreatePipeline", `{"pipeline":{"name":"p","roleArn":"arn:aws:iam::000000000000:role/r",`+
				`"artifactStore":{"type":"S3","location":"b"},"stages":[`+
				`{"name":"Source","actions":[{"name":"s","actionTypeId":{"category":"Source","owner":"AWS",`+
				`"provider":"S3","version":"1"},"configuration":{"S3Bucket":"b","S3ObjectKey":"k"},`+
				`"outputArtifacts":[{"name":"o"}]}]},`+
				`{"name":"Act","actions":[{"name":"a","actionTypeId":{"category":"`+tt.category+`","owner":"AWS",`+
				`"provider":"`+tt.provider+`","version":"1"},"configuration":`+tt.config+
				`,"inputArtifacts":[{"name":"o"}]}]}]}}`)
			call("StartPipelineExecution", `{"name":"p"}`)
			assert.Equal(t, []string{tt.region}, rec.regions)
		})
	}
}
