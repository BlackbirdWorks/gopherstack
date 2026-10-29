package codepipeline_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// scriptedStacks is a StackDeployer whose DescribeStack walks through a fixed status script.
type scriptedStacks struct {
	codepipeline.StackDeployer

	script  []codepipeline.StackState
	mu      sync.Mutex
	deleted int
	calls   int
}

func (s *scriptedStacks) DescribeStack(context.Context, string) (*codepipeline.StackState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	i := min(s.calls, len(s.script)-1)
	s.calls++
	st := s.script[i]

	return &st, nil
}

func (s *scriptedStacks) DeleteStack(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleted++

	return nil
}

type memArtifacts struct {
	data map[string][]byte
	seed []byte
	mu   sync.Mutex
}

func (m *memArtifacts) PutObject(_ context.Context, in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = body

	return &s3.PutObjectOutput{}, nil
}

func (m *memArtifacts) GetObject(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	body, ok := m.data[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok && m.seed != nil {
		body, ok = m.seed, true
	}

	if !ok {
		return nil, awserr.ErrNotFound
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

type stuckECS struct {
	codepipeline.ECSDeployer
}

func (stuckECS) DescribeService(context.Context, string, string) (*codepipeline.ECSServiceState, error) {
	return &codepipeline.ECSServiceState{
		TaskDefinition: "td:1",
		Deployments: []codepipeline.ECSDeploymentState{
			{TaskDefinition: "td:2", Status: "PRIMARY", RolloutState: "IN_PROGRESS"},
			{TaskDefinition: "td:1", Status: "ACTIVE", RolloutState: "COMPLETED"},
		},
	}, nil
}

func (stuckECS) RegisterRevisionWithImages(context.Context, string, map[string]string) (string, error) {
	return "td:2", nil
}

func (stuckECS) UpdateServiceTaskDefinition(context.Context, string, string, string) error {
	return nil
}

func builtinPipeline(name string, action codepipeline.Action) codepipeline.PipelineDeclaration {
	return codepipeline.PipelineDeclaration{
		Name:          name,
		RoleArn:       "arn:aws:iam::000000000000:role/p",
		ArtifactStore: codepipeline.ArtifactStore{Type: "S3", Location: "art"},
		Stages:        []codepipeline.Stage{{Name: "Deploy", Actions: []codepipeline.Action{action}}},
	}
}

func awsDeploy(provider string, cfg map[string]string, inputs ...string) codepipeline.Action {
	a := codepipeline.Action{
		Name: "Act",
		ActionTypeID: codepipeline.ActionTypeID{
			Category: "Deploy", Owner: "AWS", Provider: provider, Version: "1",
		},
		Configuration: cfg,
	}

	for _, in := range inputs {
		a.InputArtifacts = append(a.InputArtifacts, codepipeline.ArtifactRef{Name: in})
	}

	return a
}

func TestBuiltinActions_WaitForBackendState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantMsg    string
		script     []codepipeline.StackState
		wantDelete int
	}{
		{
			name: "delete waits then succeeds",
			script: []codepipeline.StackState{
				{Status: "CREATE_COMPLETE"}, {Status: "DELETE_IN_PROGRESS"}, {Status: "DELETE_COMPLETE"},
			},
			wantStatus: "Succeeded",
			wantDelete: 1,
		},
		{
			name: "delete failure surfaces stack reason",
			script: []codepipeline.StackState{
				{Status: "CREATE_COMPLETE"}, {Status: "DELETE_IN_PROGRESS"},
				{Status: "DELETE_FAILED", Reason: "bucket not empty"},
			},
			wantStatus: "Failed",
			wantMsg:    "bucket not empty",
			wantDelete: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stacks := &scriptedStacks{script: tt.script}
			h.Backend.SetStackDeployer(stacks)

			ctx := t.Context()
			decl := builtinPipeline("p", awsDeploy("CloudFormation",
				map[string]string{"ActionMode": "DELETE_ONLY", "StackName": "s"}))
			_, err := h.Backend.CreatePipeline(ctx, decl, nil)
			require.NoError(t, err)

			exec, err := h.Backend.StartPipelineExecution(ctx, "p")
			require.NoError(t, err)
			assert.Equal(t, "InProgress", exec.Status, "action waits for the stack")

			require.Eventually(t, func() bool {
				got, gErr := h.Backend.GetPipelineExecution(ctx, "p", exec.PipelineExecutionID)

				return gErr == nil && got.Status != "InProgress"
			}, 10*time.Second, 20*time.Millisecond)

			got, err := h.Backend.GetPipelineExecution(ctx, "p", exec.PipelineExecutionID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)

			stacks.mu.Lock()
			assert.Equal(t, tt.wantDelete, stacks.deleted)
			stacks.mu.Unlock()

			if tt.wantMsg != "" {
				details, lErr := h.Backend.ListActionExecutions(ctx, "p", exec.PipelineExecutionID)
				require.NoError(t, lErr)

				raw, mErr := json.Marshal(details)
				require.NoError(t, mErr)
				assert.Contains(t, string(raw), tt.wantMsg)
			}
		})
	}
}

func TestECSDeploy_TimesOutOnInjectedClock(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arts := &memArtifacts{data: map[string][]byte{}}
	h.Backend.SetArtifactS3(arts)
	h.Backend.SetECSDeployer(stuckECS{})

	now := time.Now()
	var mu sync.Mutex

	h.Backend.SetClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		return now
	})

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("imagedefinitions.json")
	require.NoError(t, err)
	_, err = w.Write([]byte(`[{"name":"web","imageUri":"nginx:2"}]`))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	ctx := t.Context()
	decl := builtinPipeline("p", awsDeploy("ECS",
		map[string]string{"ClusterName": "c", "ServiceName": "s", "DeploymentTimeout": "5"}, "In"))
	_, err = h.Backend.CreatePipeline(ctx, decl, nil)
	require.NoError(t, err)

	arts.seed = buf.Bytes()

	exec, err := h.Backend.StartPipelineExecution(ctx, "p")
	require.NoError(t, err)
	assert.Equal(t, "InProgress", exec.Status)

	mu.Lock()
	now = now.Add(6 * time.Minute)
	mu.Unlock()

	require.Eventually(t, func() bool {
		got, gErr := h.Backend.GetPipelineExecution(ctx, "p", exec.PipelineExecutionID)

		return gErr == nil && got.Status == "Failed"
	}, 10*time.Second, 20*time.Millisecond)
}
