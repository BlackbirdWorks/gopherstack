package app

import (
	"context"
	"sync"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

type recordingRunner struct {
	specs []container.Spec
	mu    sync.Mutex
}

func (r *recordingRunner) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.specs = append(r.specs, spec)

	return "c1", nil
}

func (r *recordingRunner) StopAndRemove(context.Context, string) error { return nil }

func (r *recordingRunner) WaitExit(context.Context, string) (int, error) { return 0, nil }

func TestWireGCPRun_SecretEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantEnv string
		wantMsg string
		wired   bool
	}{
		{name: "wired", wired: true, wantEnv: "TOKEN=hunter2"},
		{name: "not wired", wantMsg: "secret manager is not available"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			run := gcprunbackend.NewHandler(gcprunbackend.NewInMemoryBackend())
			sm := gcpsecretmanagerbackend.NewHandler(gcpsecretmanagerbackend.NewInMemoryBackend())
			rt := &recordingRunner{}
			run.Backend.SetRuntime(func() (gcprunbackend.Runner, error) { return rt, nil })
			t.Cleanup(run.Backend.Close)

			byName := map[string]service.Registerable{"GCPRun": run}
			if tt.wired {
				byName["GCPSecretManager"] = sm
			}

			wireGCPRun(byName)

			_, err := sm.Backend.CreateSecret(&secretmanagerpb.CreateSecretRequest{
				Parent: "projects/p", SecretId: "api-token", Secret: automaticSecret(),
			})
			require.NoError(t, err)

			_, err = sm.Backend.AddSecretVersion(&secretmanagerpb.AddSecretVersionRequest{
				Parent:  "projects/p/secrets/api-token",
				Payload: &secretmanagerpb.SecretPayload{Data: []byte("hunter2")},
			})
			require.NoError(t, err)

			job := &runpb.Job{Template: &runpb.ExecutionTemplate{TaskCount: 1, Template: &runpb.TaskTemplate{
				Retries: &runpb.TaskTemplate_MaxRetries{},
				Containers: []*runpb.Container{{Image: "i", Env: []*runpb.EnvVar{{
					Name: "TOKEN",
					Values: &runpb.EnvVar_ValueSource{ValueSource: &runpb.EnvVarSource{
						SecretKeyRef: &runpb.SecretKeySelector{Secret: "api-token", Version: "latest"},
					}},
				}}}},
			}}}

			_, err = run.Backend.CreateJob(&runpb.CreateJobRequest{
				Parent: "projects/p/locations/us-central1", JobId: "j", Job: job,
			})
			require.NoError(t, err)

			_, err = run.Backend.RunJob(&runpb.RunJobRequest{Name: "projects/p/locations/us-central1/jobs/j"})
			require.NoError(t, err)

			run.Backend.Drain()

			ex, _, err := run.Backend.ListExecutions(&runpb.ListExecutionsRequest{
				Parent: "projects/p/locations/us-central1/jobs/j",
			})
			require.NoError(t, err)
			require.Len(t, ex, 1)

			if tt.wired {
				require.Len(t, rt.specs, 1)
				assert.Contains(t, rt.specs[0].Env, tt.wantEnv)
				assert.Equal(t, int32(1), ex[0].GetSucceededCount())

				return
			}

			assert.Empty(t, rt.specs)
			assert.Equal(t, int32(1), ex[0].GetFailedCount())

			tasks, _, terr := run.Backend.ListTasks(&runpb.ListTasksRequest{Parent: ex[0].GetName()})
			require.NoError(t, terr)
			assert.Contains(t, tasks[0].GetLastAttemptResult().GetStatus().GetMessage(), tt.wantMsg)
		})
	}
}

func automaticSecret() *secretmanagerpb.Secret {
	return &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
		Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}},
	}}
}

func TestWireGCPRun_SubmitBuild(t *testing.T) {
	t.Parallel()

	run := gcprunbackend.NewHandler(gcprunbackend.NewInMemoryBackend())
	cb := gcpcloudbuildbackend.NewHandler(gcpcloudbuildbackend.NewInMemoryBackend())
	t.Cleanup(run.Backend.Close)
	t.Cleanup(cb.Backend.Close)
	cb.Backend.DisableEngine("no container engine in this test")

	src := &runpb.StorageSource{Bucket: "b", Object: "o.tar.gz"}
	req := &runpb.SubmitBuildRequest{
		Parent:    "projects/p/locations/us-central1",
		Source:    &runpb.SubmitBuildRequest_StorageSource{StorageSource: src},
		ImageUri:  "us-central1-docker.pkg.dev/p/repo/app",
		BuildType: &runpb.SubmitBuildRequest_DockerBuild_{DockerBuild: &runpb.SubmitBuildRequest_DockerBuild{}},
	}

	_, err := run.Backend.SubmitBuild(req)
	require.Error(t, err, "no Cloud Build emulator wired")

	wireGCPRun(map[string]service.Registerable{"GCPRun": run, "GCPCloudBuild": cb})

	resp, err := run.Backend.SubmitBuild(req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetBuildOperation().GetName())

	builds, _, err := cb.Backend.ListBuilds(&cloudbuildpb.ListBuildsRequest{Parent: "projects/p/locations/us-central1"})
	require.NoError(t, err)
	require.Len(t, builds, 1)
	assert.Equal(t, []string{req.GetImageUri()}, builds[0].GetImages())
	assert.Equal(t, "gcr.io/cloud-builders/docker", builds[0].GetSteps()[0].GetName())
}
