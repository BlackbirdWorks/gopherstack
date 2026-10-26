package app

import (
	"context"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
		wired   bool
		wantEnv string
		wantMsg string
	}{
		{"wired", true, "TOKEN=hunter2", ""},
		{"not wired", false, "", "secret manager is not available"},
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
				Parent: "projects/p", SecretId: "api-token", Secret: &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
					Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}},
				}},
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
			assert.True(t, strings.Contains(tasks[0].GetLastAttemptResult().GetStatus().GetMessage(), tt.wantMsg))
		})
	}
}
