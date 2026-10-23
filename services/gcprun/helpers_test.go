package gcprun_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	run "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcprun"
)

const parent = "projects/test-project/locations/us-central1"

type env struct {
	rt       *fakeRuntime
	handler  *gcprun.Handler
	services *run.ServicesClient
	revs     *run.RevisionsClient
	jobs     *run.JobsClient
	execs    *run.ExecutionsClient
	tasks    *run.TasksClient
	http     *http.Client
}

func newEnv(t *testing.T, withRuntime bool) *env {
	t.Helper()

	rt := newFakeRuntime()
	h := gcprun.NewHandler(gcprun.NewInMemoryBackend())
	h.Port, h.IngressPort = 0, 0

	if withRuntime {
		h.Backend.SetRuntime(func() (gcprun.Runner, error) { return rt, nil })
	}

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	e := &env{rt: rt, handler: h, http: &http.Client{Transport: &http.Transport{}}}
	t.Cleanup(e.http.CloseIdleConnections)

	var err error

	e.services, err = run.NewServicesClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.services.Close() })

	e.revs, err = run.NewRevisionsClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.revs.Close() })

	e.jobs, err = run.NewJobsClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.jobs.Close() })

	e.execs, err = run.NewExecutionsClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.execs.Close() })

	e.tasks, err = run.NewTasksClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.tasks.Close() })

	return e
}

func svcSpec(image string) *runpb.Service {
	return &runpb.Service{Template: &runpb.RevisionTemplate{
		Containers: []*runpb.Container{{
			Image: image,
			Env:   []*runpb.EnvVar{{Name: "GREETING", Values: &runpb.EnvVar_Value{Value: "hi"}}},
		}},
	}}
}

func (e *env) createService(t *testing.T, id, image string) *runpb.Service {
	t.Helper()

	op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: id, Service: svcSpec(image),
	})
	require.NoError(t, err)

	svc, err := op.Wait(t.Context())
	require.NoError(t, err)

	return svc
}

// get fetches path from the ingress using host as the Host header.
func (e *env) get(t *testing.T, host, path string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+e.handler.IngressAddr()+path, nil)
	require.NoError(t, err)

	req.Host = host

	resp, err := e.http.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

func jobSpec(image string, tasks, parallelism int32, retries int32) *runpb.Job {
	return &runpb.Job{Template: &runpb.ExecutionTemplate{
		TaskCount: tasks, Parallelism: parallelism,
		Template: &runpb.TaskTemplate{
			Containers: []*runpb.Container{{Image: image}},
			Retries:    &runpb.TaskTemplate_MaxRetries{MaxRetries: retries},
		},
	}}
}

func (e *env) createJob(t *testing.T, id string, job *runpb.Job) *runpb.Job {
	t.Helper()

	op, err := e.jobs.CreateJob(t.Context(), &runpb.CreateJobRequest{Parent: parent, JobId: id, Job: job})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

var _ container.Spec
