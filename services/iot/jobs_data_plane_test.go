package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

const jobsDataAuth = "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/iot-jobs-data/aws4_request, " +
	"SignedHeaders=host, Signature=abc"

type jobsDataEnv struct {
	client *iotsdk.Client
	url    string
}

func newJobsDataEnv(t *testing.T) jobsDataEnv {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(iot.NewHandler(iot.NewInMemoryBackend(), nil)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return jobsDataEnv{
		url: srv.URL,
		client: iotsdk.NewFromConfig(cfg, func(o *iotsdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
		}),
	}
}

func (e jobsDataEnv) device(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), method, e.url+path, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Authorization", jobsDataAuth)
	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)

	return resp.StatusCode, out
}

func TestJobsDataPlane_DeviceFlow(t *testing.T) {
	t.Parallel()

	env := newJobsDataEnv(t)
	ctx := t.Context()

	for _, name := range []string{"dev1", "dev2"} {
		_, err := env.client.CreateThing(ctx, &iotsdk.CreateThingInput{ThingName: aws.String(name)})
		require.NoError(t, err)
	}

	_, err := env.client.CreateJob(ctx, &iotsdk.CreateJobInput{
		JobId:    aws.String("j1"),
		Document: aws.String(`{"op":"reboot"}`),
		Targets: []string{
			"arn:aws:iot:us-east-1:000000000000:thing/dev1",
			"arn:aws:iot:us-east-1:000000000000:thing/dev2",
		},
	})
	require.NoError(t, err)

	code, pending := env.device(t, http.MethodGet, "/things/dev1/jobs", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Len(t, pending["queuedJobs"], 1)
	assert.Empty(t, pending["inProgressJobs"])

	code, started := env.device(t, http.MethodPut, "/things/dev1/jobs/$next", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	exec, _ := started["execution"].(map[string]any)
	require.NotNil(t, exec)
	assert.Equal(t, "IN_PROGRESS", exec["status"])
	assert.JSONEq(t, `{"op":"reboot"}`, exec["jobDocument"].(string))

	code, _ = env.device(t, http.MethodPost, "/things/dev1/jobs/j1", map[string]any{"status": "QUEUED"})
	assert.Equal(t, http.StatusBadRequest, code)

	code, stale := env.device(t, http.MethodPost, "/things/dev1/jobs/j1", map[string]any{
		"status": "SUCCEEDED", "expectedVersion": 99,
	})
	assert.Equal(t, http.StatusConflict, code, "stale expectedVersion: %v", stale)

	code, updated := env.device(t, http.MethodPost, "/things/dev1/jobs/j1", map[string]any{
		"status": "SUCCEEDED", "includeJobExecutionState": true, "statusDetails": map[string]string{"r": "ok"},
	})
	require.Equal(t, http.StatusOK, code)
	state, _ := updated["executionState"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", state["status"])

	code, _ = env.device(t, http.MethodPost, "/things/dev1/jobs/j1", map[string]any{"status": "FAILED"})
	assert.Equal(t, http.StatusGone, code, "finished executions are immutable")

	desc, err := env.client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("j1")})
	require.NoError(t, err)
	assert.Equal(t, types.JobStatusInProgress, desc.Job.Status)
	assert.Equal(t, types.TargetSelectionSnapshot, desc.Job.TargetSelection)
	assert.EqualValues(t, 1, aws.ToInt32(desc.Job.JobProcessDetails.NumberOfSucceededThings))

	code, second := env.device(t, http.MethodPut, "/things/dev2/jobs/$next", map[string]any{})
	require.Equal(t, http.StatusOK, code)
	assert.NotNil(t, second["execution"])

	code, _ = env.device(t, http.MethodPost, "/things/dev2/jobs/j1", map[string]any{"status": "FAILED"})
	require.Equal(t, http.StatusOK, code)

	desc, err = env.client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("j1")})
	require.NoError(t, err)
	assert.Equal(t, types.JobStatusCompleted, desc.Job.Status)
	assert.NotNil(t, desc.Job.CompletedAt)

	code, none := env.device(t, http.MethodPut, "/things/dev1/jobs/$next", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.Nil(t, none["execution"], "nothing left to start")

	code, _ = env.device(t, http.MethodGet, "/things/ghost/jobs", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestJobsDataPlane_ContinuousJobNeverCompletes(t *testing.T) {
	t.Parallel()

	env := newJobsDataEnv(t)
	ctx := t.Context()

	_, err := env.client.CreateThing(ctx, &iotsdk.CreateThingInput{ThingName: aws.String("dev1")})
	require.NoError(t, err)

	_, err = env.client.CreateJob(ctx, &iotsdk.CreateJobInput{
		JobId:           aws.String("cj"),
		Document:        aws.String(`{}`),
		TargetSelection: types.TargetSelectionContinuous,
		Targets:         []string{"arn:aws:iot:us-east-1:000000000000:thing/dev1"},
	})
	require.NoError(t, err)

	code, _ := env.device(t, http.MethodPut, "/things/dev1/jobs/$next", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	code, _ = env.device(t, http.MethodPost, "/things/dev1/jobs/cj", map[string]any{"status": "SUCCEEDED"})
	require.Equal(t, http.StatusOK, code)

	desc, err := env.client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("cj")})
	require.NoError(t, err)
	assert.Equal(t, types.JobStatusInProgress, desc.Job.Status)
}
