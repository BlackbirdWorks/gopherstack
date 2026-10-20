package mwaa_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

const (
	dockerAirflowVersion = "2.10.3"
	dockerAirflowImage   = "apache/airflow:" + dockerAirflowVersion
	tinyDAG              = `from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG("hello_gopherstack", start_date=datetime(2024, 1, 1), schedule=None):
    EmptyOperator(task_id="noop")
`
)

func TestAirflowEngine_RealAirflowDocker(t *testing.T) {
	t.Parallel()

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	pctx, pcancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer pcancel()

	if err = rt.Ping(pctx); err != nil {
		t.Skipf("container runtime not reachable: %v", err)
	}

	if ok, herr := rt.HasImage(pctx, dockerAirflowImage); herr != nil || !ok {
		t.Skipf("image %s not present", dockerAirflowImage)
	}

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	b.SetAppConfig(newS3Sibling(t, "dag-bucket", map[string]string{"dags/hello.py": tinyDAG}))
	b.EnableAirflow(mwaa.AirflowConfig{
		Runtime:      rt,
		PollInterval: time.Second,
		StartTimeout: 8 * time.Minute,
	})
	t.Cleanup(b.Close)

	h := mwaa.NewHandler(b)
	h.DefaultRegion = testRegion

	ctx := t.Context()
	_, err = b.CreateEnvironment(ctx, "docker-env", dockerCreateRequest())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		env, gerr := b.GetEnvironment(ctx, "docker-env")

		return gerr == nil && env.Status == "AVAILABLE"
	}, 9*time.Minute, time.Second)

	token, _, err := b.CreateCliToken(ctx, "docker-env")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		stdout, _, cerr := b.ExecCLI(ctx, token, "dags list")

		return cerr == nil && strings.Contains(stdout, "hello_gopherstack")
	}, 3*time.Minute, 2*time.Second)

	resp, err := b.InvokeRestAPI(ctx, "docker-env", mwaa.NewInvokeRequest("GET", "/dags"))
	require.NoError(t, err)
	assert.EqualValues(t, 200, resp.RestAPIStatusCode)
	assert.Contains(t, mwaa.ResponseJSON(t, resp), "hello_gopherstack")

	webToken, _, err := b.CreateWebLoginToken(ctx, "docker-env")
	require.NoError(t, err)

	target, err := b.WebLoginTarget(webToken)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target+"/home", nil)
	require.NoError(t, err)

	page, err := httpclient.New(30 * time.Second).Do(req)
	require.NoError(t, err)

	_ = page.Body.Close()

	assert.Equal(t, http.StatusOK, page.StatusCode, "webserver must serve the UI without a separate login")
}

func dockerCreateRequest() *mwaa.ExportedCreateEnvironmentRequest {
	return &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
		SourceBucketArn:  "arn:aws:s3:::dag-bucket",
		AirflowVersion:   dockerAirflowVersion,
		NetworkConfiguration: &mwaa.NetworkConfig{
			SubnetIDs:        []string{"subnet-aaaa1111", "subnet-bbbb2222"},
			SecurityGroupIDs: []string{"sg-cccc3333"},
		},
	}
}
