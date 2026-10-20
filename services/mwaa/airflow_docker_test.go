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
	startupScript        = "echo startup-ran\nexport MWAA_TEST_VAR=from-startup\n"
	envDAG               = `import os
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

dag_id = "hello_gopherstack_" + os.environ.get("MWAA_TEST_VAR", "unset")

with DAG(dag_id, start_date=datetime(2024, 1, 1), schedule=None):
    EmptyOperator(task_id="noop")
`
)

func TestAirflowEngine_RealAirflowDocker(t *testing.T) {
	t.Parallel()

	b := newDockerBackend(t, map[string]string{"dags/hello.py": envDAG, "scripts/startup.sh": startupScript})

	ctx := t.Context()
	_, err := b.CreateEnvironment(ctx, "docker-env", dockerCreateRequest())
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

	require.Eventually(t, func() bool {
		resp, rerr := b.InvokeRestAPI(ctx, "docker-env", mwaa.NewInvokeRequest("GET", "/dags"))

		return rerr == nil && resp.RestAPIStatusCode == 200 &&
			strings.Contains(mwaa.ResponseJSON(t, resp), "hello_gopherstack_from-startup")
	}, 3*time.Minute, 2*time.Second, "startup script export must reach the scheduler")

	webToken, _, err := b.CreateWebLoginToken(ctx, "docker-env")
	require.NoError(t, err)

	assertWebAuth(t, b, webToken)
}

func assertWebAuth(t *testing.T, b *mwaa.InMemoryBackend, webToken string) {
	t.Helper()

	hc := httpclient.New(30 * time.Second)
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	sess, err := b.WebLogin(t.Context(), webToken)
	require.NoError(t, err)

	anon := fetch(t, hc, sess.Target+"/home", nil)
	assert.Equal(t, http.StatusFound, anon.StatusCode, "anonymous browser must be sent to the login page")
	assert.Contains(t, anon.Header.Get("Location"), "/login")

	api := fetch(t, hc, sess.Target+"/api/v1/dags", nil)
	assert.Equal(t, http.StatusUnauthorized, api.StatusCode, "anonymous REST must be denied")

	page := fetch(t, hc, sess.Target+"/home", sess.Cookies)
	assert.Equal(t, http.StatusOK, page.StatusCode, "SSO session must open the UI")
}

func fetch(t *testing.T, hc *http.Client, target string, cookies []*http.Cookie) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	require.NoError(t, err)

	for _, c := range cookies {
		req.AddCookie(c)
	}

	resp, err := hc.Do(req)
	require.NoError(t, err)

	_ = resp.Body.Close()

	return resp
}

func newDockerBackend(t *testing.T, objects map[string]string) *mwaa.InMemoryBackend {
	t.Helper()

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
	b.SetAppConfig(newS3Sibling(t, "dag-bucket", objects))
	b.EnableAirflow(mwaa.AirflowConfig{
		Runtime:      rt,
		PollInterval: time.Second,
		StartTimeout: 8 * time.Minute,
	})
	t.Cleanup(b.Close)

	h := mwaa.NewHandler(b)
	h.DefaultRegion = testRegion

	return b
}

func TestAirflowEngine_DockerStartupScriptFailure(t *testing.T) {
	t.Parallel()

	b := newDockerBackend(t, map[string]string{
		"dags/hello.py": envDAG, "scripts/startup.sh": "echo failing-now\nexit 7\n",
	})

	_, err := b.CreateEnvironment(t.Context(), "bad-startup", dockerCreateRequest())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		env, gerr := b.GetEnvironment(t.Context(), "bad-startup")

		return gerr == nil && env.Status == "CREATE_FAILED"
	}, 3*time.Minute, time.Second)
}

func dockerCreateRequest() *mwaa.ExportedCreateEnvironmentRequest {
	return &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:                    "dags",
		ExecutionRoleArn:             "arn:aws:iam::123456789012:role/r",
		SourceBucketArn:              "arn:aws:s3:::dag-bucket",
		AirflowVersion:               dockerAirflowVersion,
		StartupScriptS3Path:          "scripts/startup.sh",
		StartupScriptS3ObjectVersion: "null",
		NetworkConfiguration: &mwaa.NetworkConfig{
			SubnetIDs:        []string{"subnet-aaaa1111", "subnet-bbbb2222"},
			SecurityGroupIDs: []string{"sg-cccc3333"},
		},
	}
}
