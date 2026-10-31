package ecs_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

const (
	metaRoleArn  = "arn:aws:iam::000000000000:role/task-role"
	metaTaskRole = "task-role"
)

var errNoTrust = errors.New("role does not trust ecs-tasks")

type metaIssuer struct {
	err      error
	sessions []string
}

func (m *metaIssuer) IssueTaskCredentials(roleArn, session string) (ecs.RoleCredentials, error) {
	if m.err != nil {
		return ecs.RoleCredentials{}, m.err
	}

	m.sessions = append(m.sessions, roleArn+"#"+session)

	return ecs.RoleCredentials{
		AccessKeyID: "ASIAECSTASK", SecretAccessKey: "task-secret", SessionToken: "task-token",
		Expires: time.Now().Add(time.Hour).UTC().Truncate(time.Second),
	}, nil
}

type statsRunner struct{ ecs.TaskRunner }

func (statsRunner) ContainerStatsJSON(_ context.Context, id string) (json.RawMessage, error) {
	return json.RawMessage(`{"id":"` + id + `","cpu_stats":{"online_cpus":2},"memory_stats":{"usage":1024}}`), nil
}

type metaFixture struct {
	srv    *httptest.Server
	issuer *metaIssuer
	bk     *ecs.InMemoryBackend
	taskID string
	task   ecs.Task
}

func newMetaFixture(t *testing.T, runner ecs.TaskRunner, role string) *metaFixture {
	t.Helper()

	bk := ecs.NewInMemoryBackend(testAccountID, testRegion, runner)
	issuer := &metaIssuer{}
	bk.SetRoleCredentialIssuer(issuer)

	h := ecs.NewHandler(bk)
	e := echo.New()
	e.GET("/_gopherstack/ecs/credentials/:task", h.ServeTaskCredentials)
	e.GET("/_gopherstack/ecs/metadata/:task/:container", h.ServeTaskMetadata)
	e.GET("/_gopherstack/ecs/metadata/:task/:container/*", h.ServeTaskMetadata)

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	bk.SetEndpointURL(srv.URL)

	td, err := bk.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family: "web", CPU: "256", Memory: "512", TaskRoleArn: role,
		ContainerDefinitions: []ecs.ContainerDefinition{{
			Name: "app", Image: "nginx:1", CPU: 128, Memory: 256, Essential: true,
			PortMappings: []ecs.PortMapping{{ContainerPort: 80, HostPort: 8080, Protocol: "tcp"}},
		}},
	})
	require.NoError(t, err)

	_, err = bk.CreateCluster(ecs.CreateClusterInput{ClusterName: "meta"})
	require.NoError(t, err)

	tasks, _, err := bk.RunTask(ecs.RunTaskInput{
		Cluster: "meta", TaskDefinition: td.TaskDefinitionArn, Group: "service:web",
		Tags: []ecs.Tag{{Key: "team", Value: "core"}},
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return &metaFixture{
		srv: srv, issuer: issuer, bk: bk, task: tasks[0],
		taskID: tasks[0].TaskArn[len(tasks[0].TaskArn)-36:],
	}
}

func (f *metaFixture) get(t *testing.T, path string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, f.srv.URL+path, nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)

	return resp.StatusCode, out
}

func TestTaskCredentialsEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		issuerErr  error
		name       string
		role       string
		wantCode   string
		wantStatus int
		stopTask   bool
	}{
		{name: "ok", role: metaRoleArn, wantStatus: http.StatusOK},
		{name: "no_role", wantStatus: http.StatusNotFound, wantCode: "NotFound"},
		{name: "stopped", role: metaRoleArn, stopTask: true, wantStatus: http.StatusNotFound, wantCode: "NotFound"},
		{
			name:       "issuer_denied",
			role:       metaRoleArn,
			issuerErr:  errNoTrust,
			wantStatus: http.StatusForbidden,
			wantCode:   "AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newMetaFixture(t, ecs.NewNoopRunner(), tt.role)
			f.issuer.err = tt.issuerErr

			if tt.stopTask {
				_, err := f.bk.StopTask("meta", f.task.TaskArn, "done")
				require.NoError(t, err)
			}

			status, body := f.get(t, "/_gopherstack/ecs/credentials/"+f.taskID)
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantStatus != http.StatusOK {
				assert.Equal(t, tt.wantCode, body["code"])
				assert.NotEmpty(t, body["message"])

				return
			}

			assert.Equal(t, metaRoleArn, body["RoleArn"])
			assert.Equal(t, "ASIAECSTASK", body["AccessKeyId"])
			assert.Equal(t, "task-secret", body["SecretAccessKey"])
			assert.Equal(t, "task-token", body["Token"])
			assert.NotEmpty(t, body["Expiration"])
			assert.Equal(t, []string{metaRoleArn + "#" + f.taskID}, f.issuer.sessions)
		})
	}
}

func TestTaskCredentialsSDKProvider(t *testing.T) {
	t.Parallel()

	f := newMetaFixture(t, ecs.NewNoopRunner(), metaRoleArn)
	p := endpointcreds.New(f.srv.URL + "/_gopherstack/ecs/credentials/" + f.taskID)

	creds, err := p.Retrieve(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "ASIAECSTASK", creds.AccessKeyID)
	assert.Equal(t, "task-secret", creds.SecretAccessKey)
	assert.Equal(t, "task-token", creds.SessionToken)
	assert.True(t, creds.CanExpire)
	assert.WithinDuration(t, time.Now().Add(time.Hour), creds.Expires, time.Minute)

	p = endpointcreds.New(f.srv.URL + "/_gopherstack/ecs/credentials/unknown-task")
	_, err = p.Retrieve(t.Context())
	require.Error(t, err)
}

func TestTaskMetadataEndpoints(t *testing.T) {
	t.Parallel()

	f := newMetaFixture(t, statsRunner{ecs.NewNoopRunner()}, metaRoleArn)
	base := "/_gopherstack/ecs/metadata/" + f.taskID + "/app"

	t.Run("container", func(t *testing.T) {
		t.Parallel()

		status, body := f.get(t, base)
		require.Equal(t, http.StatusOK, status)

		assert.Equal(t, "app", body["Name"])
		assert.Equal(t, "nginx:1", body["Image"])
		assert.Equal(t, "RUNNING", body["KnownStatus"])
		assert.Equal(t, "RUNNING", body["DesiredStatus"])
		assert.Equal(t, "NORMAL", body["Type"])
		assert.NotEmpty(t, body["DockerId"])
		assert.NotEmpty(t, body["ContainerARN"])
		assert.NotEmpty(t, body["StartedAt"])
		assert.Equal(t, map[string]any{"CPU": 128.0, "Memory": 256.0}, body["Limits"])

		labels, ok := body["Labels"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, f.task.ClusterArn, labels["com.amazonaws.ecs.cluster"])
		assert.Equal(t, f.task.TaskArn, labels["com.amazonaws.ecs.task-arn"])
		assert.Equal(t, "web", labels["com.amazonaws.ecs.task-definition-family"])
		assert.Equal(t, "1", labels["com.amazonaws.ecs.task-definition-version"])
		assert.Equal(t, "app", labels["com.amazonaws.ecs.container-name"])

		ports, ok := body["Ports"].([]any)
		require.True(t, ok)
		assert.Equal(
			t,
			map[string]any{"Protocol": "tcp", "ContainerPort": 80.0, "HostPort": 8080.0, "HostIp": "0.0.0.0"},
			ports[0],
		)
	})

	for _, tc := range []struct {
		name     string
		path     string
		wantTags bool
	}{
		{name: "task", path: "/task"},
		{name: "task_with_tags", path: "/taskWithTags", wantTags: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status, body := f.get(t, base+tc.path)
			require.Equal(t, http.StatusOK, status)

			assert.Equal(t, f.task.ClusterArn, body["Cluster"])
			assert.Equal(t, f.task.TaskArn, body["TaskARN"])
			assert.Equal(t, "web", body["Family"])
			assert.Equal(t, "1", body["Revision"])
			assert.Equal(t, "RUNNING", body["KnownStatus"])
			assert.Equal(t, "RUNNING", body["DesiredStatus"])
			assert.Equal(t, "FARGATE", body["LaunchType"])
			assert.Equal(t, testRegion+"a", body["AvailabilityZone"])
			assert.Equal(t, "web", body["ServiceName"])
			assert.Equal(t, map[string]any{"CPU": 0.25, "Memory": 512.0}, body["Limits"])

			containers, ok := body["Containers"].([]any)
			require.True(t, ok)
			require.Len(t, containers, 1)
			assert.Equal(t, "app", containers[0].(map[string]any)["Name"])

			if tc.wantTags {
				assert.Equal(t, map[string]any{"team": "core"}, body["TaskTags"])
			} else {
				assert.NotContains(t, body, "TaskTags")
			}
		})
	}

	t.Run("stats", func(t *testing.T) {
		t.Parallel()

		status, body := f.get(t, base+"/stats")
		require.Equal(t, http.StatusOK, status)
		assert.NotEmpty(t, body["id"])
		assert.Contains(t, body, "memory_stats")
	})

	t.Run("task_stats", func(t *testing.T) {
		t.Parallel()

		status, body := f.get(t, base+"/task/stats")
		require.Equal(t, http.StatusOK, status)
		require.Len(t, body, 1)

		for id, v := range body {
			assert.Equal(t, id, v.(map[string]any)["id"])
		}
	})

	for _, tc := range []struct{ name, path string }{
		{name: "unknown_task", path: "/_gopherstack/ecs/metadata/nope/app"},
		{name: "unknown_container", path: "/_gopherstack/ecs/metadata/" + f.taskID + "/nope"},
		{name: "unknown_subpath", path: base + "/bogus"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status, _ := f.get(t, tc.path)
			assert.Equal(t, http.StatusNotFound, status)
		})
	}
}

func TestTaskMetadataStatsUnavailableWithoutRuntime(t *testing.T) {
	t.Parallel()

	f := newMetaFixture(t, ecs.NewNoopRunner(), metaTaskRole)

	status, _ := f.get(t, "/_gopherstack/ecs/metadata/"+f.taskID+"/app/stats")
	assert.Equal(t, http.StatusNotFound, status)
}
