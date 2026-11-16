package azurefunctions_test

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestKudu_Command(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCmd  string
		wantCode int
		wantExit int
	}{
		{
			"runs in dir",
			`{"command":"ls -la","dir":"site\\wwwroot"}`,
			"cd '/home/site/wwwroot' && ls -la",
			http.StatusOK,
			0,
		},
		{"exit code", `{"command":"false"}`, "cd '/home/site/wwwroot' && false", http.StatusOK, 3},
		{"home relative", `{"command":"pwd","dir":"data"}`, "cd '/home/data' && pwd", http.StatusOK, 0},
		{"no command", `{"dir":"x"}`, "", http.StatusBadRequest, 0},
		{"bad json", `nope`, "", http.StatusBadRequest, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.rt.onExec = func(_ container.Spec, cmd []string) container.ExecResult {
				return container.ExecResult{Stdout: "out:" + cmd[2], Stderr: "err", ExitCode: tt.wantExit}
			}
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
			require.NoError(t, err)

			resp, body := e.do(
				t,
				http.MethodPost,
				"app.scm."+testDomain,
				"/api/command",
				[]byte(tt.body),
				e.kuduAuth(t, "app"),
			)
			require.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode != http.StatusOK {
				return
			}

			var got map[string]any
			require.NoError(t, json.Unmarshal([]byte(body), &got, jsonx.V1()))
			assert.Equal(t, "out:"+tt.wantCmd, got["Output"])
			assert.Equal(t, "err", got["Error"])
			assert.InDelta(t, tt.wantExit, got["ExitCode"], 0)
		})
	}
}

func TestKudu_CommandNeedsRuntime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
	require.NoError(t, err)

	resp, _ := e.do(
		t,
		http.MethodPost,
		"app.scm."+testDomain,
		"/api/command",
		[]byte(`{"command":"ls"}`),
		e.kuduAuth(t, "app"),
	)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestKudu_Logstream(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.srv.URL+"/api/logstream", nil)
	require.NoError(t, err)

	req.Host = "app.scm." + testDomain
	user, pass, _, err := e.backend.PublishingCredentials("app")
	require.NoError(t, err)
	req.SetBasicAuth(user, pass)

	resp, err := e.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	buf := make([]byte, len("host log line\n"))
	_, err = resp.Body.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "host log line\n", string(buf))

	cancel()
}

func TestKudu_WebJobs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.onExec = func(_ container.Spec, cmd []string) container.ExecResult {
		return container.ExecResult{Stdout: "ran " + cmd[2]}
	}
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

	pkg := zipOf(t, map[string]string{
		"host.json":                              `{"version":"2.0"}`,
		"App_Data/jobs/triggered/nightly/run.sh": "echo hi",
		"App_Data/jobs/triggered/broken/readme":  "no script",
	})
	_, err := e.backend.Deploy("app", pkg, "t", "m")
	require.NoError(t, err)

	auth, scm := e.kuduAuth(t, "app"), "app.scm."+testDomain

	_, body := e.do(t, http.MethodGet, scm, "/api/triggeredwebjobs", nil, auth)

	var jobs []map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &jobs, jsonx.V1()))
	require.Len(t, jobs, 1)
	assert.Equal(t, "nightly", jobs[0]["name"])
	assert.Equal(t, "run.sh", jobs[0]["runCommand"])
	assert.Equal(t, "triggered", jobs[0]["type"])

	resp, _ := e.do(t, http.MethodGet, scm, "/api/triggeredwebjobs/ghost", nil, auth)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, _ = e.do(t, http.MethodPost, scm, "/api/triggeredwebjobs/nightly/run?arguments=-x", nil, auth)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Location"), "/api/triggeredwebjobs/nightly/history/")

	var runID string

	require.Eventually(t, func() bool {
		_, hist := e.do(t, http.MethodGet, scm, "/api/triggeredwebjobs/nightly/history", nil, auth)

		var got struct {
			Runs []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"runs"`
		}

		if json.Unmarshal([]byte(hist), &got, jsonx.V1()) != nil || len(got.Runs) != 1 ||
			got.Runs[0].Status != "Success" {
			return false
		}

		runID = got.Runs[0].ID

		return true
	}, 10*time.Second, 20*time.Millisecond)

	_, out := e.do(
		t,
		http.MethodGet,
		scm,
		"/api/vfs/data/jobs/triggered/nightly/"+runID+"/output_"+runID+".log",
		nil,
		auth,
	)
	assert.Equal(t, "ran cd '/home/site/wwwroot/App_Data/jobs/triggered/nightly' && sh run.sh -x", out)
}

func TestKudu_VFSHome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		want     string
		wantCode int
	}{
		{"root", http.MethodGet, "/api/vfs/", `"name":"LogFiles"`, http.StatusOK},
		{"root has site", http.MethodGet, "/api/vfs/", `"name":"site"`, http.StatusOK},
		{"site", http.MethodGet, "/api/vfs/site/", `"name":"wwwroot"`, http.StatusOK},
		{"data", http.MethodGet, "/api/vfs/data/", "[]", http.StatusOK},
		{"unknown", http.MethodGet, "/api/vfs/etc/", "", http.StatusNotFound},
		{"write outside wwwroot", http.MethodPut, "/api/vfs/data/x.txt", "", http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.backend.PutApp("app", azurefunctions.AppConfig{})

			resp, body := e.do(t, tt.method, "app.scm."+testDomain, tt.path, []byte("x"), e.kuduAuth(t, "app"))
			require.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, strings.ReplaceAll(body, " ", ""), tt.want)
		})
	}
}

func TestKudu_ContinuousWebJobs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.onExec = func(_ container.Spec, cmd []string) container.ExecResult {
		return container.ExecResult{Stdout: "tick " + cmd[2]}
	}
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

	pkg := zipOf(t, map[string]string{
		"host.json":                              `{"version":"2.0"}`,
		"App_Data/jobs/continuous/worker/run.sh": "while true; do :; done",
	})
	_, err := e.backend.Deploy("app", pkg, "t", "m")
	require.NoError(t, err)

	auth, scm := e.kuduAuth(t, "app"), "app.scm."+testDomain
	status := func() string {
		_, body := e.do(t, http.MethodGet, scm, "/api/continuouswebjobs/worker", nil, auth)

		var got map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &got, jsonx.V1()))
		assert.Equal(t, "continuous", got["type"])
		assert.Equal(t, "run.sh", got["runCommand"])

		s, _ := got["status"].(string)

		return s
	}

	e.do(t, http.MethodGet, "app."+testDomain, "/", nil, nil)
	require.Eventually(t, func() bool { return status() == "Running" }, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		_, log := e.do(t, http.MethodGet, scm, "/api/vfs/data/jobs/continuous/worker/job_log.txt", nil, auth)

		return strings.Contains(log, "tick cd '/home/site/wwwroot/App_Data/jobs/continuous/worker' && sh run.sh")
	}, 10*time.Second, 10*time.Millisecond)

	_, all := e.do(t, http.MethodGet, scm, "/api/webjobs", nil, auth)
	assert.Contains(t, all, `"type":"continuous"`)

	resp, _ := e.do(t, http.MethodPost, scm, "/api/continuouswebjobs/worker/stop", nil, auth)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Stopped", status())

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), e.handler.Snapshot(t.Context())))
	dst.do(t, http.MethodGet, "app."+testDomain, "/", nil, nil)
	_, body := dst.do(t, http.MethodGet, scm, "/api/continuouswebjobs/worker", nil, dst.kuduAuth(t, "app"))
	assert.Contains(t, body, `"status":"Stopped"`, "a stopped job stays stopped after a restore")

	resp, _ = e.do(t, http.MethodPost, scm, "/api/continuouswebjobs/worker/start", nil, auth)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool { return status() == "Running" }, 10*time.Second, 10*time.Millisecond)
}
