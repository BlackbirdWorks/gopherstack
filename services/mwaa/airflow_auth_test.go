package mwaa_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const startupBody = "export MY_VAR=from-startup\necho hello-startup\n"

func envValue(spec container.Spec, name string) string {
	for _, kv := range spec.Env {
		if v, ok := strings.CutPrefix(kv, name+"="); ok {
			return v
		}
	}

	return ""
}

func stageOf(spec container.Spec) string {
	dags, _, _ := strings.Cut(spec.Mounts[0], ":")

	return filepath.Dir(dags)
}

func scriptOf(spec container.Spec) string { return spec.Cmd[len(spec.Cmd)-1] }

func TestAirflowStartupScript_StagedAndOrdered(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.ob.set("scripts/startup.sh", startupBody)
	rig.create(t, "su", map[string]any{
		"StartupScriptS3Path":          "scripts/startup.sh",
		"StartupScriptS3ObjectVersion": "ver-7",
		"RequirementsS3Path":           "requirements/req.tx",
		"RequirementsS3ObjectVersion":  "r1",
	})
	rig.ht.healthy.Store(true)
	rig.waitStatus(t, "su", "AVAILABLE")

	assert.Contains(t, rig.ob.fetched(), "scripts/startup.sh@ver-7")

	spec := rig.rt.startedSpecs()[0]
	got, err := os.ReadFile(filepath.Join(stageOf(spec), "startup.sh"))
	require.NoError(t, err)
	assert.Equal(t, startupBody, string(got))

	script := scriptOf(spec)
	run := strings.Index(script, "\nstartup\n")
	pip := strings.Index(script, "pip install")
	exec := strings.Index(script, "exec airflow standalone")

	require.Positive(t, run)
	assert.Less(t, run, pip, "startup script must run before requirements install")
	assert.Less(t, pip, exec)
	assert.Contains(t, script, "source /mwaa/startup.sh")
}

func TestAirflowStartupScript_FailureStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		patch      bool
	}{
		{name: "create", wantStatus: "CREATE_FAILED"},
		{name: "update", patch: true, wantStatus: "UPDATE_FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newAirflowRig(t)
			rig.ob.set("scripts/startup.sh", "exit 3")

			if tt.patch {
				rig.createAvailable(t, "fail")
			}

			rig.rt.onStart = func(spec container.Spec) {
				if tt.patch && len(rig.rt.started) < 2 {
					return
				}

				state := filepath.Join(stageOf(spec), "state")
				_ = os.WriteFile(filepath.Join(state, "startup.log"), []byte("boom from script\n"), 0o600)
				_ = os.WriteFile(filepath.Join(state, "exited"), []byte("3\n"), 0o600)
			}

			if tt.patch {
				rec := doMWAARequest(t, rig.h, http.MethodPatch, "/environments/fail", map[string]any{
					"StartupScriptS3Path": "scripts/startup.sh", "StartupScriptS3ObjectVersion": "v1",
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			} else {
				rig.create(t, "fail", map[string]any{
					"StartupScriptS3Path": "scripts/startup.sh", "StartupScriptS3ObjectVersion": "v1",
				})
			}

			rig.waitStatus(t, "fail", tt.wantStatus)

			if tt.patch {
				env, err := rig.b.GetEnvironment(t.Context(), "fail")
				require.NoError(t, err)
				require.NotNil(t, env.LastUpdate)
				assert.Equal(t, "FAILED", env.LastUpdate.Status)
				assert.Contains(t, env.LastUpdate.Error.ErrorMessage, "exit status 3")
				assert.Contains(t, env.LastUpdate.Error.ErrorMessage, "boom from script")
			}
		})
	}
}

func TestAirflowCLI_Allowlist(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.createAvailable(t, "allow")

	tok, _, err := rig.b.CreateCliToken(t.Context(), "allow")
	require.NoError(t, err)

	tests := []struct {
		cmd     string
		allowed bool
	}{
		{cmd: "dags list", allowed: true},
		{cmd: "dags trigger my_dag", allowed: true},
		{cmd: "tasks test d t 2024-01-01", allowed: true},
		{cmd: "variables set k v", allowed: true},
		{cmd: "connections add c --conn-uri x", allowed: true},
		{cmd: "version", allowed: true},
		{cmd: "cheat-sheet", allowed: true},
		{cmd: "users list", allowed: true},
		{cmd: "standalone", allowed: false},
		{cmd: "webserver", allowed: false},
		{cmd: "db reset", allowed: false},
		{cmd: "config get-value core dags_folder", allowed: false},
		{cmd: "dags", allowed: false},
		{cmd: "dags frobnicate", allowed: false},
		{cmd: "bash -c id", allowed: false},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(tt.cmd, " ", "_"), func(t *testing.T) {
			t.Parallel()

			out := rawRequest(t, rig.h, http.MethodPost, "/aws_mwaa/cli", tt.cmd,
				map[string]string{"Authorization": "Bearer " + tok})

			if tt.allowed {
				assert.Equal(t, http.StatusOK, out.Code, out.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, out.Code, out.Body.String())
			assert.Contains(t, out.Body.String(), "not supported by Amazon MWAA")
		})
	}

	for _, c := range rig.rt.cmds {
		assert.NotEqual(t, "standalone", c[1])
	}
}

func TestAirflowAuth_SSOLoginAndRest(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.createAvailable(t, "auth")

	spec := rig.rt.startedSpecs()[0]
	pass := envValue(spec, "MWAA_ADMIN_PASSWORD")
	require.NotEmpty(t, pass)

	cfg, err := os.ReadFile(filepath.Join(stageOf(spec), "webserver_config.py"))
	require.NoError(t, err)
	assert.NotContains(t, string(cfg), "AUTH_ROLE_PUBLIC", "anonymous access must stay disabled")
	assert.Empty(t, envValue(spec, "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"))
	assert.True(t, strings.HasPrefix(spec.Ports[0], "127.0.0.1:"), spec.Ports[0])
	assert.Contains(t, scriptOf(spec), "unset MWAA_ADMIN_PASSWORD")

	desc, err := rig.b.GetEnvironment(t.Context(), "auth")
	require.NoError(t, err)
	raw, _ := json.Marshal(desc)
	assert.NotContains(t, string(raw), pass)

	rec := doMWAARequest(t, rig.h, http.MethodPost, "/webtoken/auth", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tok map[string]string

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tok))

	sso := rawRequest(t, rig.h, http.MethodGet, "/aws_mwaa/aws-console-sso?login=true&token="+tok["WebToken"], "", nil)
	require.Equal(t, http.StatusFound, sso.Code)
	assert.Contains(t, sso.Header().Get("Set-Cookie"), "session=authed")
	assert.NotContains(t, sso.Header().Get("Location"), pass)
	assert.NotContains(t, sso.Body.String(), pass)

	_, form := rig.ht.lastLogin()
	assert.Contains(t, form, "username=admin")
	assert.Contains(t, form, "password="+pass)
	assert.Contains(t, form, "csrf_token=csrf-123")
	assert.Contains(t, form, "session=anon", "login must replay the pre-login session cookie")

	rest := doMWAARequest(t, rig.h, http.MethodPost, "/restapi/auth", map[string]any{"Method": "GET", "Path": "/dags"})
	require.Equal(t, http.StatusOK, rest.Code, rest.Body.String())

	auth, _ := rig.ht.lastLogin()
	assert.True(t, strings.HasPrefix(auth, "Basic "), auth)
}

func TestAirflowAuth_BrowserTarget(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.createAvailable(t, "host")

	rec := doMWAARequest(t, rig.h, http.MethodPost, "/webtoken/host", nil)

	var tok map[string]string

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tok))

	env, err := rig.b.GetEnvironment(t.Context(), "host")
	require.NoError(t, err)

	port := env.WebserverURL[strings.LastIndex(env.WebserverURL, ":")+1:]

	req := rawRequestHost(t, rig, "localhost:4566", "/aws_mwaa/aws-console-sso?login=true&token="+tok["WebToken"])
	assert.Equal(t, "http://localhost:"+port, req.Header().Get("Location"))
}
