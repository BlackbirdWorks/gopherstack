package gcpfunctions_test

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func secretStore() func(string) ([]byte, error) {
	data := map[string]string{
		"projects/test-project/secrets/api-key/versions/1":      "one",
		"projects/test-project/secrets/api-key/versions/latest": "two",
		"projects/other/secrets/shared/versions/latest":         "shared-value",
	}

	return func(name string) ([]byte, error) {
		if strings.Contains(name, "/off/") {
			return nil, status.Errorf(codes.FailedPrecondition, "Secret Version [%s] is in DISABLED state.", name)
		}

		if v, ok := data[name]; ok {
			return []byte(v), nil
		}

		return nil, status.Errorf(codes.NotFound, "Secret Version [%s] not found.", name)
	}
}

func (e *env) createErr(t *testing.T, id string, fn *functionspb.Function) error {
	t.Helper()

	op, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: id, Function: fn,
	})
	if err != nil {
		return err
	}

	_, err = op.Wait(t.Context())

	return err
}

func TestSecrets_EnvAndVolumes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.handler.Backend.SetSecretResolver(secretStore())

	spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
	spec.ServiceConfig.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{
		{Key: "PINNED", Secret: "api-key", Version: "1"},
		{Key: "LATEST", Secret: "api-key", Version: "latest"},
		{Key: "UNSET", Secret: "api-key"},
		{Key: "CROSS", ProjectId: "other", Secret: "shared", Version: "latest"},
	}
	spec.ServiceConfig.SecretVolumes = []*functionspb.SecretVolume{
		{MountPath: "/etc/secrets", Secret: "api-key", Versions: []*functionspb.SecretVolume_SecretVersion{
			{Version: "1", Path: "old/key"}, {Version: "latest", Path: "key"},
		}},
		{MountPath: "/etc/bare", Secret: "api-key"},
	}

	fn := e.createFunction(t, "sec", spec)
	code, body := e.call(t, http.MethodGet, hostOf(fn), "/", "")
	require.Equal(t, http.StatusOK, code)

	got := decode(t, body)
	assert.Equal(t, "one", got.Env["PINNED"])
	assert.Equal(t, "two", got.Env["LATEST"])
	assert.Equal(t, "two", got.Env["UNSET"])
	assert.Equal(t, "shared-value", got.Env["CROSS"])

	mounts := map[string]string{}

	for _, m := range e.rt.specs()[0].Mounts {
		parts := strings.Split(m, ":")
		mounts[parts[1]] = parts[0]

		if parts[1] != "/src" {
			assert.Equal(t, "ro", parts[2])
		}
	}

	tests := []struct {
		mount, file, want string
	}{
		{"/etc/secrets", "old/key", "one"},
		{"/etc/secrets", "key", "two"},
		{"/etc/bare", "api-key", "two"},
	}

	for _, tt := range tests {
		raw, err := os.ReadFile(filepath.Join(mounts[tt.mount], tt.file))
		require.NoError(t, err)
		assert.Equal(t, tt.want, string(raw))
	}
}

func TestSecrets_DeployFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		secrets  func(string) ([]byte, error)
		env      []*functionspb.SecretEnvVar
		vols     []*functionspb.SecretVolume
		wantText string
	}{
		{
			"missing secret", secretStore(),
			[]*functionspb.SecretEnvVar{{Key: "K", Secret: "nope", Version: "latest"}}, nil,
			"Secret Version [projects/test-project/secrets/nope/versions/latest] not found.",
		},
		{
			"missing version", secretStore(),
			[]*functionspb.SecretEnvVar{{Key: "K", Secret: "api-key", Version: "9"}}, nil,
			"versions/9] not found.",
		},
		{
			"disabled version", secretStore(),
			[]*functionspb.SecretEnvVar{{Key: "K", Secret: "off", Version: "1"}}, nil,
			"is in DISABLED state.",
		},
		{
			"missing volume secret", secretStore(), nil,
			[]*functionspb.SecretVolume{{MountPath: "/etc/s", Secret: "nope"}},
			"not found.",
		},
		{"no secret manager", nil, []*functionspb.SecretEnvVar{{Key: "K", Secret: "api-key"}}, nil, "Secret Manager"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			if tt.secrets != nil {
				e.handler.Backend.SetSecretResolver(tt.secrets)
			}

			spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
			spec.ServiceConfig.SecretEnvironmentVariables = tt.env
			spec.ServiceConfig.SecretVolumes = tt.vols

			err := e.createErr(t, "bad", spec)
			require.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Contains(t, err.Error(), tt.wantText)
			assert.Zero(t, e.rt.running())
		})
	}
}

func TestSecrets_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  func(sc *functionspb.ServiceConfig)
	}{
		{"bad key", func(sc *functionspb.ServiceConfig) {
			sc.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "1BAD", Secret: "s"}}
		}},
		{"reserved key", func(sc *functionspb.ServiceConfig) {
			sc.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "PORT", Secret: "s"}}
		}},
		{"duplicate with plain env", func(sc *functionspb.ServiceConfig) {
			sc.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "GREETING", Secret: "s"}}
		}},
		{"resource name as secret", func(sc *functionspb.ServiceConfig) {
			sc.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "K", Secret: "projects/p/secrets/s"}}
		}},
		{"bad version", func(sc *functionspb.ServiceConfig) {
			sc.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "K", Secret: "s", Version: "newest"}}
		}},
		{"mount under proc", func(sc *functionspb.ServiceConfig) {
			sc.SecretVolumes = []*functionspb.SecretVolume{{MountPath: "/proc/x", Secret: "s"}}
		}},
		{"relative mount", func(sc *functionspb.ServiceConfig) {
			sc.SecretVolumes = []*functionspb.SecretVolume{{MountPath: "etc/s", Secret: "s"}}
		}},
		{"duplicate mount", func(sc *functionspb.ServiceConfig) {
			sc.SecretVolumes = []*functionspb.SecretVolume{
				{MountPath: "/etc/s", Secret: "a"}, {MountPath: "/etc/s/", Secret: "b"},
			}
		}},
		{"escaping path", func(sc *functionspb.ServiceConfig) {
			sc.SecretVolumes = []*functionspb.SecretVolume{{MountPath: "/etc/s", Secret: "s",
				Versions: []*functionspb.SecretVolume_SecretVersion{{Version: "1", Path: ""}}}}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
			tt.set(spec.ServiceConfig)

			err := e.createErr(t, "bad", spec)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}
