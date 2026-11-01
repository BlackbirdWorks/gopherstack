package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

type lambdaEnvDockerAPI struct {
	envs [][]string
	mu   sync.Mutex
}

func (m *lambdaEnvDockerAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *lambdaEnvDockerAPI) ImageList(_ context.Context, _ image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (m *lambdaEnvDockerAPI) ContainerCreate(
	_ context.Context, cfg *dockercontainer.Config, _ *dockercontainer.HostConfig, _ any, _ any, name string,
) (dockercontainer.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.envs = append(m.envs, append([]string(nil), cfg.Env...))

	return dockercontainer.CreateResponse{ID: fmt.Sprintf("c-%s", name)}, nil
}

func (m *lambdaEnvDockerAPI) ContainerStart(_ context.Context, _ string, _ dockercontainer.StartOptions) error {
	return nil
}

func (m *lambdaEnvDockerAPI) ContainerStop(_ context.Context, _ string, _ dockercontainer.StopOptions) error {
	return nil
}

func (m *lambdaEnvDockerAPI) ContainerRemove(_ context.Context, _ string, _ dockercontainer.RemoveOptions) error {
	return nil
}

func (m *lambdaEnvDockerAPI) Ping(_ context.Context) (any, error) { return struct{}{}, nil }
func (m *lambdaEnvDockerAPI) Close() error                        { return nil }

const lambdaEnvTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

func newWiredServices(t *testing.T, cli *CLI) map[string]service.Registerable {
	t.Helper()

	portAlloc := portalloctest.New(t, 50)

	cli.faultStore = chaos.NewFaultStore()
	services, err := initializeServicesT(t, &service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portAlloc,
	})
	require.NoError(t, err)

	return serviceByName(services)
}

func TestInitializeServices_LambdaEndpointWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8123"}
	cli.Lambda.DockerHost = "172.17.0.1"
	byName := newWiredServices(t, cli)

	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	require.True(t, ok)

	lambdaBk, ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	require.True(t, ok)
	t.Cleanup(func() { lambdaBk.Close(context.Background()) })

	assert.Equal(t, "http://172.17.0.1:8123", lambdaBk.EndpointURL())
}

func TestLambdaRuntimeEnvAssumedRoleIdentity(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8124"}
	cli.Lambda.DockerHost = "172.17.0.1"
	byName := newWiredServices(t, cli)

	iamBk := byName["IAM"].(*iambackend.Handler).Backend.(*iambackend.InMemoryBackend)
	stsBk := byName["STS"].(*stsbackend.Handler).Backend.(*stsbackend.InMemoryBackend)

	_, err := iamBk.CreateRole("exec", "/", lambdaEnvTrust, "")
	require.NoError(t, err)

	api := &lambdaEnvDockerAPI{}
	rt := container.NewDockerRuntimeWithAPI(api, container.Config{PoolSize: 1, IdleTimeout: time.Minute})
	pa := portalloctest.New(t, 10)

	bk := lambdabackend.NewInMemoryBackend(rt, pa, lambdabackend.DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { bk.Close(context.Background()) })

	byName["Lambda"] = lambdabackend.NewHandler(bk)
	wireLambdaRuntimeEnv(cli, byName)

	tests := []struct {
		name     string
		role     string
		wantCred bool
	}{
		{name: "trusted_role", role: "arn:aws:iam::000000000000:role/exec", wantCred: true},
		{name: "missing_role", role: "arn:aws:iam::000000000000:role/nope", wantCred: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn := &lambdabackend.FunctionConfiguration{
				FunctionName: "fn-" + tt.name, PackageType: lambdabackend.PackageTypeImage, ImageURI: "x:1",
				Role: tt.role, Timeout: 30,
			}
			require.NoError(t, bk.CreateFunction(fn))

			_, _, invokeErr := bk.InvokeFunction(
				t.Context(), fn.FunctionName, lambdabackend.InvocationTypeEvent, []byte(`{}`))
			require.NoError(t, invokeErr)

			api.mu.Lock()
			env := map[string]string{}

			for _, e := range api.envs {
				if !slices.Contains(e, "AWS_LAMBDA_FUNCTION_NAME="+fn.FunctionName) {
					continue
				}

				for _, kv := range e {
					k, v, _ := strings.Cut(kv, "=")
					env[k] = v
				}
			}
			api.mu.Unlock()

			require.NotEmpty(t, env)

			assert.Equal(t, "http://172.17.0.1:8124", env["AWS_ENDPOINT_URL"])

			if !tt.wantCred {
				assert.NotContains(t, env, "AWS_ACCESS_KEY_ID")

				return
			}

			require.NotEmpty(t, env["AWS_SESSION_TOKEN"])

			id, idErr := stsBk.GetCallerIdentity(env["AWS_ACCESS_KEY_ID"], env["AWS_SESSION_TOKEN"])
			require.NoError(t, idErr)
			assert.Equal(t, "arn:aws:sts::000000000000:assumed-role/exec/fn-trusted_role",
				id.GetCallerIdentityResult.Arn)
		})
	}
}
