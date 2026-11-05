package codebuild

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const dockerGatewayHost = "172.17.0.1"

// serveS3 runs the in-process S3 emulator on all interfaces so build containers can reach it.
func serveS3(t *testing.T) (*s3backend.InMemoryBackend, string) {
	t.Helper()

	bk := s3backend.NewInMemoryBackend(nil)
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(s3backend.NewHandler(bk)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: e, ReadHeaderTimeout: dockerHeaderTimeout}

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	_, port, _ := net.SplitHostPort(ln.Addr().String())

	return bk, "http://" + net.JoinHostPort(dockerGatewayHost, port)
}

func TestDockerEngineAWSCLIReachesEmulator(t *testing.T) {
	t.Parallel()

	const cliImage = "public.ecr.aws/aws-cli/aws-cli:latest"

	h := dockerEngineHarness(t)
	pullImage(t, cliImage)

	bk, endpoint := serveS3(t)
	h.b.SetEngineServices(EngineServices{
		Store: h.store, Logs: h.logs, Values: fakeValues{}, Endpoint: endpoint, Credentials: &fakeIssuer{},
	})

	spec := `version: 0.2
phases:
  build:
    commands:
      - aws s3 mb s3://cb-emulator-bkt
      - echo from-build > f.txt
      - aws s3 cp f.txt s3://cb-emulator-bkt/f.txt
`
	cfg := specProject(spec)
	cfg.Environment = &ProjectEnvironment{Type: "LINUX_CONTAINER", Image: cliImage, ComputeType: "BUILD_GENERAL1_SMALL"}
	h.project(t, cfg)

	b := h.run(t, StartBuildConfig{})
	require.Equal(t, "SUCCEEDED", b.BuildStatus, "%s", h.logs.joined())

	out, err := bk.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("cb-emulator-bkt"), Key: aws.String("f.txt"),
	})
	require.NoError(t, err)

	body, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()

	assert.Equal(t, "from-build\n", string(body))
	assert.NotContains(t, h.logs.joined(), "sekrit")
}

func TestDockerEngineBuildspecFeatures(t *testing.T) {
	t.Parallel()

	spec := `version: 0.2
run-as: nobody
env:
  variables:
    EXPORTED: before
  secrets-manager:
    PW: mysecret:pw
  exported-variables:
    - EXPORTED
    - PW
    - AWS_REGION
phases:
  install:
    run-as: root
    on-failure: CONTINUE
    commands:
      - echo INSTALL_RAN
      - exit 7
  build:
    commands:
      - echo "who=$(id -un) pw=$PW"
      - export EXPORTED=after
`
	h := dockerEngineHarness(t)
	h.project(t, specProject(spec))

	b := h.run(t, StartBuildConfig{})
	logs := h.logs.joined()

	assert.Equal(t, "FAILED", b.BuildStatus, "install failed under CONTINUE still fails the build")
	assert.Equal(t, "FAILED", phaseStatuses(b)["INSTALL"])
	assert.Equal(t, "SUCCEEDED", phaseStatuses(b)["BUILD"], logs)
	assert.Contains(t, logs, "INSTALL_RAN")
	assert.Contains(t, logs, "who=nobody pw=****")
	assert.NotContains(t, logs, "hunter2")
	assert.Equal(t, []ExportedEnvironmentVariable{{Name: "EXPORTED", Value: "after"}}, b.ExportedEnvironmentVariables)
}

func TestDockerEngineExportedVariablesSucceedingBuild(t *testing.T) {
	t.Parallel()

	spec := "version: 0.2\nenv:\n  exported-variables: [OUT_A]\nphases:\n  build:\n    commands:\n" +
		"      - export OUT_A=\"two words\"\n"
	h := dockerEngineHarness(t)
	h.project(t, specProject(spec))

	b := h.run(t, StartBuildConfig{})
	require.Equal(t, "SUCCEEDED", b.BuildStatus, h.logs.joined())
	assert.Equal(t, []ExportedEnvironmentVariable{{Name: "OUT_A", Value: "two words"}}, b.ExportedEnvironmentVariables)
	assert.False(t, strings.Contains(h.logs.joined(), "Failed to read exported"))
}
