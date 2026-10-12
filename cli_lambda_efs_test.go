package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

type lambdaBindDockerAPI struct {
	binds map[string][]string
	mu    sync.Mutex
}

func (m *lambdaBindDockerAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *lambdaBindDockerAPI) ImageList(_ context.Context, _ image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (m *lambdaBindDockerAPI) ContainerCreate(
	_ context.Context, cfg *dockercontainer.Config, host *dockercontainer.HostConfig, _ any, _ any, name string,
) (dockercontainer.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, e := range cfg.Env {
		if fnName, ok := strings.CutPrefix(e, "AWS_LAMBDA_FUNCTION_NAME="); ok {
			m.binds[fnName] = append([]string(nil), host.Binds...)
		}
	}

	return dockercontainer.CreateResponse{ID: fmt.Sprintf("c-%s", name)}, nil
}

func (m *lambdaBindDockerAPI) ContainerStart(_ context.Context, _ string, _ dockercontainer.StartOptions) error {
	return nil
}

func (m *lambdaBindDockerAPI) ContainerStop(_ context.Context, _ string, _ dockercontainer.StopOptions) error {
	return nil
}

func (m *lambdaBindDockerAPI) ContainerRemove(_ context.Context, _ string, _ dockercontainer.RemoveOptions) error {
	return nil
}

func (m *lambdaBindDockerAPI) Ping(_ context.Context) (any, error) { return struct{}{}, nil }
func (m *lambdaBindDockerAPI) Close() error                        { return nil }

func callLambdaHandler(t *testing.T, h *lambdabackend.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func lambdaEFSFunctionBody(name, apARN, mountPath string, subnets ...string) string {
	quoted := make([]string, 0, len(subnets))
	for _, s := range subnets {
		quoted = append(quoted, `"`+s+`"`)
	}

	return fmt.Sprintf(`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"x:1"},`+
		`"Role":"arn:aws:iam::000000000000:role/r","Timeout":30,`+
		`"VpcConfig":{"SubnetIds":[%s],"SecurityGroupIds":["sg-1"]},`+
		`"FileSystemConfigs":[{"Arn":%q,"LocalMountPath":%q}]}`,
		name, strings.Join(quoted, ","), apARN, mountPath)
}

func TestLambdaEFSAccessPointMount(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8125"}
	byName := newWiredServices(t, cli, 19600)

	ec2H := byName["EC2"].(*ec2backend.Handler)
	efsBk := byName["EFS"].(*efsbackend.Handler).Backend
	efsBk.SetStorageRoot(t.TempDir())

	vpc, err := ec2H.Backend.CreateVpc("10.9.0.0/16", "default")
	require.NoError(t, err)

	subnet, err := ec2H.Backend.CreateSubnet(vpc.ID, "10.9.1.0/24", "us-east-1a")
	require.NoError(t, err)

	otherVPC, err := ec2H.Backend.CreateVpc("10.8.0.0/16", "default")
	require.NoError(t, err)

	otherSubnet, err := ec2H.Backend.CreateSubnet(otherVPC.ID, "10.8.1.0/24", "us-east-1a")
	require.NoError(t, err)

	ctx := t.Context()
	fs, err := efsBk.CreateFileSystem(ctx, efsbackend.CreateFileSystemRequest{CreationToken: "lambda-efs"})
	require.NoError(t, err)

	_, err = efsBk.CreateMountTarget(ctx, efsbackend.CreateMountTargetRequest{
		FileSystemID: fs.FileSystemID, SubnetID: subnet.ID,
	})
	require.NoError(t, err)

	ap, err := efsBk.CreateAccessPoint(ctx, efsbackend.CreateAccessPointRequest{
		FileSystemID: fs.FileSystemID,
		PosixUser:    &efsbackend.PosixUser{UID: 1000, GID: 1000},
		RootDirectory: &efsbackend.RootDirectory{
			Path:         "/lambda",
			CreationInfo: &efsbackend.CreationInfo{OwnerUID: 1000, OwnerGID: 1000, Permissions: "755"},
		},
	})
	require.NoError(t, err)

	api := &lambdaBindDockerAPI{binds: map[string][]string{}}
	rt := container.NewDockerRuntimeWithAPI(api, container.Config{PoolSize: 2, IdleTimeout: time.Minute})
	pa, err := portalloc.New(19660, 19669)
	require.NoError(t, err)

	bk := lambdabackend.NewInMemoryBackend(rt, pa, lambdabackend.DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { bk.Close(context.Background()) })

	lambdaH := lambdabackend.NewHandler(bk)
	lambdaH.DefaultRegion = "us-east-1"
	lambdaH.AccountID = "000000000000"
	byName["Lambda"] = lambdaH
	wireLambdaEFS(cli, byName)

	for _, fn := range []string{"efs-writer", "efs-reader"} {
		rec := callLambdaHandler(t, lambdaH, http.MethodPost, "/2015-03-31/functions",
			lambdaEFSFunctionBody(fn, ap.AccessPointArn, "/mnt/shared", subnet.ID))
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

		_, _, invErr := bk.InvokeFunction(t.Context(), fn, lambdabackend.InvocationTypeEvent, []byte(`{}`))
		require.NoError(t, invErr)
	}

	rejected := []struct {
		name   string
		arn    string
		subnet string
	}{
		{name: "wrong_vpc", arn: ap.AccessPointArn, subnet: otherSubnet.ID},
		{
			name: "missing_ap", subnet: subnet.ID,
			arn: "arn:aws:elasticfilesystem:us-east-1:000000000000:access-point/fsap-00000000000000000",
		},
	}

	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := callLambdaHandler(t, lambdaH, http.MethodPost, "/2015-03-31/functions",
				lambdaEFSFunctionBody("efs-"+tt.name, tt.arn, "/mnt/shared", tt.subnet))
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}

	api.mu.Lock()
	defer api.mu.Unlock()

	writerBinds, readerBinds := api.binds["efs-writer"], api.binds["efs-reader"]
	require.Len(t, writerBinds, 1)
	assert.Equal(t, writerBinds, readerBinds)

	hostDir, target, ok := strings.Cut(writerBinds[0], ":")
	require.True(t, ok)
	assert.Equal(t, "/mnt/shared", target)
	assert.Equal(t, "lambda", filepath.Base(hostDir))
	require.NoError(t, os.WriteFile(filepath.Join(hostDir, "shared.txt"), []byte("from writer"), 0o600))

	m, err := efsBk.ResolveAccessPointMount(ap.AccessPointArn)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(m.HostDir, "shared.txt"))
	require.NoError(t, err)
	assert.Equal(t, "from writer", string(data))
}

func TestInitializeServicesWiresLambdaEFS(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8126"}
	byName := newWiredServices(t, cli, 19700)

	lambdaH := byName["Lambda"].(*lambdabackend.Handler)
	t.Cleanup(func() { lambdaH.Backend.(*lambdabackend.InMemoryBackend).Close(context.Background()) })

	rec := callLambdaHandler(t, lambdaH, http.MethodPost, "/2015-03-31/functions",
		lambdaEFSFunctionBody("wired-fn",
			"arn:aws:elasticfilesystem:us-east-1:000000000000:access-point/fsap-00000000000000000",
			"/mnt/x", "subnet-1"))
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "AccessPointNotFound")
}
