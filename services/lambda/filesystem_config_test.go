package lambda_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	gophercontainer "github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	fsConfigPortBase = 20900
	testAccessPoint  = "arn:aws:elasticfilesystem:us-east-1:000000000000:access-point/fsap-0123456789abcdef0"
	fsVPCBody        = `"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]}`
)

var errNoMountTarget = errors.New("no mount target")

type fakeEFS struct {
	checkErr error
	mounts   map[string]lambda.EFSMount
}

func (f *fakeEFS) CheckAccessPoint(string, []string) error { return f.checkErr }

func (f *fakeEFS) ResolveMount(arn string) (lambda.EFSMount, error) {
	m, ok := f.mounts[arn]
	if !ok {
		return lambda.EFSMount{}, errNoMountTarget
	}

	return m, nil
}

type bindCapturingAPI struct {
	*envCapturingAPI

	binds [][]string
	bmu   sync.Mutex
}

func (m *bindCapturingAPI) ContainerCreate(
	ctx context.Context,
	cfg *dockercontainer.Config,
	host *dockercontainer.HostConfig,
	a, b any,
	name string,
) (dockercontainer.CreateResponse, error) {
	m.bmu.Lock()
	m.binds = append(m.binds, append([]string(nil), host.Binds...))
	m.bmu.Unlock()

	return m.envCapturingAPI.ContainerCreate(ctx, cfg, host, a, b, name)
}

func fsCreateBody(name, configs, vpc string) string {
	body := fmt.Sprintf(`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"x"},`+
		`"Role":"arn:aws:iam:::role/r"`, name)
	if configs != "" {
		body += `,"FileSystemConfigs":` + configs
	}

	if vpc != "" {
		body += `,` + vpc
	}

	return body + `}`
}

func TestFileSystemConfigValidation(t *testing.T) {
	t.Parallel()

	okCfg := `[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/mnt/data"}]`

	tests := []struct {
		resolver  *fakeEFS
		name      string
		configs   string
		vpc       string
		wantInMsg string
		wantCode  int
	}{
		{name: "valid", configs: okCfg, vpc: fsVPCBody, wantCode: http.StatusCreated},
		{name: "no_vpc", configs: okCfg, wantCode: http.StatusBadRequest, wantInMsg: "VPC"},
		{
			name: "two_configs", vpc: fsVPCBody, wantCode: http.StatusBadRequest, wantInMsg: "fileSystemConfigs",
			configs: `[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/mnt/a"},` +
				`{"Arn":"` + testAccessPoint + `","LocalMountPath":"/mnt/b"}]`,
		},
		{
			name: "file_system_arn", vpc: fsVPCBody, wantCode: http.StatusBadRequest, wantInMsg: "access point",
			configs: `[{"Arn":"arn:aws:elasticfilesystem:us-east-1:000000000000:file-system/fs-1234",` +
				`"LocalMountPath":"/mnt/a"}]`,
		},
		{
			name: "other_service_arn", vpc: fsVPCBody, wantCode: http.StatusBadRequest, wantInMsg: "access point",
			configs: `[{"Arn":"arn:aws:s3:::bucket","LocalMountPath":"/mnt/a"}]`,
		},
		{
			name: "path_outside_mnt", vpc: fsVPCBody, wantCode: http.StatusBadRequest, wantInMsg: "localMountPath",
			configs: `[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/data"}]`,
		},
		{
			name: "path_nested", vpc: fsVPCBody, wantCode: http.StatusBadRequest, wantInMsg: "localMountPath",
			configs: `[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/mnt/a/b"}]`,
		},
		{
			name: "path_bare_mnt", vpc: fsVPCBody, wantCode: http.StatusBadRequest, wantInMsg: "localMountPath",
			configs: `[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/mnt/"}]`,
		},
		{
			name: "efs_rejects", configs: okCfg, vpc: fsVPCBody, wantCode: http.StatusBadRequest,
			wantInMsg: "no mount target", resolver: &fakeEFS{checkErr: errNoMountTarget},
		},
		{name: "efs_accepts", configs: okCfg, vpc: fsVPCBody, wantCode: http.StatusCreated, resolver: &fakeEFS{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			if tt.resolver != nil {
				bk.SetEFSResolver(tt.resolver)
			}

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions",
				fsCreateBody("fs-fn", tt.configs, tt.vpc))
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode != http.StatusCreated {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
				assert.Contains(t, rec.Body.String(), tt.wantInMsg)
			}
		})
	}
}

func TestFileSystemConfigUpdateValidation(t *testing.T) {
	t.Parallel()

	okCfg := `[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/mnt/data"}]`

	tests := []struct {
		name     string
		update   string
		wantCode int
	}{
		{name: "add_with_vpc", update: `{"FileSystemConfigs":` + okCfg + `}`, wantCode: http.StatusOK},
		{
			name:     "drop_vpc_keeps_mount",
			update:   `{"VpcConfig":{"SubnetIds":[],"SecurityGroupIds":[]}}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "bad_path",
			update:   `{"FileSystemConfigs":[{"Arn":"` + testAccessPoint + `","LocalMountPath":"/tmp/x"}]}`,
			wantCode: http.StatusBadRequest,
		},
		{name: "clear", update: `{"FileSystemConfigs":[]}`, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			seed := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions",
				fsCreateBody("upd-fn", okCfg, fsVPCBody))
			require.Equal(t, http.StatusCreated, seed.Code, seed.Body.String())

			rec := callInMemoryHandler(t, h, http.MethodPut,
				"/2015-03-31/functions/upd-fn/configuration", tt.update)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestFileSystemConfigContainerMount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resolver   *fakeEFS
		wantBinds  []string
		wantInvoke bool
		portOffset int
	}{
		{
			name:       "mounted",
			portOffset: 0,
			resolver: &fakeEFS{mounts: map[string]lambda.EFSMount{
				testAccessPoint: {HostDir: "/host/efs/fs-1/app", UID: 1000, GID: 1000},
			}},
			wantBinds:  []string{"/host/efs/fs-1/app:/mnt/data"},
			wantInvoke: true,
		},
		{name: "unresolvable", portOffset: 1, resolver: &fakeEFS{}, wantInvoke: false},
		{name: "no_resolver", portOffset: 2, wantInvoke: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &bindCapturingAPI{envCapturingAPI: &envCapturingAPI{}}
			base := fsConfigPortBase + tt.portOffset*10
			pa, err := portalloc.New(base, base+9)
			require.NoError(t, err)

			rt := gophercontainer.NewDockerRuntimeWithAPI(api,
				gophercontainer.Config{PoolSize: 3, IdleTimeout: time.Minute})
			b := lambda.NewInMemoryBackend(rt, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, b)

			if tt.resolver != nil {
				b.SetEFSResolver(tt.resolver)
			}

			fn := &lambda.FunctionConfiguration{
				FunctionName: "mnt-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Timeout: 30,
				FileSystemConfigs: []*lambda.FileSystemConfig{{Arn: testAccessPoint, LocalMountPath: "/mnt/data"}},
			}
			require.NoError(t, b.CreateFunction(fn))

			_, _, invErr := b.InvokeFunction(t.Context(), "mnt-fn", lambda.InvocationTypeEvent, []byte(`{}`))

			if !tt.wantInvoke {
				require.Error(t, invErr)
				assert.Empty(t, api.binds)

				return
			}

			require.NoError(t, invErr)
			require.Len(t, api.binds, 1)
			assert.Equal(t, tt.wantBinds, api.binds[0])
		})
	}
}
