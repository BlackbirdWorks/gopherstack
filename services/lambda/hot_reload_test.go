package lambda_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"

	gophercontainer "github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

type bindRecordingAPI struct {
	*mockDockerAPI

	binds [][]string
	mu    sync.Mutex
}

func (m *bindRecordingAPI) ContainerCreate(
	ctx context.Context,
	cfg *container.Config,
	host *container.HostConfig,
	a, b any,
	name string,
) (container.CreateResponse, error) {
	m.mu.Lock()
	m.binds = append(m.binds, append([]string(nil), host.Binds...))
	m.mu.Unlock()

	return m.mockDockerAPI.ContainerCreate(ctx, cfg, host, a, b, name)
}

func (m *bindRecordingAPI) created() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([][]string(nil), m.binds...)
}

func newHotReloadBackend(t *testing.T, rangeStart int, s lambda.Settings) (*lambda.InMemoryBackend, *bindRecordingAPI) {
	t.Helper()

	api := &bindRecordingAPI{mockDockerAPI: &mockDockerAPI{}}
	dc := gophercontainer.NewDockerRuntimeWithAPI(api, gophercontainer.Config{PoolSize: 3, IdleTimeout: time.Minute})

	pa, err := portalloc.New(rangeStart, rangeStart+20)
	require.NoError(t, err)

	return closeBackend(t, lambda.NewInMemoryBackend(dc, pa, s, "000000000000", "us-east-1")), api
}

func hotReloadFn(name, key string) *lambda.FunctionConfiguration {
	return &lambda.FunctionConfiguration{
		FunctionName: name,
		PackageType:  lambda.PackageTypeZip,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Timeout:      3,
		S3BucketCode: "hot-reload",
		S3KeyCode:    key,
	}
}

func TestHotReload_RecyclesRuntimeOnChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate         func(t *testing.T, dir string)
		name           string
		portStart      int
		wantContainers int
	}{
		{
			name:           "unchanged_reuses_container",
			portStart:      22000,
			mutate:         func(*testing.T, string) {},
			wantContainers: 1,
		},
		{
			name:      "edited_file_recycles",
			portStart: 22030,
			mutate: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filepath.Join(dir, "index.py"), []byte("v2 changed"), 0o600))
			},
			wantContainers: 2,
		},
		{
			name:      "new_file_recycles",
			portStart: 22060,
			mutate: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filepath.Join(dir, "helper.py"), []byte("x"), 0o600))
			},
			wantContainers: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "index.py"), []byte("v1"), 0o600))

			s := lambda.DefaultSettings()
			s.HotReloadIntervalMS = 0
			bk, api := newHotReloadBackend(t, tt.portStart, s)

			require.NoError(t, bk.CreateFunction(hotReloadFn("hot-fn", dir)))

			invoke := func() {
				_, code, err := bk.InvokeFunction(t.Context(), "hot-fn", lambda.InvocationTypeEvent, []byte(`{}`))
				require.NoError(t, err)
				require.Equal(t, http.StatusAccepted, code)
			}

			invoke()
			tt.mutate(t, dir)
			invoke()

			created := api.created()
			require.Len(t, created, tt.wantContainers)
			assert.Equal(t, []string{dir + ":/var/task:ro"}, created[0])
		})
	}
}

func TestHotReload_ScanIntervalBoundsChecks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.py"), []byte("v1"), 0o600))

	s := lambda.DefaultSettings()
	s.HotReloadIntervalMS = 3_600_000

	bk, api := newHotReloadBackend(t, 19600, s)
	require.NoError(t, bk.CreateFunction(hotReloadFn("hot-fn", dir)))

	for range 2 {
		_, _, err := bk.InvokeFunction(t.Context(), "hot-fn", lambda.InvocationTypeEvent, []byte(`{}`))
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(filepath.Join(dir, "index.py"), []byte("changed"), 0o600))
	}

	assert.Len(t, api.created(), 1)
}

func TestHotReload_NormalS3FunctionUnaffected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		disabled bool
		wantHot  bool
	}{
		{name: "magic_bucket", bucket: "hot-reload", wantHot: true},
		{name: "ordinary_bucket", bucket: "my-bucket"},
		{name: "magic_bucket_disabled", bucket: "hot-reload", disabled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := lambda.DefaultSettings()
			s.DisableHotReload = tt.disabled
			bk := closeBackend(t, lambda.NewInMemoryBackend(nil, nil, s, "000000000000", "us-east-1"))
			h := lambda.NewHandler(bk)

			assert.Equal(t, tt.wantHot, bk.IsHotReloadBucket(tt.bucket))

			hot, err := bk.ValidateHotReloadCode(tt.bucket, "relative/not/valid")
			assert.Equal(t, tt.wantHot, hot)
			assert.Equal(t, tt.wantHot, err != nil)

			body := fmt.Sprintf(`{"FunctionName":"f","PackageType":"Zip","Runtime":"python3.12",`+
				`"Handler":"index.handler","Role":"arn:aws:iam:::role/r",`+
				`"Code":{"S3Bucket":%q,"S3Key":"relative/key.zip"}}`, tt.bucket)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)

			if tt.wantHot {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusCreated, rec.Code)
			assert.NotEqual(t, "hot-reloading-hash-not-available", lambdaParseBody(t, rec)["CodeSha256"])
		})
	}
}

func TestHotReload_HandlerValidationAndDigest(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "f.py")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	valid := t.TempDir()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{name: "absolute_dir", key: valid, wantStatus: http.StatusCreated},
		{name: "relative", key: "code/dir", wantStatus: http.StatusBadRequest},
		{name: "traversal", key: valid + "/../../etc", wantStatus: http.StatusBadRequest},
		{name: "missing", key: valid + "/nope", wantStatus: http.StatusBadRequest},
		{name: "file_not_dir", key: file, wantStatus: http.StatusBadRequest},
		{name: "unset_env", key: "$GOPHERSTACK_HOT_RELOAD_UNSET/x", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			body := fmt.Sprintf(`{"FunctionName":"hot","PackageType":"Zip","Runtime":"python3.12",`+
				`"Handler":"index.handler","Role":"arn:aws:iam:::role/r",`+
				`"Code":{"S3Bucket":"hot-reload","S3Key":%q}}`, tt.key)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus != http.StatusCreated {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")

				return
			}

			out := lambdaParseBody(t, rec)
			assert.Equal(t, "hot-reloading-hash-not-available", out["CodeSha256"])
			assert.InDelta(t, 0, out["CodeSize"], 0)

			get := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/hot", "")
			require.Equal(t, http.StatusOK, get.Code)
			assert.Equal(t, "hot-reloading-hash-not-available",
				lambdaParseBody(t, get)["Configuration"].(map[string]any)["CodeSha256"])
		})
	}
}

// t.Setenv forbids t.Parallel, so the placeholder cases live in their own test.
func TestHotReload_EnvPlaceholders(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOPHERSTACK_HOT_RELOAD_TEST_DIR", dir)

	for _, key := range []string{"$GOPHERSTACK_HOT_RELOAD_TEST_DIR", "${GOPHERSTACK_HOT_RELOAD_TEST_DIR}"} {
		got, err := lambda.ResolveHotReloadPath(key)
		require.NoError(t, err)
		assert.Equal(t, dir, got)
	}
}

func TestHotReload_UpdateFunctionCode(t *testing.T) {
	t.Parallel()

	valid := t.TempDir()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{name: "valid", key: valid, wantStatus: http.StatusOK},
		{name: "relative", key: "rel/dir", wantStatus: http.StatusBadRequest},
		{name: "traversal", key: valid + "/../x", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", baseZipFn("fn"))
			require.Equal(t, http.StatusCreated, rec.Code)

			body := fmt.Sprintf(`{"S3Bucket":"hot-reload","S3Key":%q}`, tt.key)
			up := callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/functions/fn/code", body)
			require.Equal(t, tt.wantStatus, up.Code, up.Body.String())

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, "hot-reloading-hash-not-available", lambdaParseBody(t, up)["CodeSha256"])
			}
		})
	}
}

func makeSocketFile(t *testing.T, dir string) {
	t.Helper()

	require.NoError(t, syscall.Mknod(filepath.Join(dir, "s"), syscall.S_IFSOCK|0o600, 0))
}

func TestHotReload_RejectsUnsafeMounts(t *testing.T) {
	t.Parallel()

	linkTo := func(target string) func(t *testing.T) string {
		return func(t *testing.T) string {
			t.Helper()

			link := filepath.Join(t.TempDir(), "link")
			require.NoError(t, os.Symlink(target, link))

			return link
		}
	}

	withSocket := func(t *testing.T) string {
		t.Helper()

		dir := t.TempDir()
		makeSocketFile(t, dir)

		return dir
	}

	tests := []struct {
		path func(t *testing.T) string
		name string
	}{
		{name: "root", path: func(*testing.T) string { return "/" }},
		{name: "proc", path: func(*testing.T) string { return "/proc" }},
		{name: "sys", path: func(*testing.T) string { return "/sys" }},
		{name: "dev", path: func(*testing.T) string { return "/dev" }},
		{name: "run", path: func(*testing.T) string { return "/run" }},
		{name: "var_run", path: func(*testing.T) string { return "/var/run" }},
		{name: "etc", path: func(*testing.T) string { return "/etc" }},
		{name: "etc_subdir", path: func(*testing.T) string { return "/etc/ssl" }},
		{name: "symlink_to_etc", path: linkTo("/etc")},
		{name: "symlink_to_run", path: linkTo("/run")},
		{name: "socket_in_tree", path: withSocket},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := tt.path(t)
			if _, statErr := os.Stat(path); statErr != nil {
				t.Skipf("%s not present on this host", path)
			}

			bk := closeBackend(t, lambda.NewInMemoryBackend(
				nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1",
			))
			_, err := bk.ValidateHotReloadCode("hot-reload", path)
			require.ErrorIs(t, err, lambda.ErrInvalidHotReloadPath)

			h := lambda.NewHandler(bk)
			body := fmt.Sprintf(`{"FunctionName":"f","PackageType":"Zip","Runtime":"python3.12",`+
				`"Handler":"index.handler","Role":"arn:aws:iam:::role/r",`+
				`"Code":{"S3Bucket":"hot-reload","S3Key":%q}}`, path)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
		})
	}
}

func TestHotReload_SymlinkResolvedAndMounted(t *testing.T) {
	t.Parallel()

	target := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(target, "index.py"), []byte("v1"), 0o600))

	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(target, link))

	want, err := filepath.EvalSymlinks(target)
	require.NoError(t, err)

	bk, api := newHotReloadBackend(t, 19700, lambda.DefaultSettings())
	require.NoError(t, bk.CreateFunction(hotReloadFn("sym-fn", link)))

	_, _, err = bk.InvokeFunction(t.Context(), "sym-fn", lambda.InvocationTypeEvent, []byte(`{}`))
	require.NoError(t, err)
	require.Len(t, api.created(), 1)
	assert.Equal(t, []string{want + ":/var/task:ro"}, api.created()[0])
}

func TestHotReload_SocketAppearingAfterStartFailsInvoke(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.py"), []byte("v1"), 0o600))

	s := lambda.DefaultSettings()
	s.HotReloadIntervalMS = 0

	bk, api := newHotReloadBackend(t, 19730, s)
	require.NoError(t, bk.CreateFunction(hotReloadFn("sock-fn", dir)))

	_, _, err := bk.InvokeFunction(t.Context(), "sock-fn", lambda.InvocationTypeEvent, []byte(`{}`))
	require.NoError(t, err)

	makeSocketFile(t, dir)

	_, _, err = bk.InvokeFunction(t.Context(), "sock-fn", lambda.InvocationTypeEvent, []byte(`{}`))
	require.ErrorIs(t, err, lambda.ErrInvalidHotReloadPath)
	assert.Len(t, api.created(), 1)
}

func TestHotReload_RootAllowlist(t *testing.T) {
	t.Parallel()

	rootA, rootB, outside := t.TempDir(), t.TempDir(), t.TempDir()
	sub := filepath.Join(rootA, "sub")
	require.NoError(t, os.Mkdir(sub, 0o750))

	escape := filepath.Join(rootA, "escape")
	require.NoError(t, os.Symlink(outside, escape))

	sibling := rootA + "x"
	require.NoError(t, os.Mkdir(sibling, 0o750))
	t.Cleanup(func() { _ = os.Remove(sibling) })

	inside := filepath.Join(rootB, "link")
	require.NoError(t, os.Symlink(sub, inside))

	tests := []struct {
		name    string
		key     string
		roots   []string
		wantErr bool
	}{
		{name: "inside_root", key: sub, roots: []string{rootA}},
		{name: "root_itself", key: rootA, roots: []string{rootA}},
		{name: "outside_every_root", key: outside, roots: []string{rootA, rootB}, wantErr: true},
		{name: "symlink_escaping_root", key: escape, roots: []string{rootA}, wantErr: true},
		{name: "symlink_within_other_root", key: inside, roots: []string{rootA}},
		{name: "second_root", key: rootB, roots: []string{rootA, rootB}},
		{name: "missing_root_skipped", key: sub, roots: []string{filepath.Join(rootA, "nope"), rootA}},
		{name: "no_usable_roots", key: sub, roots: []string{filepath.Join(rootA, "nope")}, wantErr: true},
		{name: "sibling_prefix", key: sibling, roots: []string{rootA}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := lambda.ResolveHotReloadPathIn(tt.key, tt.roots)
			if tt.wantErr {
				require.ErrorIs(t, err, lambda.ErrInvalidHotReloadPath)
				assert.Contains(t, err.Error(), "LAMBDA_HOT_RELOAD_ROOTS")

				return
			}

			require.NoError(t, err)
			assert.DirExists(t, got)
		})
	}
}

func TestHotReload_DefaultRootsAndEnv(t *testing.T) {
	outside := t.TempDir()
	t.Setenv("LAMBDA_HOT_RELOAD_ROOTS", filepath.Join(outside, "other")+string(os.PathListSeparator)+outside)

	got, err := lambda.ResolveHotReloadPath(outside)
	require.NoError(t, err)
	assert.Equal(t, outside, got)

	t.Setenv("LAMBDA_HOT_RELOAD_ROOTS", t.TempDir())
	_, err = lambda.ResolveHotReloadPath(outside)
	require.ErrorIs(t, err, lambda.ErrInvalidHotReloadPath)

	t.Setenv("LAMBDA_HOT_RELOAD_ROOTS", "")
	got, err = lambda.ResolveHotReloadPath(outside)
	require.NoError(t, err)
	assert.Equal(t, outside, got)
}
