package app

import (
	"archive/zip"
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const efsDockerRuntimeImage = "public.ecr.aws/lambda/python:3.12"

const efsDockerHandler = `import json

def writer(event, context):
    with open("/mnt/shared/note.txt", "w") as f:
        f.write(event["value"])
    return {"ok": True}

def reader(event, context):
    with open("/mnt/shared/note.txt") as f:
        return {"value": f.read()}
`

func efsDockerZip(t *testing.T) []byte { return zipOf(t, "index.py", efsDockerHandler) }

func zipOf(t *testing.T, name, src string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	require.NoError(t, err)
	_, err = w.Write([]byte(src))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestLambdaEFSSharedAcrossFunctionsDocker(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker test")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", efsDockerRuntimeImage).Run(); err != nil {
		t.Skipf("lambda runtime image %s not available locally: %v", efsDockerRuntimeImage, err)
	}

	rt, err := container.NewRuntime(container.Config{
		Logger: slog.Default(), PoolSize: 2, IdleTimeout: time.Minute,
	})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8127"}
	byName := newWiredServices(t, cli)

	efsBk := byName["EFS"].(*efsbackend.Handler).Backend
	efsBk.SetStorageRoot(t.TempDir())

	fs, err := efsBk.CreateFileSystem(t.Context(), efsbackend.CreateFileSystemRequest{CreationToken: "docker-efs"})
	require.NoError(t, err)

	ap, err := efsBk.CreateAccessPoint(t.Context(), efsbackend.CreateAccessPointRequest{
		FileSystemID: fs.FileSystemID,
		RootDirectory: &efsbackend.RootDirectory{
			Path:         "/shared",
			CreationInfo: &efsbackend.CreationInfo{OwnerUID: 1000, OwnerGID: 1000, Permissions: "777"},
		},
	})
	require.NoError(t, err)

	pa := portalloctest.New(t, 20)

	bk := lambdabackend.NewInMemoryBackend(rt, pa, lambdabackend.DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { bk.Close(context.Background()) })

	byName["Lambda"] = lambdabackend.NewHandler(bk)
	wireLambdaEFS(cli, byName)

	for name, handler := range map[string]string{"efs-writer": "index.writer", "efs-reader": "index.reader"} {
		require.NoError(t, bk.CreateFunction(&lambdabackend.FunctionConfiguration{
			FunctionName: name,
			PackageType:  lambdabackend.PackageTypeZip,
			Runtime:      "python3.12",
			Handler:      handler,
			ZipData:      efsDockerZip(t),
			Timeout:      60,
			FileSystemConfigs: []*lambdabackend.FileSystemConfig{
				{Arn: ap.AccessPointArn, LocalMountPath: "/mnt/shared"},
			},
		}))
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	_, status, err := bk.InvokeFunction(ctx, "efs-writer", lambdabackend.InvocationTypeRequestResponse,
		[]byte(`{"value":"hello from writer"}`))
	require.NoError(t, err)
	require.Equal(t, 200, status)

	out, status, err := bk.InvokeFunction(ctx, "efs-reader", lambdabackend.InvocationTypeRequestResponse, []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, 200, status)
	assert.JSONEq(t, `{"value":"hello from writer"}`, string(out))
}

const kmsEFSDockerHandler = `import json, os

def handler(event, context):
    with open("/mnt/shared/who.txt", "w") as f:
        f.write("x")
    return {"secret": os.environ.get("DB_PASSWORD"), "uid": os.getuid(), "gid": os.getgid(),
            "groups": sorted(os.getgroups())}
`

func TestLambdaKMSEnvAndEFSPosixUserDocker(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker test")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", efsDockerRuntimeImage).Run(); err != nil {
		t.Skipf("lambda runtime image %s not available locally: %v", efsDockerRuntimeImage, err)
	}

	rt, err := container.NewRuntime(container.Config{
		Logger: slog.Default(), PoolSize: 2, IdleTimeout: time.Minute,
	})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8128"}
	byName := newWiredServices(t, cli)

	efsBk := byName["EFS"].(*efsbackend.Handler).Backend
	efsBk.SetStorageRoot(t.TempDir())

	fs, err := efsBk.CreateFileSystem(t.Context(), efsbackend.CreateFileSystemRequest{CreationToken: "posix-efs"})
	require.NoError(t, err)

	ap, err := efsBk.CreateAccessPoint(t.Context(), efsbackend.CreateAccessPointRequest{
		FileSystemID: fs.FileSystemID,
		PosixUser:    &efsbackend.PosixUser{UID: 1234, GID: 5678, SecondaryGids: []int64{4321}},
		RootDirectory: &efsbackend.RootDirectory{
			Path:         "/apps/posix",
			CreationInfo: &efsbackend.CreationInfo{OwnerUID: 1234, OwnerGID: 5678, Permissions: "777"},
		},
	})
	require.NoError(t, err)

	key, err := byName["KMS"].(*kmsbackend.Handler).Backend.CreateKey(t.Context(), &kmsbackend.CreateKeyInput{})
	require.NoError(t, err)

	pa := portalloctest.New(t, 20)
	bk := lambdabackend.NewInMemoryBackend(rt, pa, lambdabackend.DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { bk.Close(context.Background()) })

	byName["Lambda"] = lambdabackend.NewHandler(bk)
	wireLambdaEFS(cli, byName)
	wireLambdaKMS(byName)

	require.NoError(t, bk.CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: "posix-fn",
		PackageType:  lambdabackend.PackageTypeZip,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		ZipData:      zipOf(t, "index.py", kmsEFSDockerHandler),
		Timeout:      60,
		KMSKeyArn:    key.KeyMetadata.Arn,
		Environment:  &lambdabackend.EnvironmentConfig{Variables: map[string]string{"DB_PASSWORD": "s3cr3t-value"}},
		FileSystemConfigs: []*lambdabackend.FileSystemConfig{
			{Arn: ap.AccessPointArn, LocalMountPath: "/mnt/shared"},
		},
	}))

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	out, status, err := bk.InvokeFunction(ctx, "posix-fn", lambdabackend.InvocationTypeRequestResponse, []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, 200, status)
	assert.JSONEq(t, `{"secret":"s3cr3t-value","uid":1234,"gid":5678,"groups":[4321,5678]}`, string(out))

	m, err := efsBk.ResolveAccessPointMount(ap.AccessPointArn)
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(m.HostDir, "/apps/posix"), m.HostDir)

	info, err := os.Stat(filepath.Join(m.HostDir, "who.txt"))
	require.NoError(t, err)

	st, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	assert.EqualValues(t, 1234, st.Uid)
	assert.EqualValues(t, 5678, st.Gid)
}
