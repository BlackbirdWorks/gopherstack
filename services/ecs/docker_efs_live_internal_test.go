package ecs

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockerimage "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
	"github.com/blackbirdworks/gopherstack/services/efs"
)

type noPullClient struct{ dockerClient }

func (noPullClient) ImagePull(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

type liveEFS struct{ bk *efs.InMemoryBackend }

func (l liveEFS) ResolveVolume(_ context.Context, region, fsID, ap, root string) (EFSVolumeMount, error) {
	m, err := l.bk.ResolveVolumeMount(region, fsID, ap, root)
	if err != nil {
		return EFSVolumeMount{}, err
	}

	return EFSVolumeMount{
		HostDir: m.HostDir, UID: m.UID, GID: m.GID, SecondaryGIDs: m.SecondaryGIDs, HasPosixUser: m.HasPosixUser,
	}, nil
}

func TestDockerRunnerLiveEFSAccessPoint(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	probe, err := mobyclient.New(mobyclient.FromEnv)
	require.NoError(t, err)

	t.Cleanup(func() { _ = probe.Close() })

	if _, imgErr := probe.ImageInspect(t.Context(), liveImage); imgErr != nil {
		t.Skipf("%s not present: %v", liveImage, imgErr)
	}

	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	t.Cleanup(func() { _ = cli.Close() })

	efsBk := efs.NewInMemoryBackend("123456789012", "us-east-1")
	efsBk.SetStorageRoot(t.TempDir())

	fs, err := efsBk.CreateFileSystem(t.Context(), efs.CreateFileSystemRequest{CreationToken: "live-ecs-efs"})
	require.NoError(t, err)

	ap, err := efsBk.CreateAccessPoint(t.Context(), efs.CreateAccessPointRequest{
		FileSystemID: fs.FileSystemID,
		PosixUser:    &efs.PosixUser{UID: 1234, GID: 5678},
		RootDirectory: &efs.RootDirectory{
			Path:         "/shared",
			CreationInfo: &efs.CreationInfo{OwnerUID: 1234, OwnerGID: 5678, Permissions: "777"},
		},
	})
	require.NoError(t, err)

	runner := newDockerRunnerWithClient(context.Background(), noPullClient{cli})
	b := NewInMemoryBackend("123456789012", "us-east-1", runner)
	b.wireRunnerHooks(runner)
	b.SetEFSResolver(liveEFS{efsBk})

	_, err = b.CreateCluster(CreateClusterInput{ClusterName: "live-efs"})
	require.NoError(t, err)

	t.Cleanup(b.Close)

	vol := Volume{Name: "shared", EFSVolumeConfiguration: &EFSVolumeConfiguration{
		FileSystemID: fs.FileSystemID, TransitEncryption: "ENABLED",
		AuthorizationConfig: &EFSAuthorizationConfig{AccessPointID: ap.AccessPointID, IAM: "ENABLED"},
	}}

	run := func(family, script string) *Task {
		td, tdErr := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
			Family: family, Volumes: []Volume{vol},
			ContainerDefinitions: []ContainerDefinition{{
				Name: "app", Image: liveImage, Essential: true, Command: []string{"sh", "-c", script},
				MountPoints: []MountPoint{{SourceVolume: "shared", ContainerPath: "/mnt/efs"}},
			}},
		})
		require.NoError(t, tdErr)

		tasks, _, runErr := b.RunTask(RunTaskInput{Cluster: "live-efs", TaskDefinition: td.TaskDefinitionArn})
		require.NoError(t, runErr)
		require.Len(t, tasks, 1)

		require.Eventually(t, func() bool {
			got, _, dErr := b.DescribeTasks("live-efs", []string{tasks[0].TaskArn})

			return dErr == nil && len(got) == 1 && got[0].LastStatus == statusStopped
		}, liveTimeout*2, liveTick)

		got, _, dErr := b.DescribeTasks("live-efs", []string{tasks[0].TaskArn})
		require.NoError(t, dErr)

		return &got[0]
	}

	writer := run("efs-writer", `echo "hello efs" > /mnt/efs/note.txt`)
	assert.Equal(t, stopCodeEssentialContainerExited, writer.StopCode)

	m, err := efsBk.ResolveAccessPointMount(ap.AccessPointArn)
	require.NoError(t, err)

	body, err := os.ReadFile(filepath.Join(m.HostDir, "note.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello efs\n", string(body))

	info, err := os.Stat(filepath.Join(m.HostDir, "note.txt"))
	require.NoError(t, err)

	st, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	assert.EqualValues(t, 1234, st.Uid, "container ran as the access point's POSIX user")
	assert.EqualValues(t, 5678, st.Gid)

	run("efs-reader", `cp /mnt/efs/note.txt /mnt/efs/copy.txt`)

	copied, err := os.ReadFile(filepath.Join(m.HostDir, "copy.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello efs\n", string(copied), "second task reads what the first wrote")
}
