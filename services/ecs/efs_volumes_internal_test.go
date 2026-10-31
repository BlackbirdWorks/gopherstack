package ecs

import (
	"context"
	"errors"
	"testing"

	"github.com/moby/moby/api/types/mount"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestNoAccessPoint = errors.New("AccessPointNotFound")

type fakeEFS struct {
	err  error
	got  EFSVolumeConfiguration
	reg  string
	mnt  EFSVolumeMount
	call int
}

func (f *fakeEFS) ResolveVolume(
	_ context.Context, region, fsID, ap, root string,
) (EFSVolumeMount, error) {
	f.call++
	f.reg = region
	f.got = EFSVolumeConfiguration{
		FileSystemID: fsID, RootDirectory: root, AuthorizationConfig: &EFSAuthorizationConfig{AccessPointID: ap},
	}

	return f.mnt, f.err
}

func TestRunTask_EFSVolume(t *testing.T) {
	t.Parallel()

	posix := EFSVolumeMount{
		HostDir:       "/efs/fs-1/ap",
		UID:           1234,
		GID:           5678,
		SecondaryGIDs: []int64{4321},
		HasPosixUser:  true,
	}

	tests := []struct {
		efs        *fakeEFS
		cfg        *EFSVolumeConfiguration
		name       string
		user       string
		wantUser   string
		wantReason string
		wantMount  string
		wantGroups []string
		noResolver bool
	}{
		{
			name: "access_point_posix_user",
			efs:  &fakeEFS{mnt: posix},
			cfg: &EFSVolumeConfiguration{
				FileSystemID: "fs-1", TransitEncryption: "ENABLED",
				AuthorizationConfig: &EFSAuthorizationConfig{AccessPointID: "fsap-1", IAM: "ENABLED"},
			},
			wantUser: "1234:5678", wantGroups: []string{"4321"}, wantMount: "/efs/fs-1/ap",
		},
		{
			name: "container_user_wins",
			efs:  &fakeEFS{mnt: posix},
			user: "7:7",
			cfg: &EFSVolumeConfiguration{
				FileSystemID: "fs-1", AuthorizationConfig: &EFSAuthorizationConfig{AccessPointID: "fsap-1"},
			},
			wantUser: "7:7", wantMount: "/efs/fs-1/ap",
		},
		{
			name: "root_directory_no_access_point",
			efs:  &fakeEFS{mnt: EFSVolumeMount{HostDir: "/efs/fs-1/sub"}},
			cfg:  &EFSVolumeConfiguration{FileSystemID: "fs-1", RootDirectory: "/sub"}, wantMount: "/efs/fs-1/sub",
		},
		{
			name: "missing_access_point",
			efs:  &fakeEFS{err: errTestNoAccessPoint},
			cfg: &EFSVolumeConfiguration{
				FileSystemID: "fs-1", AuthorizationConfig: &EFSAuthorizationConfig{AccessPointID: "fsap-x"},
			},
			wantReason: efsInitFailure,
		},
		{
			name: "no_efs_integration", noResolver: true,
			cfg:        &EFSVolumeConfiguration{FileSystemID: "fs-1"},
			wantReason: efsInitFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			r := newLaunchRunner(t, fake)

			if !tt.noResolver {
				b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
				b.SetEFSResolver(tt.efs)
				r.SetEFSVolumeResolver(b.resolveEFSVolume)
			}

			td := &TaskDefinition{
				Volumes: []Volume{{Name: "shared", EFSVolumeConfiguration: tt.cfg}},
				ContainerDefinitions: []ContainerDefinition{{
					Name: "app", Image: "busybox", Essential: true, User: tt.user,
					MountPoints: []MountPoint{{SourceVolume: "shared", ContainerPath: "/mnt/efs"}},
				}},
			}

			err := r.RunTask(launchTask(), td)
			if tt.wantReason != "" {
				var le *taskLaunchError

				require.ErrorAs(t, err, &le)
				assert.Contains(t, le.Reason, tt.wantReason)
				assert.Empty(t, fake.createdImages())

				return
			}

			require.NoError(t, err)

			c := fake.createAt(0)
			assert.Equal(t, tt.wantUser, c.cfg.User)
			assert.Equal(t, tt.wantGroups, c.host.GroupAdd)
			assert.Equal(t, "us-east-1", tt.efs.reg)
			require.Len(t, c.host.Mounts, 1)
			assert.Equal(t, mount.TypeBind, c.host.Mounts[0].Type)
			assert.Equal(t, tt.wantMount, c.host.Mounts[0].Source)
			assert.Equal(t, "/mnt/efs", c.host.Mounts[0].Target)
		})
	}
}
