package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
)

func TestECSEFSResolverWiring(t *testing.T) {
	t.Parallel()

	byName := newWiredServices(t, &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8129"})
	efsBk := byName["EFS"].(*efsbackend.Handler).Backend
	efsBk.SetStorageRoot(t.TempDir())

	fs, err := efsBk.CreateFileSystem(t.Context(), efsbackend.CreateFileSystemRequest{CreationToken: "ecs-wire"})
	require.NoError(t, err)

	ap, err := efsBk.CreateAccessPoint(t.Context(), efsbackend.CreateAccessPointRequest{
		FileSystemID: fs.FileSystemID,
		PosixUser:    &efsbackend.PosixUser{UID: 10, GID: 20},
	})
	require.NoError(t, err)

	tests := []struct {
		byName  map[string]service.Registerable
		name    string
		ap      string
		wantErr bool
	}{
		{name: "access_point", byName: byName, ap: ap.AccessPointID},
		{name: "unknown_access_point", byName: byName, ap: "fsap-0000000000000000", wantErr: true},
		{name: "efs_missing", byName: map[string]service.Registerable{}, ap: ap.AccessPointID, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, rerr := (&ecsEFS{byName: tt.byName}).ResolveVolume(t.Context(), "us-east-1", fs.FileSystemID, tt.ap, "")
			if tt.wantErr {
				require.Error(t, rerr)

				return
			}

			require.NoError(t, rerr)
			assert.True(t, m.HasPosixUser)
			assert.EqualValues(t, 10, m.UID)
			assert.NotEmpty(t, m.HostDir)
		})
	}

	assert.NotNil(t, ecsBackendOf(byName), "ecs backend present for wiring")
}
