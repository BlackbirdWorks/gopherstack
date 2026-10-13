package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

func TestCreateMountTarget_IPAddressInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subnet  string
		ip      string
		wantErr bool
	}{
		{name: "same subnet same ip", subnet: "subnet-a", ip: "10.0.0.5", wantErr: true},
		{name: "same subnet other ip", subnet: "subnet-a", ip: "10.0.0.6"},
		{name: "other subnet same ip", subnet: "subnet-b", ip: "10.0.0.5"},
		{name: "no ip requested", subnet: "subnet-a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestEFSBackend()

			first, err := b.CreateFileSystem(ctx, fsReq("tok-ip-1"))
			require.NoError(t, err)
			second, err := b.CreateFileSystem(ctx, fsReq("tok-ip-2"))
			require.NoError(t, err)

			held := mtReq(first.FileSystemID, "subnet-a")
			held.IPAddress = "10.0.0.5"
			_, err = b.CreateMountTarget(ctx, held)
			require.NoError(t, err)

			req := mtReq(second.FileSystemID, tt.subnet)
			req.IPAddress = tt.ip
			_, err = b.CreateMountTarget(ctx, req)

			if tt.wantErr {
				require.ErrorIs(t, err, efs.ErrIPAddressInUse)

				return
			}

			require.NoError(t, err)
		})
	}
}
