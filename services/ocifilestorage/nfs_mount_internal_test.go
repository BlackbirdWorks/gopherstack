package ocifilestorage

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nfs "github.com/willscott/go-nfs"
)

type fakeConn struct{ net.Conn }

func (fakeConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50000} }

func TestMountHonoursAllowedAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth []string
		want nfs.MountStatus
	}{
		{"sys", []string{authSys}, nfs.MountStatusOk},
		{"sys and kerberos", []string{"KRB5", authSys}, nfs.MountStatusOk},
		{"kerberos only", []string{"KRB5"}, nfs.MountStatusErrAcces},
		{"unset means SYS", nil, nfs.MountStatusOk},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plane, err := NewNFSPlane(NFSConfig{Dir: t.TempDir()})
			require.NoError(t, err)
			require.NoError(t, plane.CreateFS("fs1"))
			t.Cleanup(plane.Close)

			no := false
			opts := clientOptions{Source: "127.0.0.0/8", RequirePrivilegedSourcePort: &no, AllowedAuth: tt.auth}
			ex := ExportMount{ExportID: "x1", Path: "/x", FileSystemID: "fs1", Options: []clientOptions{opts}}
			h := &nfsHandler{plane: plane, mtID: "mt", exports: func(string) []ExportMount { return []ExportMount{ex} }}

			status, _, flavors := h.Mount(t.Context(), fakeConn{}, nfs.MountRequest{Dirpath: []byte("/x")})
			assert.Equal(t, tt.want, status)

			if tt.want == nfs.MountStatusOk {
				assert.NotEmpty(t, flavors)
			}
		})
	}
}
