package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

func TestWireTransferSFTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enabled  bool
		withS3   bool
		wantAddr bool
	}{
		{name: "disabled", enabled: false, withS3: true},
		{name: "no s3", enabled: true, withS3: false},
		{name: "enabled", enabled: true, withS3: true, wantAddr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			xfer := transferbackend.NewHandler(
				transferbackend.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1"),
			)
			t.Cleanup(func() { xfer.Shutdown(t.Context()) })

			byName := map[string]service.Registerable{"Transfer": xfer}
			if tt.withS3 {
				byName["S3"] = s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))
			}

			wireTransferSFTP(tt.enabled, "127.0.0.1", byName)

			srv, err := xfer.Backend.CreateServerFull(&transferbackend.CreateServerInput{Protocols: []string{"SFTP"}})
			require.NoError(t, err)

			bk, _ := xfer.Backend.(*transferbackend.InMemoryBackend)
			assert.Eventually(t, func() bool {
				return (bk.SFTPAddress(srv.ServerID) != "") == tt.wantAddr
			}, 2*time.Second, 10*time.Millisecond)
		})
	}
}
