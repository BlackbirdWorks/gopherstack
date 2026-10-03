package ecr_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestJanitor_SweepExpiresAbandonedLayerUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		idle    time.Duration
		wantErr bool
	}{
		{name: "idle past ttl", idle: ecr.LayerUploadTTLForTest + time.Hour, wantErr: true},
		{name: "idle within ttl", idle: time.Hour, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				be := ecr.NewInMemoryBackend("000000000000", "us-east-1", "localhost:5000")

				_, err := be.CreateRepository(ctx, "repo", "MUTABLE", false, "", "")
				require.NoError(t, err)

				up, err := be.InitiateLayerUpload(ctx, "repo")
				require.NoError(t, err)

				time.Sleep(tt.idle)
				ecr.NewJanitor(be, 0).SweepOnce(ctx)

				_, err = be.UploadLayerPart(ctx, "repo", up.UploadID, 0, 3, []byte("blob"))
				if tt.wantErr {
					require.ErrorIs(t, err, ecr.ErrUploadNotFound)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}
