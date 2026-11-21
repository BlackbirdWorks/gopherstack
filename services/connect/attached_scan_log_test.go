package connect_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

func TestAttachedFileApprovalLogsWouldScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolver fakeResolvers
		name     string
		wantErr  bool
	}{
		{
			name: "approved",
			resolver: fakeResolvers{
				objects: map[string]bool{"rec/call.wav": true},
				data:    map[string][]byte{"rec/call.wav": []byte("RIFF")},
			},
		},
		{name: "missing object", resolver: fakeResolvers{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			voice := e.outbound(t, "+442071230001", true)
			active := e.contactARN(t, voice)
			e.stop(t, voice)
			e.f.backend.SetResolvers(tt.resolver)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			in := &cn.CreateAttachedFileInput{
				FileSourceURI: "s3://rec/call.wav", FileUseCaseType: "VOICE_RECORDING", AssociatedResourceArn: active,
			}
			in.InstanceID = e.inst

			out, err := e.f.backend.CreateAttachedFile(ctx, in)
			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, buf.String(), "would scan")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "APPROVED", out.FileStatus)
			assert.Contains(t, buf.String(), "would scan attached file for viruses")
			assert.Contains(t, buf.String(), out.FileID)
		})
	}
}
