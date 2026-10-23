package container_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errLogsFailed = errors.New("logs failed")

type logsAPI struct {
	err     error
	gotOpts dockercontainer.LogsOptions
	stream  []byte
	mockAPI
}

func (l *logsAPI) ContainerLogs(
	_ context.Context,
	_ string,
	opts dockercontainer.LogsOptions,
) (io.ReadCloser, error) {
	l.gotOpts = opts

	return io.NopCloser(bytes.NewReader(l.stream)), l.err
}

func frame(stream byte, payload string) []byte {
	hdr := make([]byte, 8, 8+len(payload))
	hdr[0] = stream
	binary.BigEndian.PutUint32(hdr[4:], uint32(len(payload)))

	return append(hdr, payload...)
}

func muxFrames(t *testing.T) []byte {
	t.Helper()

	return append(frame(1, "out line\n"), frame(2, "err line\n")...)
}

func TestDockerRuntime_StreamLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		api     func(t *testing.T) container.APIClient
		wantErr error
		name    string
		out     string
		errOut  string
	}{
		{
			name: "demultiplexes",
			api: func(t *testing.T) container.APIClient {
				t.Helper()

				return &logsAPI{stream: muxFrames(t)}
			},
			out: "out line\n", errOut: "err line\n",
		},
		{
			name:    "client error",
			api:     func(*testing.T) container.APIClient { return &logsAPI{err: errLogsFailed} },
			wantErr: errLogsFailed,
		},
		{
			name:    "unsupported client",
			api:     func(*testing.T) container.APIClient { return &mockAPI{} },
			wantErr: container.ErrLogsUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := tt.api(t)

			var stdout, stderr bytes.Buffer

			err := newRuntime(api).StreamLogs(t.Context(), "ctr-1", &stdout, &stderr)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.out, stdout.String())
			assert.Equal(t, tt.errOut, stderr.String())

			opts := api.(*logsAPI).gotOpts
			assert.True(t, opts.Follow)
			assert.True(t, opts.ShowStdout)
			assert.True(t, opts.ShowStderr)
		})
	}
}
