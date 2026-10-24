package batch

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockerimage "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
)

var (
	errFakePull   = errors.New("pull denied")
	errFakeCreate = errors.New("no such image")
)

type fakeDocker struct {
	pullErr     error
	createErr   error
	logs        []byte
	cfg         *dockertypes.Config
	host        *dockertypes.HostConfig
	removed     []string
	keptVolumes []string
	stopped     []string
	exit        int64
	mu          sync.Mutex
	blockWait   bool
}

func (f *fakeDocker) ImagePull(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
	if f.pullErr != nil {
		return nil, f.pullErr
	}

	return io.NopCloser(strings.NewReader("")), nil
}

func (f *fakeDocker) ContainerCreate(
	_ context.Context, cfg *dockertypes.Config, host *dockertypes.HostConfig,
	_ *dockernetwork.NetworkingConfig, _ *ocispec.Platform, _ string,
) (dockertypes.CreateResponse, error) {
	f.cfg, f.host = cfg, host

	return dockertypes.CreateResponse{ID: "cid"}, f.createErr
}

func (f *fakeDocker) ContainerStart(context.Context, string, dockertypes.StartOptions) error {
	return nil
}

func (f *fakeDocker) ContainerStop(_ context.Context, id string, _ dockertypes.StopOptions) error {
	f.mu.Lock()
	f.stopped = append(f.stopped, id)
	f.mu.Unlock()

	return nil
}

func (f *fakeDocker) ContainerRemove(_ context.Context, id string, opts dockertypes.RemoveOptions) error {
	f.mu.Lock()
	if !opts.RemoveVolumes {
		f.keptVolumes = append(f.keptVolumes, id)
	}
	f.removed = append(f.removed, id)
	f.mu.Unlock()

	return nil
}

func (f *fakeDocker) ContainerLogs(
	context.Context, string, dockertypes.LogsOptions,
) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.logs)), nil
}

func (f *fakeDocker) ContainerWait(_ context.Context, _ string, _ dockertypes.WaitOptions) dockertypes.WaitResult {
	res := make(chan dockertypes.WaitResponse, 1)
	errc := make(chan error, 1)

	if !f.blockWait {
		res <- dockertypes.WaitResponse{StatusCode: f.exit}
	}

	return dockertypes.WaitResult{Result: res, Error: errc}
}

func muxFrame(stream byte, payload string) []byte {
	frame := make([]byte, 8, 8+len(payload))
	frame[0] = stream
	binary.BigEndian.PutUint32(frame[4:], uint32(len(payload)))

	return append(frame, payload...)
}

func TestDockerContainerRunnerRunContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		docker    *fakeDocker
		name      string
		wantErr   string
		wantLogs  []string
		wantExit  int
		wantStart bool
	}{
		{
			name: "exit code and demuxed logs",
			docker: &fakeDocker{
				exit: 3,
				logs: append(muxFrame(1, "out one\nout two\n"), muxFrame(2, "err tail")...),
			},
			wantExit:  3,
			wantLogs:  []string{"out one", "out two", "err tail"},
			wantStart: true,
		},
		{
			name:    "pull failure surfaces when image absent",
			docker:  &fakeDocker{pullErr: errFakePull, createErr: errFakeCreate},
			wantErr: reasonPullFailed,
		},
		{
			name:    "create failure without pull error",
			docker:  &fakeDocker{createErr: errFakeCreate},
			wantErr: reasonCreateFailed,
		},
		{
			name:      "local image runs despite pull failure",
			docker:    &fakeDocker{pullErr: errFakePull},
			wantStart: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &dockerContainerRunner{cli: tt.docker}

			var (
				mu      sync.Mutex
				lines   []string
				started bool
			)

			code, err := r.RunContainer(context.Background(), ContainerSpec{
				Image: "img", Command: []string{"sh"}, Env: []string{"A=1"}, MemoryMiB: 1, VCPUs: 2,
				Labels: map[string]string{"k": "v"},
			}, RunHooks{
				OnStarted: func() { started = true },
				OnLog: func(l []string) {
					mu.Lock()
					lines = append(lines, l...)
					mu.Unlock()
				},
			})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.False(t, started)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantExit, code)
			assert.Equal(t, tt.wantStart, started)
			assert.Equal(t, tt.wantLogs, lines)
			assert.Equal(t, []string{"A=1"}, tt.docker.cfg.Env)
			assert.Equal(t, []string{"sh"}, tt.docker.cfg.Cmd)
			assert.Equal(t, int64(dockerMinMemoryMiB*bytesPerMiB), tt.docker.host.Memory)
			assert.Equal(t, int64(2*cpuSharesPerVCPU), tt.docker.host.CPUShares)
			assert.Equal(t, []string{"cid"}, tt.docker.removed)
			assert.Empty(t, tt.docker.keptVolumes, "removal must pass RemoveVolumes")
		})
	}
}

func TestDockerContainerRunnerCancelStopsAndRemoves(t *testing.T) {
	t.Parallel()

	f := &fakeDocker{blockWait: true}
	r := &dockerContainerRunner{cli: f}
	ctx, cancel := context.WithCancel(context.Background())

	_, err := r.RunContainer(ctx, ContainerSpec{Image: "img"}, RunHooks{OnStarted: cancel})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"cid"}, f.stopped)
	assert.Equal(t, []string{"cid"}, f.removed)
}

func TestLineWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		writes []string
		want   []string
	}{
		{name: "split across writes", writes: []string{"a\nb", "c\nd"}, want: []string{"a", "bc", "d"}},
		{name: "crlf", writes: []string{"x\r\ny\r\n"}, want: []string{"x", "y"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got []string

			w := &lineWriter{emit: func(l []string) { got = append(got, l...) }}
			for _, s := range tt.writes {
				_, err := w.Write([]byte(s))
				require.NoError(t, err)
			}

			w.flush()
			assert.Equal(t, tt.want, got)
		})
	}
}
