package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errPost = errors.New("post failed")

// mobyWait mimics moby's ContainerWait: unbuffered result, buffered error,
// and a sender goroutine that never selects on ctx. The sender runs only
// after release is closed.
func mobyWait(mode string, release <-chan struct{}) (dockercontainer.WaitResult, <-chan struct{}) {
	resultC := make(chan dockercontainer.WaitResponse)
	errC := make(chan error, 1)
	done := make(chan struct{})

	switch mode {
	case "post_error":
		errC <- errPost
		close(done)
	case "error_first":
		errC <- errPost
		close(done)
	default:
		go func() {
			defer close(done)
			<-release
			resultC <- dockercontainer.WaitResponse{StatusCode: 7}
		}()
	}

	return dockercontainer.WaitResult{Result: resultC, Error: errC}, done
}

func TestAwaitWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      string
		cancel    bool
		wantErr   error
		wantCode  int64
		wantCtxEr bool
	}{
		{name: "ctx_cancelled_before_result", mode: "result", cancel: true, wantCtxEr: true},
		{name: "result_first", mode: "result", wantCode: 7},
		{name: "error_first", mode: "error_first", wantErr: errPost},
		{name: "post_error", mode: "post_error", wantErr: errPost},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			release := make(chan struct{})
			res, done := mobyWait(tt.mode, release)

			if tt.cancel {
				cancel()
			} else if tt.mode == "result" {
				close(release)
			}

			resp, err := container.AwaitWait(ctx, res)

			switch {
			case tt.wantCtxEr:
				require.ErrorIs(t, err, context.Canceled)
				close(release)
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantCode, resp.StatusCode)
			}

			<-done // sender goroutine exited: nothing leaked
		})
	}
}
