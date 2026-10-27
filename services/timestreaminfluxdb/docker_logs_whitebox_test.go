package timestreaminfluxdb

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type blockingStreamer struct {
	ContainerRuntime
}

func (s *blockingStreamer) StreamLogs(ctx context.Context, _ string, _, _ io.Writer) error {
	<-ctx.Done()

	return ctx.Err()
}

func TestDockerEngine_FollowDropRaceFree(t *testing.T) {
	t.Parallel()

	d := &DockerEngine{Runtime: &blockingStreamer{}}

	var wg sync.WaitGroup

	for range 50 {
		wg.Go(func() { d.followLogs("c1") })
		wg.Go(func() { d.dropLogs("c1") })
	}

	wg.Wait()
	d.dropLogs("c1")

	_, ok := d.logs.Load("c1")
	assert.False(t, ok)
}
