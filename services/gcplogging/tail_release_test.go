package gcplogging_test

import (
	"context"
	"sync"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

type blockedTailStream struct {
	grpc.ServerStream
	ctx     context.Context
	started chan struct{}
	once    *sync.Once
}

func (s blockedTailStream) Context() context.Context { return s.ctx }

func (s blockedTailStream) Recv() (*loggingpb.TailLogEntriesRequest, error) {
	s.once.Do(func() { close(s.started) })
	<-s.ctx.Done()

	return nil, s.ctx.Err()
}

func (blockedTailStream) Send(*loggingpb.TailLogEntriesResponse) error { return nil }

func TestTailReleasesSessionWhenStreamEnds(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	b := e.handler.Backend

	ctx, cancel := context.WithCancel(t.Context())
	stream := blockedTailStream{ctx: ctx, started: make(chan struct{}), once: &sync.Once{}}
	done := make(chan error, 1)

	go func() {
		done <- b.Tail(stream, &loggingpb.TailLogEntriesRequest{
			ResourceNames: []string{proj}, BufferWindow: durationpb.New(0),
		})
	}()

	<-stream.started
	require.Equal(t, 1, b.TailSessions())

	cancel()
	require.NoError(t, <-done)
	assert.Zero(t, b.TailSessions())
}
