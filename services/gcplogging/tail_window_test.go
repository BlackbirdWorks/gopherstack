package gcplogging_test

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcplogging"
)

type fakeTail struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *loggingpb.TailLogEntriesResponse
}

func (f *fakeTail) Send(r *loggingpb.TailLogEntriesResponse) error {
	f.sent <- r

	return nil
}

func (f *fakeTail) Recv() (*loggingpb.TailLogEntriesRequest, error) { return nil, io.EOF }
func (f *fakeTail) Context() context.Context                        { return f.ctx }

func tailTexts(r *loggingpb.TailLogEntriesResponse) []string {
	out := make([]string, 0, len(r.GetEntries()))
	for _, e := range r.GetEntries() {
		out = append(out, e.GetTextPayload())
	}

	return out
}

func TestTailBuffersAndOrdersEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		window *durationpb.Duration
		name   string
		want   []string
		early  bool
	}{
		{name: "default window", window: nil, want: []string{"older", "newer"}},
		{name: "explicit window", window: durationpb.New(5 * time.Second), want: []string{"older", "newer"}},
		{name: "no buffering", window: durationpb.New(0), want: []string{"newer"}, early: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := gcplogging.NewInMemoryBackend()
				ctx, cancel := context.WithCancel(t.Context())
				stream := &fakeTail{ctx: ctx, sent: make(chan *loggingpb.TailLogEntriesResponse, 8)}
				finished := make(chan error, 1)

				go func() {
					finished <- b.Tail(stream, &loggingpb.TailLogEntriesRequest{
						ResourceNames: []string{proj}, BufferWindow: tt.window,
					})
				}()
				synctest.Wait()

				now := time.Now()
				write := func(text string, ts time.Time) {
					require.NoError(t, b.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
						Resource: global(), Entries: []*loggingpb.LogEntry{textEntry("tl", text, 0, ts)},
					}))
				}

				write("newer", now)
				time.Sleep(500 * time.Millisecond)
				write("older", now.Add(-time.Second))
				synctest.Wait()

				if tt.early {
					got := <-stream.sent
					assert.Equal(t, []string{"newer"}, tailTexts(got))
					got = <-stream.sent
					assert.Equal(t, []string{"older"}, tailTexts(got))
				} else {
					assert.Empty(t, stream.sent, "entries wait for the buffer window")

					time.Sleep(tailWait(tt.window))
					synctest.Wait()

					got := <-stream.sent
					assert.Equal(t, tt.want, tailTexts(got), "released in timestamp order")
				}

				cancel()
				synctest.Wait()
				require.NoError(t, <-finished)
			})
		})
	}
}

func tailWait(w *durationpb.Duration) time.Duration {
	if w == nil {
		return 2 * time.Second
	}

	return w.AsDuration()
}
