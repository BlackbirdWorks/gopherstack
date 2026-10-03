package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errMQTestFunction = errors.New("boom")

type fakeMQConsumer struct {
	closed   chan struct{}
	notify   chan struct{}
	pending  []mqMessage
	acked    []string
	requeues int
	mu       sync.Mutex
	once     sync.Once
}

func newFakeMQConsumer(msgs ...mqMessage) *fakeMQConsumer {
	return &fakeMQConsumer{pending: msgs, closed: make(chan struct{}), notify: make(chan struct{}, 64)}
}

func (f *fakeMQConsumer) Poll(ctx context.Context, maxMessages int) ([]mqMessage, error) {
	f.mu.Lock()

	if len(f.pending) > 0 {
		n := min(len(f.pending), maxMessages)
		out := f.pending[:n]
		f.pending = f.pending[n:]
		f.mu.Unlock()

		return out, nil
	}

	f.mu.Unlock()
	<-ctx.Done()

	return nil, nil
}

func (f *fakeMQConsumer) Ack(_ context.Context, msgs []mqMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, m := range msgs {
		f.acked = append(f.acked, m.MessageID)
	}

	f.notify <- struct{}{}

	return nil
}

func (f *fakeMQConsumer) Requeue(_ context.Context, msgs []mqMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requeues++

	back := make([]mqMessage, 0, len(msgs)+len(f.pending))
	for _, m := range msgs {
		m.Redelivered = true
		back = append(back, m)
	}

	f.pending = append(back, f.pending...)

	return nil
}

func (f *fakeMQConsumer) Close() { f.once.Do(func() { close(f.closed) }) }

func (f *fakeMQConsumer) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.acked...)
}

func (f *fakeMQConsumer) requeueCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.requeues
}

func mqTestMessages(bodies ...string) []mqMessage {
	out := make([]mqMessage, len(bodies))
	for i, b := range bodies {
		out[i] = mqMessage{
			MessageID: "m" + string(rune('0'+i)),
			Queue:     "q",
			Body:      []byte(b),
			Timestamp: time.UnixMilli(1000),
		}
	}

	return out
}

func TestMQWorkerBatching(t *testing.T) {
	t.Parallel()

	orderFilter := &FilterCriteria{Filters: []Filter{{Pattern: `{"data":{"type":["order"]}}`}}}

	tests := []struct {
		name         string
		source       string
		wantAcked    []string
		wantSizes    []int
		msgs         []mqMessage
		spec         mqWorkerSpec
		failures     int
		wantRequeues int
		wantInvokeAt time.Duration
	}{
		{
			name:      "batch size reached invokes immediately",
			spec:      mqWorkerSpec{BatchSize: 3, Window: time.Hour},
			msgs:      mqTestMessages("a", "b", "c"),
			wantSizes: []int{3},
			wantAcked: []string{"m0", "m1", "m2"},
		},
		{
			name:         "partial batch waits for the window",
			spec:         mqWorkerSpec{BatchSize: 10, Window: 5 * time.Second},
			msgs:         mqTestMessages("a", "b"),
			wantSizes:    []int{2},
			wantAcked:    []string{"m0", "m1"},
			wantInvokeAt: 5 * time.Second,
		},
		{
			name:      "messages over batch size split across invocations",
			spec:      mqWorkerSpec{BatchSize: 2, Window: time.Millisecond},
			msgs:      mqTestMessages("a", "b", "c", "d", "e"),
			wantSizes: []int{2, 2, 1},
			wantAcked: []string{"m0", "m1", "m2", "m3", "m4"},
		},
		{
			name:         "failed invocation requeues and redelivers before acking",
			spec:         mqWorkerSpec{BatchSize: 2, Window: time.Millisecond},
			msgs:         mqTestMessages("a", "b"),
			failures:     2,
			wantSizes:    []int{2, 2, 2},
			wantAcked:    []string{"m0", "m1"},
			wantRequeues: 2,
			wantInvokeAt: 3 * time.Second,
		},
		{
			name:      "filtered messages are acked without invoking",
			spec:      mqWorkerSpec{BatchSize: 2, Window: time.Millisecond, Filter: orderFilter},
			msgs:      mqTestMessages(`{"type":"other"}`, `{"type":"other"}`),
			wantAcked: []string{"m0", "m1"},
		},
		{
			name:      "mixed batch delivers matches and acks both",
			spec:      mqWorkerSpec{BatchSize: 2, Window: time.Millisecond, Filter: orderFilter},
			msgs:      mqTestMessages(`{"type":"order"}`, `{"type":"other"}`),
			wantSizes: []int{1},
			wantAcked: []string{"m1", "m0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)

			synctest.Test(t, func(t *testing.T) {
				cons := newFakeMQConsumer(tt.msgs...)
				p := NewEventSourcePoller(b, nil)
				p.mqFactory = func(mqConsumerConfig, int) (mqConsumer, error) { return cons, nil }

				var (
					mu       sync.Mutex
					sizes    []int
					calls    int
					lastAt   time.Duration
					ackedAtF []int
				)

				start := time.Now()
				p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
					mu.Lock()
					defer mu.Unlock()

					calls++
					lastAt = time.Since(start)
					ackedAtF = append(ackedAtF, len(cons.ackedIDs()))

					var ev struct {
						Messages []struct{} `json:"messages"`
					}

					require.NoError(t, json.Unmarshal(payload, &ev))

					sizes = append(sizes, len(ev.Messages))

					if calls <= tt.failures {
						return errMQTestFunction
					}

					return nil
				}

				ctx, cancel := context.WithCancel(t.Context())

				var done atomic.Bool

				go func() {
					defer done.Store(true)
					p.runMQWorker(ctx, tt.spec)
				}()

				for len(cons.ackedIDs()) < len(tt.wantAcked) {
					<-cons.notify
				}

				cancel()
				synctest.Wait()

				assert.True(t, done.Load())
				assert.Equal(t, tt.wantSizes, sizes)
				assert.ElementsMatch(t, tt.wantAcked, cons.ackedIDs())
				assert.Equal(t, tt.wantRequeues, cons.requeueCount())
				assert.GreaterOrEqual(t, lastAt, tt.wantInvokeAt)

				for i := range min(tt.failures+1, len(ackedAtF)) {
					assert.Zero(t, ackedAtF[i], "nothing is acked before the invocation that covers it succeeds")
				}
			})
		})
	}
}
