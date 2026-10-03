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

var errKafkaTestFunction = errors.New("boom")

type fakeKafkaConsumer struct {
	closed  chan struct{}
	notify  chan struct{}
	batches [][]KafkaRecord
	commits [][]KafkaRecord
	mu      sync.Mutex
	once    sync.Once
}

func newFakeKafkaConsumer(batches ...[]KafkaRecord) *fakeKafkaConsumer {
	return &fakeKafkaConsumer{batches: batches, closed: make(chan struct{}), notify: make(chan struct{}, 64)}
}

func (f *fakeKafkaConsumer) Poll(ctx context.Context, maxRecords int) ([]KafkaRecord, error) {
	f.mu.Lock()

	if len(f.batches) > 0 {
		b := f.batches[0]
		n := min(len(b), maxRecords)

		if n == len(b) {
			f.batches = f.batches[1:]
		} else {
			f.batches[0] = b[n:]
		}

		f.mu.Unlock()

		return b[:n], nil
	}

	f.mu.Unlock()
	<-ctx.Done()

	return nil, nil
}

func (f *fakeKafkaConsumer) Commit(_ context.Context, recs []KafkaRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.commits = append(f.commits, recs)
	f.notify <- struct{}{}

	return nil
}

func (f *fakeKafkaConsumer) Close() { f.once.Do(func() { close(f.closed) }) }

func (f *fakeKafkaConsumer) committedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []int64

	for _, c := range f.commits {
		for _, r := range c {
			out = append(out, r.Offset)
		}
	}

	return out
}

func newKafkaTestBackend(t *testing.T) *InMemoryBackend {
	t.Helper()

	b := NewInMemoryBackend(nil, nil, DefaultSettings(), "000000000000", "us-east-1")

	t.Cleanup(func() { b.Close(context.Background()) })

	return b
}

func kafkaOffsets(n int) []KafkaRecord {
	recs := make([]KafkaRecord, n)
	for i := range recs {
		recs[i] = kafkaTestRecord("t", 0, int64(i), "v")
	}

	return recs
}

func TestKafkaWorkerBatching(t *testing.T) {
	t.Parallel()

	orderFilter := &FilterCriteria{Filters: []Filter{{Pattern: `{"value":{"type":["order"]}}`}}}

	tests := []struct {
		name         string
		wantBatches  []int
		wantCommits  []int64
		batches      [][]KafkaRecord
		spec         kafkaWorkerSpec
		failures     int
		wantInvokeAt time.Duration
	}{
		{
			name:        "batch size reached invokes immediately",
			spec:        kafkaWorkerSpec{BatchSize: 3, Window: time.Hour},
			batches:     [][]KafkaRecord{kafkaOffsets(3)},
			wantBatches: []int{3},
			wantCommits: []int64{0, 1, 2},
		},
		{
			name:         "partial batch waits for the window",
			spec:         kafkaWorkerSpec{BatchSize: 10, Window: 5 * time.Second},
			batches:      [][]KafkaRecord{kafkaOffsets(2)},
			wantBatches:  []int{2},
			wantCommits:  []int64{0, 1},
			wantInvokeAt: 5 * time.Second,
		},
		{
			name:        "records over batch size split across invocations",
			spec:        kafkaWorkerSpec{BatchSize: 2, Window: time.Millisecond},
			batches:     [][]KafkaRecord{kafkaOffsets(5)},
			wantBatches: []int{2, 2, 1},
			wantCommits: []int64{0, 1, 2, 3, 4},
		},
		{
			name:         "failed invocation retries the same batch before committing",
			spec:         kafkaWorkerSpec{BatchSize: 2, Window: time.Millisecond},
			batches:      [][]KafkaRecord{kafkaOffsets(2)},
			failures:     2,
			wantBatches:  []int{2, 2, 2},
			wantCommits:  []int64{0, 1},
			wantInvokeAt: 3 * time.Second,
		},
		{
			name: "filtered records are committed without invoking",
			spec: kafkaWorkerSpec{
				BatchSize: 2, Window: time.Millisecond,
				Filter: orderFilter,
			},
			batches: [][]KafkaRecord{{
				kafkaTestRecord("t", 0, 0, `{"type":"other"}`),
				kafkaTestRecord("t", 0, 1, `{"type":"other"}`),
			}},
			wantBatches: nil,
			wantCommits: []int64{0, 1},
		},
		{
			name: "mixed batch delivers matches and commits both",
			spec: kafkaWorkerSpec{
				BatchSize: 2, Window: time.Millisecond,
				Filter: orderFilter,
			},
			batches: [][]KafkaRecord{{
				kafkaTestRecord("t", 0, 0, `{"type":"order"}`),
				kafkaTestRecord("t", 0, 1, `{"type":"other"}`),
			}},
			wantBatches: []int{1},
			wantCommits: []int64{0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)

			synctest.Test(t, func(t *testing.T) {
				cons := newFakeKafkaConsumer(tt.batches...)
				p := NewEventSourcePoller(b, nil)
				p.kafkaFactory = func(kafkaConsumerConfig) (kafkaConsumer, error) { return cons, nil }

				var (
					mu     sync.Mutex
					sizes  []int
					calls  int
					lastAt time.Duration
				)

				start := time.Now()
				p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
					mu.Lock()
					defer mu.Unlock()

					calls++
					lastAt = time.Since(start)

					var ev struct {
						Records map[string][]struct{} `json:"records"`
					}

					require.NoError(t, json.Unmarshal(payload, &ev))

					n := 0
					for _, rs := range ev.Records {
						n += len(rs)
					}

					sizes = append(sizes, n)

					if calls <= tt.failures {
						return errKafkaTestFunction
					}

					return nil
				}

				ctx, cancel := context.WithCancel(t.Context())

				var done atomic.Bool

				go func() {
					defer done.Store(true)
					p.runKafkaWorker(ctx, tt.spec)
				}()

				for len(cons.committedOffsets()) < len(tt.wantCommits) {
					<-cons.notify
				}

				cancel()
				synctest.Wait()

				assert.True(t, done.Load())
				assert.Equal(t, tt.wantBatches, sizes)
				assert.Equal(t, tt.wantCommits, cons.committedOffsets())
				assert.GreaterOrEqual(t, lastAt, tt.wantInvokeAt)
			})
		})
	}
}
